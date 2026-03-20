from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from datetime import datetime, timedelta, timezone
from typing import Optional
import uuid
import os
import json
import random
import string
import secrets
import hashlib
import math

from sqlalchemy import create_engine, Column, String, DateTime, Float, Integer, Text, text
from sqlalchemy.orm import declarative_base, sessionmaker
import redis

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@db:5432/xcpro")
STALE_AFTER_SECONDS = 120
MAX_POSITION_FUTURE_SKEW_SECONDS = 300
MAX_REASONABLE_ALT_M = 20000
MIN_REASONABLE_ALT_M = -1000
MAX_REASONABLE_SPEED = 1000
MAX_IMPOSSIBLE_GROUND_SPEED_KMH = 500
MAX_TASK_RADIUS_M = 500000

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)


def generate_share_code(length=8):
    chars = string.ascii_uppercase + string.digits
    return ''.join(random.choice(chars) for _ in range(length))


def generate_write_token():
    return secrets.token_urlsafe(24)


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def to_utc_naive(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def to_iso_utc(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc).isoformat()
    return dt.astimezone(timezone.utc).isoformat()


def parse_number(value, field_name: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail=f"{field_name} must be numeric")


def validate_lat_lon(lat: float, lon: float, field_prefix: str) -> None:
    if lat < -90 or lat > 90:
        raise HTTPException(status_code=400, detail=f"{field_prefix}.lat out of range")
    if lon < -180 or lon > 180:
        raise HTTPException(status_code=400, detail=f"{field_prefix}.lon out of range")


def validate_radius(radius_value, field_name: str) -> None:
    radius = parse_number(radius_value, field_name)
    if radius <= 0 or radius > MAX_TASK_RADIUS_M:
        raise HTTPException(status_code=400, detail=f"{field_name} out of range")


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371000.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)

    a = math.sin(dlat / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


class LiveSession(Base):
    __tablename__ = "live_sessions"

    id = Column(String, primary_key=True, index=True)
    share_code = Column(String, unique=True, index=True, nullable=False)
    created_at = Column(DateTime, nullable=False)
    status = Column(
    String(20),
    nullable=False,
    default="active",
    server_default=text("'active'::character varying"))
    last_position_at = Column(DateTime, nullable=True)
    ended_at = Column(DateTime, nullable=True)
    write_token_hash = Column(String(64), nullable=True)


class LivePosition(Base):
    __tablename__ = "live_positions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String, index=True, nullable=False)
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    alt = Column(Float, nullable=False)
    speed = Column(Float, nullable=False)
    heading = Column(Float, nullable=False)
    timestamp = Column(DateTime, nullable=False)


class LiveTask(Base):
    __tablename__ = "live_tasks"

    id = Column(String, primary_key=True, index=True)
    session_id = Column(String, unique=True, index=True, nullable=False)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    current_revision = Column(Integer, nullable=False)


class LiveTaskRevision(Base):
    __tablename__ = "live_task_revisions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String, index=True, nullable=False)
    revision = Column(Integer, nullable=False)
    created_at = Column(DateTime, nullable=False)
    payload_json = Column(Text, nullable=False)


class Position(BaseModel):
    session_id: str
    lat: float
    lon: float
    alt: float
    speed: float
    heading: float
    timestamp: datetime


class TaskUpsertRequest(BaseModel):
    session_id: str
    task_name: str
    task: dict


class SessionEndRequest(BaseModel):
    session_id: str


def get_session_or_404(db, session_id: str) -> LiveSession:
    session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
    if not session:
        raise HTTPException(status_code=404, detail="session not found")
    return session


def require_write_access(session: LiveSession, x_session_token: Optional[str]) -> None:
    if not x_session_token:
        raise HTTPException(status_code=401, detail="missing X-Session-Token header")

    if not session.write_token_hash:
        raise HTTPException(status_code=403, detail="write token unavailable for this session")

    if not secrets.compare_digest(session.write_token_hash, hash_token(x_session_token)):
        raise HTTPException(status_code=403, detail="invalid session token")


def compute_effective_status(session: LiveSession) -> str:
    if session.status == "ended":
        return "ended"

    if session.last_position_at is None:
        return "active"

    age = datetime.utcnow() - session.last_position_at
    if age > timedelta(seconds=STALE_AFTER_SECONDS):
        return "stale"

    return "active"


def validate_position_payload(p: Position, position_ts: datetime) -> None:
    validate_lat_lon(p.lat, p.lon, "position")

    if p.alt < MIN_REASONABLE_ALT_M or p.alt > MAX_REASONABLE_ALT_M:
        raise HTTPException(status_code=400, detail="position.alt out of range")

    if p.speed < 0 or p.speed > MAX_REASONABLE_SPEED:
        raise HTTPException(status_code=400, detail="position.speed out of range")

    if p.heading < 0 or p.heading > 360:
        raise HTTPException(status_code=400, detail="position.heading out of range")

    if position_ts > datetime.utcnow() + timedelta(seconds=MAX_POSITION_FUTURE_SKEW_SECONDS):
        raise HTTPException(status_code=400, detail="position.timestamp too far in the future")


def validate_task_payload(req: TaskUpsertRequest) -> str:
    task_name = (req.task_name or "").strip()
    if not task_name:
        raise HTTPException(status_code=400, detail="task_name is required")

    task = req.task
    turnpoints = task.get("turnpoints")

    if not isinstance(turnpoints, list) or len(turnpoints) < 2:
        raise HTTPException(status_code=400, detail="task.turnpoints must contain at least 2 items")

    for idx, tp in enumerate(turnpoints):
        if not isinstance(tp, dict):
            raise HTTPException(status_code=400, detail=f"task.turnpoints[{idx}] must be an object")

        name = str(tp.get("name", "")).strip()
        tp_type = str(tp.get("type", "")).strip()

        if not name:
            raise HTTPException(status_code=400, detail=f"task.turnpoints[{idx}].name is required")
        if not tp_type:
            raise HTTPException(status_code=400, detail=f"task.turnpoints[{idx}].type is required")
        if "lat" not in tp or "lon" not in tp:
            raise HTTPException(status_code=400, detail=f"task.turnpoints[{idx}] requires lat/lon")

        lat = parse_number(tp.get("lat"), f"task.turnpoints[{idx}].lat")
        lon = parse_number(tp.get("lon"), f"task.turnpoints[{idx}].lon")
        validate_lat_lon(lat, lon, f"task.turnpoints[{idx}]")

        if "radius_m" in tp and tp.get("radius_m") is not None:
            validate_radius(tp.get("radius_m"), f"task.turnpoints[{idx}].radius_m")

    for boundary_name in ["start", "finish"]:
        boundary = task.get(boundary_name)
        if boundary is None:
            continue

        if not isinstance(boundary, dict):
            raise HTTPException(status_code=400, detail=f"task.{boundary_name} must be an object")

        if "type" in boundary and not str(boundary.get("type", "")).strip():
            raise HTTPException(status_code=400, detail=f"task.{boundary_name}.type is invalid")

        if "radius_m" in boundary and boundary.get("radius_m") is not None:
            validate_radius(boundary.get("radius_m"), f"task.{boundary_name}.radius_m")

    return task_name


def build_live_response(db, session):
    latest_raw = redis_client.get(f"live:latest:{session.id}")
    latest = json.loads(latest_raw) if latest_raw else None

    positions = (
        db.query(LivePosition)
        .filter(LivePosition.session_id == session.id)
        .order_by(LivePosition.timestamp.desc(), LivePosition.id.desc())
        .limit(10)
        .all()
    )

    task = db.query(LiveTask).filter(LiveTask.session_id == session.id).first()
    task_revision_data = None

    if task:
        revision = (
            db.query(LiveTaskRevision)
            .filter(
                LiveTaskRevision.task_id == task.id,
                LiveTaskRevision.revision == task.current_revision
            )
            .first()
        )
        if revision:
            task_revision_data = {
                "task_id": task.id,
                "current_revision": task.current_revision,
                "updated_at": to_iso_utc(task.updated_at),
                "payload": json.loads(revision.payload_json)
            }

    return {
        "session": session.id,
        "share_code": session.share_code,
        "status": compute_effective_status(session),
        "created_at": to_iso_utc(session.created_at),
        "last_position_at": to_iso_utc(session.last_position_at),
        "ended_at": to_iso_utc(session.ended_at),
        "latest": latest,
        "positions": [
            {
                "lat": p.lat,
                "lon": p.lon,
                "alt": p.alt,
                "speed": p.speed,
                "heading": p.heading,
                "timestamp": to_iso_utc(p.timestamp)
            }
            for p in reversed(positions)
        ],
        "task": task_revision_data
    }


@app.get("/")
def root():
    return {"status": "XCPro backend running"}


@app.post("/api/v1/session/start")
def start_session():
    db = SessionLocal()
    try:
        session_id = str(uuid.uuid4())
        write_token = generate_write_token()

        share_code = generate_share_code()
        while db.query(LiveSession).filter(LiveSession.share_code == share_code).first():
            share_code = generate_share_code()

        row = LiveSession(
            id=session_id,
            share_code=share_code,
            created_at=datetime.utcnow(),
            status="active",
            last_position_at=None,
            ended_at=None,
            write_token_hash=hash_token(write_token)
        )
        db.add(row)
        db.commit()

        return {
            "session_id": session_id,
            "share_code": share_code,
            "status": "active",
            "write_token": write_token
        }
    finally:
        db.close()


@app.post("/api/v1/position")
def post_position(
    p: Position,
    x_session_token: Optional[str] = Header(default=None, alias="X-Session-Token")
):
    db = SessionLocal()
    try:
        session = get_session_or_404(db, p.session_id)
        require_write_access(session, x_session_token)

        if session.status == "ended":
            raise HTTPException(status_code=409, detail="session already ended")

        position_ts = to_utc_naive(p.timestamp)
        validate_position_payload(p, position_ts)

        last_position = (
            db.query(LivePosition)
            .filter(LivePosition.session_id == p.session_id)
            .order_by(LivePosition.timestamp.desc(), LivePosition.id.desc())
            .first()
        )

        if last_position:
            if position_ts < last_position.timestamp:
                raise HTTPException(status_code=409, detail="out-of-order position timestamp")

            exact_duplicate = (
                position_ts == last_position.timestamp and
                p.lat == last_position.lat and
                p.lon == last_position.lon and
                p.alt == last_position.alt and
                p.speed == last_position.speed and
                p.heading == last_position.heading
            )

            if exact_duplicate:
                return {"ok": True, "deduped": True}

            if position_ts == last_position.timestamp:
                raise HTTPException(status_code=409, detail="conflicting duplicate timestamp")

            delta_seconds = (position_ts - last_position.timestamp).total_seconds()
            if delta_seconds > 0:
                jump_m = haversine_m(last_position.lat, last_position.lon, p.lat, p.lon)
                implied_kmh = (jump_m / delta_seconds) * 3.6
                if implied_kmh > MAX_IMPOSSIBLE_GROUND_SPEED_KMH:
                    raise HTTPException(
                        status_code=400,
                        detail=f"impossible jump detected ({implied_kmh:.1f} km/h)"
                    )

        row = LivePosition(
            session_id=p.session_id,
            lat=p.lat,
            lon=p.lon,
            alt=p.alt,
            speed=p.speed,
            heading=p.heading,
            timestamp=position_ts
        )
        db.add(row)

        session.last_position_at = datetime.utcnow()
        db.commit()

        latest = {
            "lat": p.lat,
            "lon": p.lon,
            "alt": p.alt,
            "speed": p.speed,
            "heading": p.heading,
            "timestamp": to_iso_utc(position_ts)
        }
        redis_client.set(f"live:latest:{p.session_id}", json.dumps(latest))

        return {"ok": True}
    finally:
        db.close()


@app.post("/api/v1/task/upsert")
def task_upsert(
    req: TaskUpsertRequest,
    x_session_token: Optional[str] = Header(default=None, alias="X-Session-Token")
):
    db = SessionLocal()
    try:
        session = get_session_or_404(db, req.session_id)
        require_write_access(session, x_session_token)

        if session.status == "ended":
            raise HTTPException(status_code=409, detail="session already ended")

        task_name = validate_task_payload(req)
        now = datetime.utcnow()

        payload = {
            "task_name": task_name,
            "task": req.task
        }

        task = db.query(LiveTask).filter(LiveTask.session_id == req.session_id).first()

        if not task:
            task = LiveTask(
                id=str(uuid.uuid4()),
                session_id=req.session_id,
                created_at=now,
                updated_at=now,
                current_revision=1
            )
            db.add(task)
            db.flush()

            revision = LiveTaskRevision(
                task_id=task.id,
                revision=1,
                created_at=now,
                payload_json=json.dumps(payload, sort_keys=True)
            )
            db.add(revision)
            db.commit()

            return {
                "ok": True,
                "task_id": task.id,
                "revision": 1
            }

        current_revision = (
            db.query(LiveTaskRevision)
            .filter(
                LiveTaskRevision.task_id == task.id,
                LiveTaskRevision.revision == task.current_revision
            )
            .first()
        )

        if current_revision:
            current_payload = json.loads(current_revision.payload_json)
            if current_payload == payload:
                return {
                    "ok": True,
                    "task_id": task.id,
                    "revision": task.current_revision,
                    "deduped": True
                }

        revision_number = task.current_revision + 1
        task.current_revision = revision_number
        task.updated_at = now

        revision = LiveTaskRevision(
            task_id=task.id,
            revision=revision_number,
            created_at=now,
            payload_json=json.dumps(payload, sort_keys=True)
        )
        db.add(revision)
        db.commit()

        return {
            "ok": True,
            "task_id": task.id,
            "revision": revision_number
        }
    finally:
        db.close()


@app.post("/api/v1/session/end")
def end_session(
    req: SessionEndRequest,
    x_session_token: Optional[str] = Header(default=None, alias="X-Session-Token")
):
    db = SessionLocal()
    try:
        session = get_session_or_404(db, req.session_id)
        require_write_access(session, x_session_token)

        if session.status == "ended":
            return {
                "ok": True,
                "session_id": session.id,
                "status": "ended",
                "ended_at": to_iso_utc(session.ended_at)
            }

        session.status = "ended"
        session.ended_at = datetime.utcnow()
        db.commit()

        return {
            "ok": True,
            "session_id": session.id,
            "status": "ended",
            "ended_at": to_iso_utc(session.ended_at)
        }
    finally:
        db.close()


@app.get("/api/v1/live/{session_id}")
def get_live(session_id: str):
    db = SessionLocal()
    try:
        session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
        if not session:
            raise HTTPException(status_code=404, detail="not found")

        return build_live_response(db, session)
    finally:
        db.close()


@app.get("/api/v1/live/share/{share_code}")
def get_live_by_share_code(share_code: str):
    db = SessionLocal()
    try:
        session = db.query(LiveSession).filter(LiveSession.share_code == share_code).first()
        if not session:
            raise HTTPException(status_code=404, detail="not found")

        return build_live_response(db, session)
    finally:
        db.close()
