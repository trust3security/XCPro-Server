from dataclasses import dataclass
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
import uuid
import os
import json
import random
import string
import secrets
import hashlib
import hmac
import math
import re
import base64

from sqlalchemy import CheckConstraint, Column, DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint, create_engine, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import declarative_base, sessionmaker
import redis

try:
    from google.auth.transport import requests as google_requests
    from google.oauth2 import id_token as google_id_token
except ImportError:
    google_requests = None
    google_id_token = None

try:
    from pydantic import model_validator

    PYDANTIC_V2 = True
except ImportError:
    from pydantic import root_validator

    PYDANTIC_V2 = False

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@db:5432/xcpro")
STALE_AFTER_SECONDS = 120
MAX_POSITION_FUTURE_SKEW_SECONDS = 300
MAX_REASONABLE_ALT_M = 20000
MIN_REASONABLE_ALT_M = -1000
MAX_REASONABLE_SPEED = 1000
MAX_IMPOSSIBLE_GROUND_SPEED_KMH = 500
MAX_TASK_RADIUS_M = 500000
HANDLE_PATTERN = re.compile(r"^[a-z0-9._]{3,24}$")
FOLLOW_REQUEST_STATUS_PENDING = "pending"
FOLLOW_REQUEST_STATUS_ACCEPTED = "accepted"
FOLLOW_REQUEST_STATUS_DECLINED = "declined"
LIVE_VISIBILITY_OFF = "off"
LIVE_VISIBILITY_FOLLOWERS = "followers"
LIVE_VISIBILITY_PUBLIC = "public"
SEARCH_RELATIONSHIP_NONE = "none"
SEARCH_RELATIONSHIP_OUTGOING_PENDING = "outgoing_pending"
SEARCH_RELATIONSHIP_INCOMING_PENDING = "incoming_pending"
SEARCH_RELATIONSHIP_FOLLOWING = "following"
SEARCH_RELATIONSHIP_FOLLOWED_BY = "followed_by"
SEARCH_RELATIONSHIP_MUTUAL = "mutual"
MIN_SEARCH_QUERY_LENGTH = 2
SEARCH_RESULT_LIMIT = 25
PRIVATE_FOLLOW_BEARER_VERSION = 1
DEFAULT_PRIVATE_FOLLOW_BEARER_TTL_SECONDS = 60 * 60 * 24 * 30
DISCOVERABILITY_VALUES = frozenset({"searchable", "hidden"})
FOLLOW_POLICY_VALUES = frozenset({"approval_required", "auto_approve", "closed"})
DEFAULT_LIVE_VISIBILITY_VALUES = frozenset({
    LIVE_VISIBILITY_OFF,
    LIVE_VISIBILITY_FOLLOWERS,
    LIVE_VISIBILITY_PUBLIC
})
CONNECTION_LIST_VISIBILITY_VALUES = frozenset({"owner_only", "mutuals_only", "public"})
DEFAULT_DISCOVERABILITY = "searchable"
DEFAULT_FOLLOW_POLICY = "approval_required"
DEFAULT_LIVE_VISIBILITY = LIVE_VISIBILITY_FOLLOWERS
DEFAULT_CONNECTION_LIST_VISIBILITY = "owner_only"
RUNTIME_ENV_DEV = "dev"
RUNTIME_ENV_STAGING = "staging"
RUNTIME_ENV_PROD = "prod"
PRIVATE_FOLLOW_BOOLEAN_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
PRIVATE_FOLLOW_BOOLEAN_FALSE_VALUES = frozenset({"0", "false", "no", "off"})

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)


class ErrorCode:
    VALIDATION_ERROR = "validation_error"
    UNAUTHENTICATED = "unauthenticated"
    AUTH_UNAVAILABLE = "auth_unavailable"
    INVALID_GOOGLE_ID_TOKEN = "invalid_google_id_token"
    SESSION_NOT_FOUND = "session_not_found"
    MISSING_SESSION_TOKEN = "missing_session_token"
    SESSION_TOKEN_UNAVAILABLE = "session_token_unavailable"
    INVALID_SESSION_TOKEN = "invalid_session_token"
    SESSION_ALREADY_ENDED = "session_already_ended"
    POSITION_COORDINATE_OUT_OF_RANGE = "position_coordinate_out_of_range"
    POSITION_ALT_OUT_OF_RANGE = "position_alt_out_of_range"
    POSITION_SPEED_OUT_OF_RANGE = "position_speed_out_of_range"
    POSITION_HEADING_OUT_OF_RANGE = "position_heading_out_of_range"
    POSITION_TIMESTAMP_IN_FUTURE = "position_timestamp_in_future"
    POSITION_OUT_OF_ORDER = "position_out_of_order"
    POSITION_CONFLICTING_DUPLICATE_TIMESTAMP = "position_conflicting_duplicate_timestamp"
    POSITION_IMPOSSIBLE_JUMP = "position_impossible_jump"
    INVALID_NUMERIC_VALUE = "invalid_numeric_value"
    TASK_NAME_REQUIRED = "task_name_required"
    TASK_TURNPOINTS_INVALID = "task_turnpoints_invalid"
    TASK_TURNPOINT_INVALID = "task_turnpoint_invalid"
    TASK_TURNPOINT_NAME_REQUIRED = "task_turnpoint_name_required"
    TASK_TURNPOINT_TYPE_REQUIRED = "task_turnpoint_type_required"
    TASK_TURNPOINT_COORDINATES_REQUIRED = "task_turnpoint_coordinates_required"
    TASK_COORDINATE_OUT_OF_RANGE = "task_coordinate_out_of_range"
    TASK_RADIUS_OUT_OF_RANGE = "task_radius_out_of_range"
    TASK_BOUNDARY_INVALID = "task_boundary_invalid"
    TASK_BOUNDARY_TYPE_INVALID = "task_boundary_type_invalid"
    TASK_BOUNDARY_RADIUS_OUT_OF_RANGE = "task_boundary_radius_out_of_range"
    TASK_CLEAR_PAYLOAD_INVALID = "task_clear_payload_invalid"
    HANDLE_ALREADY_TAKEN = "handle_already_taken"
    INVALID_HANDLE = "invalid_handle"
    PROFILE_INCOMPLETE = "profile_incomplete"
    INVALID_PRIVACY_SETTING = "invalid_privacy_setting"
    SEARCH_QUERY_TOO_SHORT = "search_query_too_short"
    USER_NOT_FOUND = "user_not_found"
    FOLLOW_REQUEST_SELF = "follow_request_self"
    FOLLOW_REQUEST_ALREADY_EXISTS = "follow_request_already_exists"
    FOLLOW_REQUEST_CLOSED = "follow_request_closed"
    ALREADY_FOLLOWING = "already_following"
    FOLLOW_REQUEST_NOT_FOUND = "follow_request_not_found"
    FOLLOW_REQUEST_NOT_PENDING = "follow_request_not_pending"


POSITION_MONOTONIC_FIELD_NAMES = frozenset({
    "fix_mono_ms",
    "fixMonoMs",
    "monotonic_ms",
    "monotonicMs",
    "monotonic_time_ms",
    "monotonicTimeMs",
    "client_monotonic_ms",
    "clientMonotonicMs"
})


class ApiHTTPException(HTTPException):
    def __init__(self, status_code: int, code: str, detail: Any):
        super().__init__(status_code=status_code, detail=detail)
        self.code = code


def utcnow() -> datetime:
    return datetime.utcnow()


@dataclass(frozen=True)
class ResolvedBearerIdentity:
    provider: str
    provider_subject: str
    email: Optional[str] = None
    display_name: Optional[str] = None


@dataclass(frozen=True)
class PrivateFollowRuntimeConfig:
    runtime_env: str
    allow_static_dev_bearer_auth: bool
    has_static_bearer_tokens_env: bool
    static_bearer_tokens: dict[str, ResolvedBearerIdentity]
    google_server_client_ids: frozenset[str]
    private_follow_bearer_secret: Optional[bytes]
    private_follow_bearer_ttl_seconds: int


def parse_boolean_env(name: str, raw_value: Optional[str], default: bool = False) -> bool:
    normalized = (raw_value or "").strip().lower()
    if not normalized:
        return default
    if normalized in PRIVATE_FOLLOW_BOOLEAN_TRUE_VALUES:
        return True
    if normalized in PRIVATE_FOLLOW_BOOLEAN_FALSE_VALUES:
        return False
    raise RuntimeError(
        f"{name} must be one of {sorted(PRIVATE_FOLLOW_BOOLEAN_TRUE_VALUES | PRIVATE_FOLLOW_BOOLEAN_FALSE_VALUES)}"
    )


def normalize_runtime_env(raw_value: Optional[str]) -> str:
    normalized = (raw_value or "").strip().lower()
    if not normalized:
        return RUNTIME_ENV_PROD
    aliases = {
        "dev": RUNTIME_ENV_DEV,
        "development": RUNTIME_ENV_DEV,
        "local": RUNTIME_ENV_DEV,
        "staging": RUNTIME_ENV_STAGING,
        "stage": RUNTIME_ENV_STAGING,
        "prod": RUNTIME_ENV_PROD,
        "production": RUNTIME_ENV_PROD,
    }
    resolved = aliases.get(normalized)
    if resolved is None:
        raise RuntimeError(
            "XCPRO_RUNTIME_ENV must be one of ['dev', 'staging', 'prod']"
        )
    return resolved


def parse_static_bearer_tokens(raw_value: str) -> dict[str, ResolvedBearerIdentity]:
    if not raw_value.strip():
        return {}

    parsed = json.loads(raw_value)
    if not isinstance(parsed, dict):
        raise RuntimeError("XCPRO_STATIC_BEARER_TOKENS_JSON must be a JSON object")

    token_map: dict[str, ResolvedBearerIdentity] = {}
    for raw_token, raw_identity in parsed.items():
        token = str(raw_token).strip()
        if not token:
            continue

        if isinstance(raw_identity, str):
            provider = "static"
            subject = raw_identity.strip()
            email = None
            display_name = None
        elif isinstance(raw_identity, dict):
            provider = str(raw_identity.get("provider", "static")).strip()
            subject = str(raw_identity.get("subject", "")).strip()
            email = str(raw_identity.get("email", "")).strip() or None
            display_name = str(raw_identity.get("display_name", "")).strip() or None
        else:
            raise RuntimeError("static bearer identity must be a string or object")

        if not provider or not subject:
            raise RuntimeError("static bearer identity must include provider and subject")

        token_map[token] = ResolvedBearerIdentity(
            provider=provider,
            provider_subject=subject,
            email=email,
            display_name=display_name
        )

    return token_map


def is_static_dev_bearer_auth_enabled_for_env(
    env: Optional[dict[str, str]] = None
) -> bool:
    resolved_env = os.environ if env is None else env
    return parse_boolean_env(
        "XCPRO_ALLOW_DEV_STATIC_BEARER_AUTH",
        resolved_env.get("XCPRO_ALLOW_DEV_STATIC_BEARER_AUTH"),
        default=False
    )


def load_google_server_client_ids_from_env(
    env: Optional[dict[str, str]] = None
) -> frozenset[str]:
    resolved_env = os.environ if env is None else env
    raw_values = [
        resolved_env.get("XCPRO_GOOGLE_SERVER_CLIENT_IDS", ""),
        resolved_env.get("XCPRO_GOOGLE_SERVER_CLIENT_ID", "")
    ]
    client_ids = {
        entry.strip()
        for raw_value in raw_values
        for entry in raw_value.split(",")
        if entry.strip()
    }
    return frozenset(client_ids)


def load_private_follow_bearer_secret_from_env(
    env: Optional[dict[str, str]] = None
) -> Optional[bytes]:
    resolved_env = os.environ if env is None else env
    raw_value = resolved_env.get("XCPRO_PRIVATE_FOLLOW_BEARER_SECRET", "").strip()
    if not raw_value:
        return None
    return raw_value.encode("utf-8")


def load_private_follow_bearer_ttl_seconds_from_env(
    env: Optional[dict[str, str]] = None
) -> int:
    resolved_env = os.environ if env is None else env
    return max(
        300,
        int(
            resolved_env.get(
                "XCPRO_PRIVATE_FOLLOW_BEARER_TTL_SECONDS",
                str(DEFAULT_PRIVATE_FOLLOW_BEARER_TTL_SECONDS)
            )
        )
    )


def build_private_follow_runtime_config(
    env: Optional[dict[str, str]] = None
) -> PrivateFollowRuntimeConfig:
    resolved_env = os.environ if env is None else env
    runtime_env = normalize_runtime_env(resolved_env.get("XCPRO_RUNTIME_ENV"))
    allow_static_dev_bearer_auth = is_static_dev_bearer_auth_enabled_for_env(resolved_env)
    raw_static_bearer_tokens = resolved_env.get("XCPRO_STATIC_BEARER_TOKENS_JSON", "").strip()
    has_static_bearer_tokens_env = bool(raw_static_bearer_tokens)
    static_bearer_tokens = {}
    if allow_static_dev_bearer_auth and runtime_env == RUNTIME_ENV_DEV:
        static_bearer_tokens = parse_static_bearer_tokens(raw_static_bearer_tokens)
    return PrivateFollowRuntimeConfig(
        runtime_env=runtime_env,
        allow_static_dev_bearer_auth=allow_static_dev_bearer_auth,
        has_static_bearer_tokens_env=has_static_bearer_tokens_env,
        static_bearer_tokens=static_bearer_tokens,
        google_server_client_ids=load_google_server_client_ids_from_env(resolved_env),
        private_follow_bearer_secret=load_private_follow_bearer_secret_from_env(resolved_env),
        private_follow_bearer_ttl_seconds=load_private_follow_bearer_ttl_seconds_from_env(resolved_env)
    )


def collect_private_follow_runtime_safety_errors(
    config: PrivateFollowRuntimeConfig
) -> list[str]:
    errors: list[str] = []
    if config.allow_static_dev_bearer_auth and config.runtime_env != RUNTIME_ENV_DEV:
        errors.append(
            "XCPRO_ALLOW_DEV_STATIC_BEARER_AUTH is only permitted when XCPRO_RUNTIME_ENV=dev"
        )
    if config.has_static_bearer_tokens_env and config.runtime_env != RUNTIME_ENV_DEV:
        errors.append(
            "XCPRO_STATIC_BEARER_TOKENS_JSON must not be set unless XCPRO_RUNTIME_ENV=dev"
        )
    return errors


def collect_private_follow_preflight_errors(
    config: PrivateFollowRuntimeConfig
) -> list[str]:
    errors = list(collect_private_follow_runtime_safety_errors(config))
    if config.runtime_env in {RUNTIME_ENV_STAGING, RUNTIME_ENV_PROD}:
        if not config.google_server_client_ids:
            errors.append(
                "Missing XCPRO_GOOGLE_SERVER_CLIENT_ID or XCPRO_GOOGLE_SERVER_CLIENT_IDS"
            )
        if config.private_follow_bearer_secret is None:
            errors.append("Missing XCPRO_PRIVATE_FOLLOW_BEARER_SECRET")
    return errors


def collect_private_follow_preflight_warnings(
    config: PrivateFollowRuntimeConfig
) -> list[str]:
    warnings: list[str] = []
    if (
        config.runtime_env == RUNTIME_ENV_DEV and
        config.has_static_bearer_tokens_env and
        not config.allow_static_dev_bearer_auth
    ):
        warnings.append(
            "XCPRO_STATIC_BEARER_TOKENS_JSON is set but ignored until XCPRO_ALLOW_DEV_STATIC_BEARER_AUTH=1"
        )
    if config.runtime_env == RUNTIME_ENV_DEV and not config.google_server_client_ids:
        warnings.append(
            "Google exchange remains unavailable until XCPRO_GOOGLE_SERVER_CLIENT_ID(S) is configured"
        )
    if config.runtime_env == RUNTIME_ENV_DEV and config.private_follow_bearer_secret is None:
        warnings.append(
            "Issued XCPro bearer tokens remain unavailable until XCPRO_PRIVATE_FOLLOW_BEARER_SECRET is configured"
        )
    return warnings


def build_private_follow_preflight_report(
    config: Optional[PrivateFollowRuntimeConfig] = None
) -> dict[str, Any]:
    resolved_config = config or PRIVATE_FOLLOW_RUNTIME_CONFIG
    errors = collect_private_follow_preflight_errors(resolved_config)
    warnings = collect_private_follow_preflight_warnings(resolved_config)
    return {
        "ok": not errors,
        "runtime_env": resolved_config.runtime_env,
        "allow_static_dev_bearer_auth": resolved_config.allow_static_dev_bearer_auth,
        "has_static_bearer_tokens_env": resolved_config.has_static_bearer_tokens_env,
        "active_static_bearer_tokens": len(resolved_config.static_bearer_tokens),
        "has_google_server_client_ids": bool(resolved_config.google_server_client_ids),
        "has_private_follow_bearer_secret": resolved_config.private_follow_bearer_secret is not None,
        "private_follow_bearer_ttl_seconds": resolved_config.private_follow_bearer_ttl_seconds,
        "errors": errors,
        "warnings": warnings,
    }


def assert_private_follow_runtime_safety(
    config: PrivateFollowRuntimeConfig
) -> None:
    errors = collect_private_follow_runtime_safety_errors(config)
    if errors:
        raise RuntimeError("Unsafe private-follow auth configuration: " + "; ".join(errors))


PRIVATE_FOLLOW_RUNTIME_CONFIG = build_private_follow_runtime_config()
assert_private_follow_runtime_safety(PRIVATE_FOLLOW_RUNTIME_CONFIG)
STATIC_BEARER_TOKENS = PRIVATE_FOLLOW_RUNTIME_CONFIG.static_bearer_tokens
GOOGLE_SERVER_CLIENT_IDS = PRIVATE_FOLLOW_RUNTIME_CONFIG.google_server_client_ids
PRIVATE_FOLLOW_BEARER_SECRET = PRIVATE_FOLLOW_RUNTIME_CONFIG.private_follow_bearer_secret
PRIVATE_FOLLOW_BEARER_TTL_SECONDS = PRIVATE_FOLLOW_RUNTIME_CONFIG.private_follow_bearer_ttl_seconds


def base64url_encode(raw_bytes: bytes) -> str:
    return base64.urlsafe_b64encode(raw_bytes).decode("ascii").rstrip("=")


def base64url_decode(raw_value: str) -> bytes:
    padding = "=" * (-len(raw_value) % 4)
    return base64.urlsafe_b64decode(f"{raw_value}{padding}")


def issue_private_follow_bearer(identity: ResolvedBearerIdentity) -> str:
    if PRIVATE_FOLLOW_BEARER_SECRET is None:
        raise ApiHTTPException(
            status_code=503,
            code=ErrorCode.AUTH_UNAVAILABLE,
            detail="private-follow bearer secret is not configured"
        )

    issued_at = int(utcnow().timestamp())
    expires_at = issued_at + PRIVATE_FOLLOW_BEARER_TTL_SECONDS
    payload = {
        "v": PRIVATE_FOLLOW_BEARER_VERSION,
        "provider": identity.provider,
        "sub": identity.provider_subject,
        "email": identity.email,
        "display_name": identity.display_name,
        "iat": issued_at,
        "exp": expires_at
    }
    payload_json = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    payload_segment = base64url_encode(payload_json)
    signature = hmac.new(
        PRIVATE_FOLLOW_BEARER_SECRET,
        payload_segment.encode("ascii"),
        hashlib.sha256
    ).digest()
    return f"xcps.{payload_segment}.{base64url_encode(signature)}"


def verify_private_follow_bearer(token: str) -> Optional[ResolvedBearerIdentity]:
    if PRIVATE_FOLLOW_BEARER_SECRET is None or not token.startswith("xcps."):
        return None

    parts = token.split(".")
    if len(parts) != 3 or parts[0] != "xcps":
        return None

    payload_segment = parts[1].strip()
    signature_segment = parts[2].strip()
    if not payload_segment or not signature_segment:
        return None

    expected_signature = base64url_encode(
        hmac.new(
            PRIVATE_FOLLOW_BEARER_SECRET,
            payload_segment.encode("ascii"),
            hashlib.sha256
        ).digest()
    )
    if not secrets.compare_digest(signature_segment, expected_signature):
        return None

    try:
        payload = json.loads(base64url_decode(payload_segment).decode("utf-8"))
    except Exception:
        return None

    if payload.get("v") != PRIVATE_FOLLOW_BEARER_VERSION:
        return None

    expires_at = int(payload.get("exp", 0))
    if expires_at <= int(utcnow().timestamp()):
        return None

    provider = str(payload.get("provider", "")).strip()
    provider_subject = str(payload.get("sub", "")).strip()
    if not provider or not provider_subject:
        return None

    email = str(payload.get("email", "")).strip() or None
    display_name = str(payload.get("display_name", "")).strip() or None
    return ResolvedBearerIdentity(
        provider=provider,
        provider_subject=provider_subject,
        email=email,
        display_name=display_name
    )


PRIVATE_FOLLOW_BEARER_TOKEN_VERIFIER = verify_private_follow_bearer


def verify_google_id_token_for_exchange(token: str) -> Optional[ResolvedBearerIdentity]:
    if google_id_token is None or google_requests is None or not GOOGLE_SERVER_CLIENT_IDS:
        return None

    try:
        decoded_token = google_id_token.verify_oauth2_token(
            token,
            google_requests.Request(),
            audience=None
        )
    except Exception:
        return None

    audience = str(decoded_token.get("aud", "")).strip()
    if audience not in GOOGLE_SERVER_CLIENT_IDS:
        return None

    provider_subject = str(decoded_token.get("sub") or "").strip()
    if not provider_subject:
        return None

    issuer = str(decoded_token.get("iss", "")).strip()
    if issuer not in {"accounts.google.com", "https://accounts.google.com"}:
        return None

    email = str(decoded_token.get("email", "")).strip() or None
    display_name = str(decoded_token.get("name", "")).strip() or None
    return ResolvedBearerIdentity(
        provider="google",
        provider_subject=provider_subject,
        email=email,
        display_name=display_name
    )


GOOGLE_ID_TOKEN_VERIFIER = verify_google_id_token_for_exchange


def reject_monotonic_position_fields(payload: Any) -> Any:
    if not isinstance(payload, dict):
        return payload

    monotonic_fields = sorted(POSITION_MONOTONIC_FIELD_NAMES.intersection(payload.keys()))
    if monotonic_fields:
        field_list = ", ".join(monotonic_fields)
        raise ValueError(
            f"client monotonic time is not accepted on the wire ({field_list})"
        )
    return payload


@app.exception_handler(ApiHTTPException)
def api_http_exception_handler(_request: Request, exc: ApiHTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "code": exc.code,
            "detail": exc.detail
        }
    )


@app.exception_handler(RequestValidationError)
def request_validation_exception_handler(_request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=422,
        content={
            "code": ErrorCode.VALIDATION_ERROR,
            "detail": jsonable_encoder(exc.errors())
        }
    )


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


def parse_number(value, field_name: str, code: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        raise ApiHTTPException(status_code=400, code=code, detail=f"{field_name} must be numeric")


def validate_lat_lon(lat: float, lon: float, field_prefix: str, code: str) -> None:
    if lat < -90 or lat > 90:
        raise ApiHTTPException(status_code=400, code=code, detail=f"{field_prefix}.lat out of range")
    if lon < -180 or lon > 180:
        raise ApiHTTPException(status_code=400, code=code, detail=f"{field_prefix}.lon out of range")


def validate_radius(radius_value, field_name: str, code: str) -> None:
    radius = parse_number(radius_value, field_name, code)
    if radius <= 0 or radius > MAX_TASK_RADIUS_M:
        raise ApiHTTPException(status_code=400, code=code, detail=f"{field_name} out of range")


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
    __table_args__ = (
        CheckConstraint(
            "visibility IN ('off', 'followers', 'public')",
            name="ck_live_sessions_visibility"
        ),
    )

    id = Column(String, primary_key=True, index=True)
    share_code = Column(String, unique=True, index=True, nullable=False)
    owner_user_id = Column(String, ForeignKey("users.id"), index=True, nullable=True)
    visibility = Column(
        String(24),
        nullable=False,
        default=LIVE_VISIBILITY_PUBLIC,
        server_default=text("'public'")
    )
    created_at = Column(DateTime, nullable=False)
    status = Column(
    String(20),
    nullable=False,
    default="active",
    server_default=text("'active'"))
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
    agl_meters = Column(Float, nullable=True)
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


class User(Base):
    __tablename__ = "users"

    id = Column(String, primary_key=True, index=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)


class AuthIdentity(Base):
    __tablename__ = "auth_identities"
    __table_args__ = (
        UniqueConstraint("provider", "provider_subject", name="uq_auth_identities_provider_subject"),
    )

    id = Column(String, primary_key=True, index=True)
    user_id = Column(String, ForeignKey("users.id"), index=True, nullable=False)
    provider = Column(String(40), nullable=False)
    provider_subject = Column(String(255), nullable=False)
    provider_email = Column(String(255), nullable=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    last_seen_at = Column(DateTime, nullable=False)


class PilotProfile(Base):
    __tablename__ = "pilot_profiles"
    __table_args__ = (
        UniqueConstraint("handle_normalized", name="uq_pilot_profiles_handle_normalized"),
    )

    user_id = Column(String, ForeignKey("users.id"), primary_key=True)
    handle = Column(String(24), nullable=True)
    handle_normalized = Column(String(24), index=True, nullable=True)
    display_name = Column(String(80), nullable=True)
    comp_number = Column(String(24), nullable=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)


class PrivacySetting(Base):
    __tablename__ = "privacy_settings"
    __table_args__ = (
        CheckConstraint(
            "discoverability IN ('searchable', 'hidden')",
            name="ck_privacy_settings_discoverability"
        ),
        CheckConstraint(
            "follow_policy IN ('approval_required', 'auto_approve', 'closed')",
            name="ck_privacy_settings_follow_policy"
        ),
        CheckConstraint(
            "default_live_visibility IN ('off', 'followers', 'public')",
            name="ck_privacy_settings_default_live_visibility"
        ),
        CheckConstraint(
            "connection_list_visibility IN ('owner_only', 'mutuals_only', 'public')",
            name="ck_privacy_settings_connection_list_visibility"
        ),
    )

    user_id = Column(String, ForeignKey("users.id"), primary_key=True)
    discoverability = Column(String(24), nullable=False)
    follow_policy = Column(String(32), nullable=False)
    default_live_visibility = Column(String(24), nullable=False)
    connection_list_visibility = Column(String(24), nullable=False)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)


class FollowRequest(Base):
    __tablename__ = "follow_requests"
    __table_args__ = (
        UniqueConstraint(
            "requester_user_id",
            "target_user_id",
            name="uq_follow_requests_requester_target"
        ),
        CheckConstraint(
            "status IN ('pending', 'accepted', 'declined')",
            name="ck_follow_requests_status"
        ),
    )

    id = Column(String, primary_key=True, index=True)
    requester_user_id = Column(String, ForeignKey("users.id"), index=True, nullable=False)
    target_user_id = Column(String, ForeignKey("users.id"), index=True, nullable=False)
    status = Column(String(24), nullable=False)
    responded_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)


class FollowEdge(Base):
    __tablename__ = "follow_edges"

    follower_user_id = Column(String, ForeignKey("users.id"), primary_key=True)
    followed_user_id = Column(String, ForeignKey("users.id"), primary_key=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)


class Position(BaseModel):
    """Deployed telemetry ingest contract.

    `speed` is XCPro groundSpeedMs in meters per second.
    `timestamp` is client wall-clock time in a UTC/ISO-8601-compatible format.
    Client monotonic timestamps stay transport-local and are not part of this wire DTO.
    """

    session_id: str
    lat: float
    lon: float
    alt: float
    agl_meters: Optional[float] = Field(
        default=None,
        description="Optional XCPro AGL height in meters."
    )
    speed: float = Field(description="XCPro groundSpeedMs in meters per second.")
    heading: float
    timestamp: datetime = Field(
        description="Client wall-clock time in UTC/ISO-8601-compatible format."
    )

    if PYDANTIC_V2:
        @model_validator(mode="before")
        @classmethod
        def validate_wire_contract(cls, payload: Any):
            return reject_monotonic_position_fields(payload)
    else:
        @root_validator(pre=True)
        def validate_wire_contract(cls, payload):
            return reject_monotonic_position_fields(payload)


class TaskUpsertRequest(BaseModel):
    session_id: str
    clear_task: bool = False
    task_name: Optional[str] = None
    task: Optional[dict] = None


class SessionEndRequest(BaseModel):
    session_id: str


class LiveSessionStartRequest(BaseModel):
    visibility: Optional[str] = None


class LiveSessionVisibilityPatchRequest(BaseModel):
    visibility: str


class MeProfilePatchRequest(BaseModel):
    handle: Optional[str] = None
    display_name: Optional[str] = None
    comp_number: Optional[str] = None


class MePrivacyPatchRequest(BaseModel):
    discoverability: Optional[str] = None
    follow_policy: Optional[str] = None
    default_live_visibility: Optional[str] = None
    connection_list_visibility: Optional[str] = None


class GoogleAuthExchangeRequest(BaseModel):
    google_id_token: str


class FollowRequestCreateRequest(BaseModel):
    target_user_id: str


@dataclass
class CurrentUserRecord:
    user: User
    auth_identity: AuthIdentity
    profile: PilotProfile
    privacy: PrivacySetting


@dataclass(frozen=True)
class RelationshipLookup:
    outgoing_pending: frozenset[str]
    incoming_pending: frozenset[str]
    following: frozenset[str]
    followed_by: frozenset[str]


def requested_fields(model: BaseModel) -> set[str]:
    if PYDANTIC_V2:
        return set(model.model_fields_set)
    return set(model.__fields_set__)


def parse_bearer_token(authorization: Optional[str]) -> str:
    if authorization is None or not authorization.strip():
        raise ApiHTTPException(
            status_code=401,
            code=ErrorCode.UNAUTHENTICATED,
            detail="missing Authorization header"
        )

    parts = authorization.strip().split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer" or not parts[1].strip():
        raise ApiHTTPException(
            status_code=401,
            code=ErrorCode.UNAUTHENTICATED,
            detail="invalid bearer token"
        )
    return parts[1].strip()


def resolve_bearer_identity(authorization: Optional[str]) -> ResolvedBearerIdentity:
    token = parse_bearer_token(authorization)
    identity = STATIC_BEARER_TOKENS.get(token)
    if identity is None:
        identity = PRIVATE_FOLLOW_BEARER_TOKEN_VERIFIER(token)
    if identity is None:
        raise ApiHTTPException(
            status_code=401,
            code=ErrorCode.UNAUTHENTICATED,
            detail="invalid bearer token"
        )
    return identity


def trim_to_none(raw_value: Optional[str]) -> Optional[str]:
    if raw_value is None:
        return None
    trimmed = raw_value.strip()
    return trimmed or None


def normalize_handle(raw_handle: Optional[str]) -> str:
    normalized = (raw_handle or "").strip().lower()
    if not HANDLE_PATTERN.fullmatch(normalized):
        raise ApiHTTPException(
            status_code=422,
            code=ErrorCode.INVALID_HANDLE,
            detail="handle must be 3-24 chars of lowercase letters, digits, underscore, or dot"
        )
    return normalized


def normalize_display_name(raw_display_name: Optional[str]) -> str:
    trimmed = trim_to_none(raw_display_name)
    if trimmed is None:
        raise ApiHTTPException(
            status_code=422,
            code=ErrorCode.PROFILE_INCOMPLETE,
            detail="display_name is required"
        )
    return trimmed


def normalize_search_query(raw_query: Optional[str]) -> str:
    normalized = (raw_query or "").strip().lower()
    if len(normalized) < MIN_SEARCH_QUERY_LENGTH:
        raise ApiHTTPException(
            status_code=422,
            code=ErrorCode.SEARCH_QUERY_TOO_SHORT,
            detail=f"q must be at least {MIN_SEARCH_QUERY_LENGTH} characters"
        )
    return normalized


def validate_privacy_value(field_name: str, value: str) -> str:
    allowed_values = {
        "discoverability": DISCOVERABILITY_VALUES,
        "follow_policy": FOLLOW_POLICY_VALUES,
        "default_live_visibility": DEFAULT_LIVE_VISIBILITY_VALUES,
        "connection_list_visibility": CONNECTION_LIST_VISIBILITY_VALUES,
    }[field_name]
    if value not in allowed_values:
        raise ApiHTTPException(
            status_code=422,
            code=ErrorCode.INVALID_PRIVACY_SETTING,
            detail=f"{field_name} must be one of {sorted(allowed_values)}"
        )
    return value


def ensure_current_user_record(
    db,
    authorization: Optional[str]
) -> CurrentUserRecord:
    identity = resolve_bearer_identity(authorization)
    return ensure_current_user_record_for_identity(db, identity)


def ensure_current_user_record_for_identity(
    db,
    identity: ResolvedBearerIdentity
) -> CurrentUserRecord:
    now = utcnow()

    auth_identity = (
        db.query(AuthIdentity)
        .filter(
            AuthIdentity.provider == identity.provider,
            AuthIdentity.provider_subject == identity.provider_subject
        )
        .first()
    )

    if auth_identity is None:
        user = User(
            id=str(uuid.uuid4()),
            created_at=now,
            updated_at=now
        )
        db.add(user)
        db.flush()

        auth_identity = AuthIdentity(
            id=str(uuid.uuid4()),
            user_id=user.id,
            provider=identity.provider,
            provider_subject=identity.provider_subject,
            provider_email=trim_to_none(identity.email),
            created_at=now,
            updated_at=now,
            last_seen_at=now
        )
        db.add(auth_identity)

        profile = PilotProfile(
            user_id=user.id,
            handle=None,
            handle_normalized=None,
            display_name=trim_to_none(identity.display_name),
            comp_number=None,
            created_at=now,
            updated_at=now
        )
        privacy = PrivacySetting(
            user_id=user.id,
            discoverability=DEFAULT_DISCOVERABILITY,
            follow_policy=DEFAULT_FOLLOW_POLICY,
            default_live_visibility=DEFAULT_LIVE_VISIBILITY,
            connection_list_visibility=DEFAULT_CONNECTION_LIST_VISIBILITY,
            created_at=now,
            updated_at=now
        )
        db.add(profile)
        db.add(privacy)
        db.commit()
        return CurrentUserRecord(
            user=user,
            auth_identity=auth_identity,
            profile=profile,
            privacy=privacy
        )

    user = db.query(User).filter(User.id == auth_identity.user_id).first()
    if user is None:
        raise ApiHTTPException(
            status_code=401,
            code=ErrorCode.UNAUTHENTICATED,
            detail="invalid bearer token"
        )

    profile = db.query(PilotProfile).filter(PilotProfile.user_id == user.id).first()
    if profile is None:
        profile = PilotProfile(
            user_id=user.id,
            handle=None,
            handle_normalized=None,
            display_name=trim_to_none(identity.display_name),
            comp_number=None,
            created_at=now,
            updated_at=now
        )
        db.add(profile)

    privacy = db.query(PrivacySetting).filter(PrivacySetting.user_id == user.id).first()
    if privacy is None:
        privacy = PrivacySetting(
            user_id=user.id,
            discoverability=DEFAULT_DISCOVERABILITY,
            follow_policy=DEFAULT_FOLLOW_POLICY,
            default_live_visibility=DEFAULT_LIVE_VISIBILITY,
            connection_list_visibility=DEFAULT_CONNECTION_LIST_VISIBILITY,
            created_at=now,
            updated_at=now
        )
        db.add(privacy)

    user.updated_at = now
    auth_identity.provider_email = trim_to_none(identity.email)
    auth_identity.updated_at = now
    auth_identity.last_seen_at = now
    db.commit()
    return CurrentUserRecord(
        user=user,
        auth_identity=auth_identity,
        profile=profile,
        privacy=privacy
    )


def build_profile_response(profile: PilotProfile) -> dict[str, Optional[str]]:
    return {
        "user_id": profile.user_id,
        "handle": profile.handle,
        "display_name": profile.display_name,
        "comp_number": profile.comp_number,
    }


def build_user_summary(profile: PilotProfile) -> dict[str, Optional[str]]:
    return build_profile_response(profile)


def build_privacy_response(privacy: PrivacySetting) -> dict[str, str]:
    return {
        "discoverability": privacy.discoverability,
        "follow_policy": privacy.follow_policy,
        "default_live_visibility": privacy.default_live_visibility,
        "connection_list_visibility": privacy.connection_list_visibility,
    }


def build_me_response(current_user: CurrentUserRecord) -> dict[str, Any]:
    response = build_profile_response(current_user.profile)
    response["privacy"] = build_privacy_response(current_user.privacy)
    return response


def build_google_auth_exchange_response(
    current_user: CurrentUserRecord,
    access_token: str
) -> dict[str, Any]:
    expires_at = utcnow() + timedelta(seconds=PRIVATE_FOLLOW_BEARER_TTL_SECONDS)
    return {
        "access_token": access_token,
        "token_type": "Bearer",
        "auth_method": "google",
        "user_id": current_user.user.id,
        "expires_at": to_iso_utc(expires_at)
    }


def ensure_profile_complete(
    profile: PilotProfile,
    detail: str = "handle and display_name are required"
) -> None:
    if profile.handle is None or profile.display_name is None:
        raise ApiHTTPException(
            status_code=422,
            code=ErrorCode.PROFILE_INCOMPLETE,
            detail=detail
        )


def build_relationship_state(
    lookup: RelationshipLookup,
    other_user_id: str
) -> str:
    if other_user_id in lookup.outgoing_pending:
        return SEARCH_RELATIONSHIP_OUTGOING_PENDING
    if other_user_id in lookup.incoming_pending:
        return SEARCH_RELATIONSHIP_INCOMING_PENDING

    current_follows_other = other_user_id in lookup.following
    other_follows_current = other_user_id in lookup.followed_by
    if current_follows_other and other_follows_current:
        return SEARCH_RELATIONSHIP_MUTUAL
    if current_follows_other:
        return SEARCH_RELATIONSHIP_FOLLOWING
    if other_follows_current:
        return SEARCH_RELATIONSHIP_FOLLOWED_BY
    return SEARCH_RELATIONSHIP_NONE


def load_relationship_lookup(
    db,
    current_user_id: str,
    other_user_ids: list[str]
) -> RelationshipLookup:
    if not other_user_ids:
        empty = frozenset()
        return RelationshipLookup(
            outgoing_pending=empty,
            incoming_pending=empty,
            following=empty,
            followed_by=empty
        )

    outgoing_pending = frozenset(
        row[0]
        for row in (
            db.query(FollowRequest.target_user_id)
            .filter(
                FollowRequest.requester_user_id == current_user_id,
                FollowRequest.status == FOLLOW_REQUEST_STATUS_PENDING,
                FollowRequest.target_user_id.in_(other_user_ids)
            )
            .all()
        )
    )
    incoming_pending = frozenset(
        row[0]
        for row in (
            db.query(FollowRequest.requester_user_id)
            .filter(
                FollowRequest.target_user_id == current_user_id,
                FollowRequest.status == FOLLOW_REQUEST_STATUS_PENDING,
                FollowRequest.requester_user_id.in_(other_user_ids)
            )
            .all()
        )
    )
    following = frozenset(
        row[0]
        for row in (
            db.query(FollowEdge.followed_user_id)
            .filter(
                FollowEdge.follower_user_id == current_user_id,
                FollowEdge.followed_user_id.in_(other_user_ids)
            )
            .all()
        )
    )
    followed_by = frozenset(
        row[0]
        for row in (
            db.query(FollowEdge.follower_user_id)
            .filter(
                FollowEdge.followed_user_id == current_user_id,
                FollowEdge.follower_user_id.in_(other_user_ids)
            )
            .all()
        )
    )
    return RelationshipLookup(
        outgoing_pending=outgoing_pending,
        incoming_pending=incoming_pending,
        following=following,
        followed_by=followed_by
    )


def get_searchable_target_or_404(db, user_id: str) -> tuple[PilotProfile, PrivacySetting]:
    profile = db.query(PilotProfile).filter(PilotProfile.user_id == user_id).first()
    if profile is None:
        raise ApiHTTPException(
            status_code=404,
            code=ErrorCode.USER_NOT_FOUND,
            detail="user not found"
        )
    ensure_profile_complete(profile)
    privacy = db.query(PrivacySetting).filter(PrivacySetting.user_id == user_id).first()
    if privacy is None:
        now = utcnow()
        privacy = PrivacySetting(
            user_id=user_id,
            discoverability=DEFAULT_DISCOVERABILITY,
            follow_policy=DEFAULT_FOLLOW_POLICY,
            default_live_visibility=DEFAULT_LIVE_VISIBILITY,
            connection_list_visibility=DEFAULT_CONNECTION_LIST_VISIBILITY,
            created_at=now,
            updated_at=now
        )
        db.add(privacy)
        db.flush()
    return profile, privacy


def ensure_follow_edge(
    db,
    follower_user_id: str,
    followed_user_id: str,
    now: datetime
) -> None:
    edge = (
        db.query(FollowEdge)
        .filter(
            FollowEdge.follower_user_id == follower_user_id,
            FollowEdge.followed_user_id == followed_user_id
        )
        .first()
    )
    if edge is None:
        db.add(
            FollowEdge(
                follower_user_id=follower_user_id,
                followed_user_id=followed_user_id,
                created_at=now,
                updated_at=now
            )
        )
        return
    edge.updated_at = now


def build_search_result_response(
    profile: PilotProfile,
    lookup: RelationshipLookup
) -> dict[str, Optional[str]]:
    response = build_user_summary(profile)
    response["relationship_state"] = build_relationship_state(lookup, profile.user_id)
    return response


def build_follow_request_response(
    current_user_id: str,
    follow_request: FollowRequest,
    counterpart_profile: PilotProfile,
    lookup: RelationshipLookup
) -> dict[str, Any]:
    direction = (
        "outgoing"
        if follow_request.requester_user_id == current_user_id
        else "incoming"
    )
    return {
        "request_id": follow_request.id,
        "status": follow_request.status,
        "direction": direction,
        "created_at": to_iso_utc(follow_request.created_at),
        "updated_at": to_iso_utc(follow_request.updated_at),
        "counterpart": build_user_summary(counterpart_profile),
        "relationship_state": build_relationship_state(lookup, counterpart_profile.user_id)
    }


def apply_profile_patch(
    profile: PilotProfile,
    request: MeProfilePatchRequest
) -> None:
    fields = requested_fields(request)
    if "handle" in fields:
        if request.handle is None:
            raise ApiHTTPException(
                status_code=422,
                code=ErrorCode.INVALID_HANDLE,
                detail="handle is required"
            )
        normalized_handle = normalize_handle(request.handle)
        profile.handle = normalized_handle
        profile.handle_normalized = normalized_handle

    if "display_name" in fields:
        profile.display_name = normalize_display_name(request.display_name)

    if "comp_number" in fields:
        profile.comp_number = trim_to_none(request.comp_number)

    ensure_profile_complete(profile)

    profile.updated_at = utcnow()


def apply_privacy_patch(
    privacy: PrivacySetting,
    request: MePrivacyPatchRequest
) -> None:
    fields = requested_fields(request)
    if "discoverability" in fields:
        if request.discoverability is None:
            raise ApiHTTPException(
                status_code=422,
                code=ErrorCode.INVALID_PRIVACY_SETTING,
                detail="discoverability is required"
            )
        privacy.discoverability = validate_privacy_value(
            "discoverability",
            request.discoverability.strip()
        )
    if "follow_policy" in fields:
        if request.follow_policy is None:
            raise ApiHTTPException(
                status_code=422,
                code=ErrorCode.INVALID_PRIVACY_SETTING,
                detail="follow_policy is required"
            )
        privacy.follow_policy = validate_privacy_value(
            "follow_policy",
            request.follow_policy.strip()
        )
    if "default_live_visibility" in fields:
        if request.default_live_visibility is None:
            raise ApiHTTPException(
                status_code=422,
                code=ErrorCode.INVALID_PRIVACY_SETTING,
                detail="default_live_visibility is required"
            )
        privacy.default_live_visibility = validate_privacy_value(
            "default_live_visibility",
            request.default_live_visibility.strip()
        )
    if "connection_list_visibility" in fields:
        if request.connection_list_visibility is None:
            raise ApiHTTPException(
                status_code=422,
                code=ErrorCode.INVALID_PRIVACY_SETTING,
                detail="connection_list_visibility is required"
            )
        privacy.connection_list_visibility = validate_privacy_value(
            "connection_list_visibility",
            request.connection_list_visibility.strip()
        )
    privacy.updated_at = utcnow()


def get_session_or_404(db, session_id: str) -> LiveSession:
    session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
    if not session:
        raise ApiHTTPException(
            status_code=404,
            code=ErrorCode.SESSION_NOT_FOUND,
            detail="session not found"
        )
    return session


def require_write_access(session: LiveSession, x_session_token: Optional[str]) -> None:
    if not x_session_token:
        raise ApiHTTPException(
            status_code=401,
            code=ErrorCode.MISSING_SESSION_TOKEN,
            detail="missing X-Session-Token header"
        )

    if not session.write_token_hash:
        raise ApiHTTPException(
            status_code=403,
            code=ErrorCode.SESSION_TOKEN_UNAVAILABLE,
            detail="write token unavailable for this session"
        )

    if not secrets.compare_digest(session.write_token_hash, hash_token(x_session_token)):
        raise ApiHTTPException(
            status_code=403,
            code=ErrorCode.INVALID_SESSION_TOKEN,
            detail="invalid session token"
        )


def compute_effective_status(session: LiveSession) -> str:
    if session.status == "ended":
        return "ended"

    if session.last_position_at is None:
        return "active"

    age = utcnow() - session.last_position_at
    if age > timedelta(seconds=STALE_AFTER_SECONDS):
        return "stale"

    return "active"


def validate_live_visibility(raw_value: Optional[str]) -> str:
    normalized = (raw_value or "").strip()
    if normalized not in DEFAULT_LIVE_VISIBILITY_VALUES:
        raise ApiHTTPException(
            status_code=422,
            code=ErrorCode.INVALID_PRIVACY_SETTING,
            detail=f"visibility must be one of {sorted(DEFAULT_LIVE_VISIBILITY_VALUES)}"
        )
    return normalized


def effective_live_visibility(session: LiveSession) -> str:
    return (session.visibility or LIVE_VISIBILITY_PUBLIC).strip() or LIVE_VISIBILITY_PUBLIC


def public_share_code_for(session: LiveSession) -> Optional[str]:
    if effective_live_visibility(session) != LIVE_VISIBILITY_PUBLIC:
        return None
    return session.share_code


def is_public_live_session(session: LiveSession) -> bool:
    return effective_live_visibility(session) == LIVE_VISIBILITY_PUBLIC


def can_user_view_live_session(
    db,
    viewer_user_id: str,
    session: LiveSession
) -> bool:
    owner_user_id = session.owner_user_id
    if owner_user_id == viewer_user_id:
        return True

    visibility = effective_live_visibility(session)
    if visibility == LIVE_VISIBILITY_PUBLIC:
        return True
    if owner_user_id is None:
        return False
    if visibility != LIVE_VISIBILITY_FOLLOWERS:
        return False

    existing_edge = (
        db.query(FollowEdge)
        .filter(
            FollowEdge.follower_user_id == viewer_user_id,
            FollowEdge.followed_user_id == owner_user_id
        )
        .first()
    )
    return existing_edge is not None


def get_owned_live_session_or_404(
    db,
    session_id: str,
    owner_user_id: str
) -> LiveSession:
    session = (
        db.query(LiveSession)
        .filter(
            LiveSession.id == session_id,
            LiveSession.owner_user_id == owner_user_id
        )
        .first()
    )
    if session is None:
        raise ApiHTTPException(
            status_code=404,
            code=ErrorCode.SESSION_NOT_FOUND,
            detail="session not found"
        )
    return session


def select_latest_owned_live_session(
    db,
    owner_user_id: str
) -> Optional[LiveSession]:
    return (
        db.query(LiveSession)
        .filter(
            LiveSession.owner_user_id == owner_user_id,
            LiveSession.status != "ended"
        )
        .order_by(
            LiveSession.last_position_at.desc(),
            LiveSession.created_at.desc(),
            LiveSession.id.asc()
        )
        .first()
    )


def end_active_owned_live_sessions(
    db,
    owner_user_id: str,
    now: datetime
) -> None:
    sessions = (
        db.query(LiveSession)
        .filter(
            LiveSession.owner_user_id == owner_user_id,
            LiveSession.status != "ended"
        )
        .all()
    )
    for session in sessions:
        session.status = "ended"
        session.ended_at = now


def build_live_session_command_response(
    session: LiveSession,
    write_token: Optional[str] = None
) -> dict[str, Any]:
    response = {
        "session_id": session.id,
        "status": compute_effective_status(session),
        "visibility": effective_live_visibility(session),
        "owner_user_id": session.owner_user_id,
        "share_code": public_share_code_for(session)
    }
    if write_token is not None:
        response["write_token"] = write_token
    return response


def validate_position_payload(p: Position, position_ts: datetime) -> None:
    validate_lat_lon(
        p.lat,
        p.lon,
        "position",
        ErrorCode.POSITION_COORDINATE_OUT_OF_RANGE
    )

    if p.alt < MIN_REASONABLE_ALT_M or p.alt > MAX_REASONABLE_ALT_M:
        raise ApiHTTPException(
            status_code=400,
            code=ErrorCode.POSITION_ALT_OUT_OF_RANGE,
            detail="position.alt out of range"
        )

    if p.speed < 0 or p.speed > MAX_REASONABLE_SPEED:
        raise ApiHTTPException(
            status_code=400,
            code=ErrorCode.POSITION_SPEED_OUT_OF_RANGE,
            detail="position.speed out of range"
        )

    if p.heading < 0 or p.heading > 360:
        raise ApiHTTPException(
            status_code=400,
            code=ErrorCode.POSITION_HEADING_OUT_OF_RANGE,
            detail="position.heading out of range"
        )

    if position_ts > utcnow() + timedelta(seconds=MAX_POSITION_FUTURE_SKEW_SECONDS):
        raise ApiHTTPException(
            status_code=400,
            code=ErrorCode.POSITION_TIMESTAMP_IN_FUTURE,
            detail="position.timestamp too far in the future"
        )


def validate_task_payload(req: TaskUpsertRequest) -> dict:
    if req.clear_task:
        if req.task_name is not None or req.task is not None:
            raise ApiHTTPException(
                status_code=400,
                code=ErrorCode.TASK_CLEAR_PAYLOAD_INVALID,
                detail="clear_task cannot be combined with task_name or task"
            )
        return {"clear_task": True}

    task_name = (req.task_name or "").strip()
    if not task_name:
        raise ApiHTTPException(
            status_code=400,
            code=ErrorCode.TASK_NAME_REQUIRED,
            detail="task_name is required"
        )

    task = req.task or {}
    turnpoints = task.get("turnpoints")

    if not isinstance(turnpoints, list) or len(turnpoints) < 2:
        raise ApiHTTPException(
            status_code=400,
            code=ErrorCode.TASK_TURNPOINTS_INVALID,
            detail="task.turnpoints must contain at least 2 items"
        )

    for idx, tp in enumerate(turnpoints):
        if not isinstance(tp, dict):
            raise ApiHTTPException(
                status_code=400,
                code=ErrorCode.TASK_TURNPOINT_INVALID,
                detail=f"task.turnpoints[{idx}] must be an object"
            )

        name = str(tp.get("name", "")).strip()
        tp_type = str(tp.get("type", "")).strip()

        if not name:
            raise ApiHTTPException(
                status_code=400,
                code=ErrorCode.TASK_TURNPOINT_NAME_REQUIRED,
                detail=f"task.turnpoints[{idx}].name is required"
            )
        if not tp_type:
            raise ApiHTTPException(
                status_code=400,
                code=ErrorCode.TASK_TURNPOINT_TYPE_REQUIRED,
                detail=f"task.turnpoints[{idx}].type is required"
            )
        if "lat" not in tp or "lon" not in tp:
            raise ApiHTTPException(
                status_code=400,
                code=ErrorCode.TASK_TURNPOINT_COORDINATES_REQUIRED,
                detail=f"task.turnpoints[{idx}] requires lat/lon"
            )

        lat = parse_number(
            tp.get("lat"),
            f"task.turnpoints[{idx}].lat",
            ErrorCode.INVALID_NUMERIC_VALUE
        )
        lon = parse_number(
            tp.get("lon"),
            f"task.turnpoints[{idx}].lon",
            ErrorCode.INVALID_NUMERIC_VALUE
        )
        validate_lat_lon(
            lat,
            lon,
            f"task.turnpoints[{idx}]",
            ErrorCode.TASK_COORDINATE_OUT_OF_RANGE
        )

        if "radius_m" in tp and tp.get("radius_m") is not None:
            validate_radius(
                tp.get("radius_m"),
                f"task.turnpoints[{idx}].radius_m",
                ErrorCode.TASK_RADIUS_OUT_OF_RANGE
            )

    for boundary_name in ["start", "finish"]:
        boundary = task.get(boundary_name)
        if boundary is None:
            continue

        if not isinstance(boundary, dict):
            raise ApiHTTPException(
                status_code=400,
                code=ErrorCode.TASK_BOUNDARY_INVALID,
                detail=f"task.{boundary_name} must be an object"
            )

        if "type" in boundary and not str(boundary.get("type", "")).strip():
            raise ApiHTTPException(
                status_code=400,
                code=ErrorCode.TASK_BOUNDARY_TYPE_INVALID,
                detail=f"task.{boundary_name}.type is invalid"
            )

        if "radius_m" in boundary and boundary.get("radius_m") is not None:
            validate_radius(
                boundary.get("radius_m"),
                f"task.{boundary_name}.radius_m",
                ErrorCode.TASK_BOUNDARY_RADIUS_OUT_OF_RANGE
            )

    return {
        "task_name": task_name,
        "task": task
    }


def payload_clears_task(payload: Any) -> bool:
    return isinstance(payload, dict) and payload.get("clear_task") is True


def get_cached_latest(session_id: str) -> Optional[dict]:
    latest_raw = redis_client.get(f"live:latest:{session_id}")
    if not latest_raw:
        return None

    latest = json.loads(latest_raw)
    if isinstance(latest, dict) and "agl_meters" not in latest:
        latest["agl_meters"] = None
    return latest


def build_live_list_display_label(session: LiveSession) -> str:
    # Public UI label only. Share code is server-owned; no stronger identity is implied.
    return f"Live {session.share_code}"


def build_live_active_item(session: LiveSession) -> dict:
    return {
        "session_id": session.id,
        "share_code": public_share_code_for(session),
        "status": compute_effective_status(session),
        "created_at": to_iso_utc(session.created_at),
        "last_position_at": to_iso_utc(session.last_position_at),
        "latest": get_cached_latest(session.id),
        "display_label": build_live_list_display_label(session)
    }


def build_live_owner_display_label(
    profile: Optional[PilotProfile],
    session: LiveSession
) -> str:
    if profile is not None:
        if profile.display_name:
            return profile.display_name
        if profile.handle:
            return profile.handle
    public_share_code = public_share_code_for(session)
    if public_share_code:
        return f"Live {public_share_code}"
    if session.owner_user_id:
        return session.owner_user_id
    return session.id


def build_authorized_live_active_item(
    session: LiveSession,
    owner_profile: Optional[PilotProfile]
) -> dict[str, Any]:
    return {
        "session_id": session.id,
        "user_id": session.owner_user_id,
        "visibility": effective_live_visibility(session),
        "share_code": public_share_code_for(session),
        "status": compute_effective_status(session),
        "created_at": to_iso_utc(session.created_at),
        "last_position_at": to_iso_utc(session.last_position_at),
        "latest": get_cached_latest(session.id),
        "display_label": build_live_owner_display_label(owner_profile, session),
        "profile": build_profile_response(owner_profile) if owner_profile is not None else None
    }


def build_live_response(
    db,
    session: LiveSession,
    owner_profile: Optional[PilotProfile] = None
):
    latest = get_cached_latest(session.id)

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
            payload = json.loads(revision.payload_json)
            if not payload_clears_task(payload):
                task_revision_data = {
                    "task_id": task.id,
                    "current_revision": task.current_revision,
                    "updated_at": to_iso_utc(task.updated_at),
                    "payload": payload
                }

    return {
        "session": session.id,
        "share_code": public_share_code_for(session),
        "status": compute_effective_status(session),
        "visibility": effective_live_visibility(session),
        "owner_user_id": session.owner_user_id,
        "display_label": build_live_owner_display_label(owner_profile, session),
        "profile": build_profile_response(owner_profile) if owner_profile is not None else None,
        "created_at": to_iso_utc(session.created_at),
        "last_position_at": to_iso_utc(session.last_position_at),
        "ended_at": to_iso_utc(session.ended_at),
        "latest": latest,
        "positions": [
            {
                "lat": p.lat,
                "lon": p.lon,
                "alt": p.alt,
                "agl_meters": p.agl_meters,
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
            owner_user_id=None,
            visibility=LIVE_VISIBILITY_PUBLIC,
            created_at=utcnow(),
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
            raise ApiHTTPException(
                status_code=409,
                code=ErrorCode.SESSION_ALREADY_ENDED,
                detail="session already ended"
            )

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
                raise ApiHTTPException(
                    status_code=409,
                    code=ErrorCode.POSITION_OUT_OF_ORDER,
                    detail="out-of-order position timestamp"
                )

            exact_duplicate = (
                position_ts == last_position.timestamp and
                p.lat == last_position.lat and
                p.lon == last_position.lon and
                p.alt == last_position.alt and
                p.agl_meters == last_position.agl_meters and
                p.speed == last_position.speed and
                p.heading == last_position.heading
            )

            if exact_duplicate:
                return {"ok": True, "deduped": True}

            if position_ts == last_position.timestamp:
                raise ApiHTTPException(
                    status_code=409,
                    code=ErrorCode.POSITION_CONFLICTING_DUPLICATE_TIMESTAMP,
                    detail="conflicting duplicate timestamp"
                )

            delta_seconds = (position_ts - last_position.timestamp).total_seconds()
            if delta_seconds > 0:
                jump_m = haversine_m(last_position.lat, last_position.lon, p.lat, p.lon)
                implied_kmh = (jump_m / delta_seconds) * 3.6
                if implied_kmh > MAX_IMPOSSIBLE_GROUND_SPEED_KMH:
                    raise ApiHTTPException(
                        status_code=400,
                        code=ErrorCode.POSITION_IMPOSSIBLE_JUMP,
                        detail=f"impossible jump detected ({implied_kmh:.1f} km/h)"
                    )

        row = LivePosition(
            session_id=p.session_id,
            lat=p.lat,
            lon=p.lon,
            alt=p.alt,
            agl_meters=p.agl_meters,
            speed=p.speed,
            heading=p.heading,
            timestamp=position_ts
        )
        db.add(row)

        session.last_position_at = utcnow()
        db.commit()

        latest = {
            "lat": p.lat,
            "lon": p.lon,
            "alt": p.alt,
            "agl_meters": p.agl_meters,
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
            raise ApiHTTPException(
                status_code=409,
                code=ErrorCode.SESSION_ALREADY_ENDED,
                detail="session already ended"
            )

        payload = validate_task_payload(req)
        now = utcnow()
        cleared = payload_clears_task(payload)

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
                "revision": 1,
                "cleared": cleared
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
                    "deduped": True,
                    "cleared": payload_clears_task(current_payload)
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
            "revision": revision_number,
            "cleared": cleared
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
        session.ended_at = utcnow()
        db.commit()

        return {
            "ok": True,
            "session_id": session.id,
            "status": "ended",
            "ended_at": to_iso_utc(session.ended_at)
        }
    finally:
        db.close()


@app.post("/api/v2/live/session/start")
def start_authenticated_live_session(
    request: Optional[LiveSessionStartRequest] = None,
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        visibility = validate_live_visibility(
            request.visibility
            if request is not None and request.visibility is not None
            else current_user.privacy.default_live_visibility
        )
        now = utcnow()
        end_active_owned_live_sessions(db, current_user.user.id, now)

        session_id = str(uuid.uuid4())
        write_token = generate_write_token()
        share_code = generate_share_code()
        while db.query(LiveSession).filter(LiveSession.share_code == share_code).first():
            share_code = generate_share_code()

        row = LiveSession(
            id=session_id,
            share_code=share_code,
            owner_user_id=current_user.user.id,
            visibility=visibility,
            created_at=now,
            status="active",
            last_position_at=None,
            ended_at=None,
            write_token_hash=hash_token(write_token)
        )
        db.add(row)
        db.commit()

        return build_live_session_command_response(row, write_token=write_token)
    finally:
        db.close()


@app.patch("/api/v2/live/session/{session_id}/visibility")
def patch_authenticated_live_session_visibility(
    session_id: str,
    request: LiveSessionVisibilityPatchRequest,
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        session = get_owned_live_session_or_404(db, session_id, current_user.user.id)
        if session.status == "ended":
            raise ApiHTTPException(
                status_code=409,
                code=ErrorCode.SESSION_ALREADY_ENDED,
                detail="session already ended"
            )
        session.visibility = validate_live_visibility(request.visibility)
        db.commit()
        return build_live_session_command_response(session)
    finally:
        db.close()


@app.post("/api/v2/auth/google/exchange")
def exchange_google_auth_token(
    request: GoogleAuthExchangeRequest
):
    trimmed_token = request.google_id_token.strip()
    if not trimmed_token:
        raise ApiHTTPException(
            status_code=422,
            code=ErrorCode.INVALID_GOOGLE_ID_TOKEN,
            detail="google_id_token is required"
        )

    if not GOOGLE_SERVER_CLIENT_IDS:
        raise ApiHTTPException(
            status_code=503,
            code=ErrorCode.AUTH_UNAVAILABLE,
            detail="Google server client ID is not configured"
        )

    identity = GOOGLE_ID_TOKEN_VERIFIER(trimmed_token)
    if identity is None:
        raise ApiHTTPException(
            status_code=401,
            code=ErrorCode.INVALID_GOOGLE_ID_TOKEN,
            detail="invalid Google ID token"
        )

    db = SessionLocal()
    try:
        current_user = ensure_current_user_record_for_identity(db, identity)
        access_token = issue_private_follow_bearer(identity)
        return build_google_auth_exchange_response(current_user, access_token)
    finally:
        db.close()


@app.get("/api/v2/me")
def get_current_user_me(
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        return build_me_response(current_user)
    finally:
        db.close()


@app.patch("/api/v2/me/profile")
def patch_current_user_profile(
    request: MeProfilePatchRequest,
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        apply_profile_patch(current_user.profile, request)
        try:
            db.commit()
        except IntegrityError:
            db.rollback()
            raise ApiHTTPException(
                status_code=409,
                code=ErrorCode.HANDLE_ALREADY_TAKEN,
                detail="handle already taken"
            )
        return build_profile_response(current_user.profile)
    finally:
        db.close()


@app.patch("/api/v2/me/privacy")
def patch_current_user_privacy(
    request: MePrivacyPatchRequest,
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        apply_privacy_patch(current_user.privacy, request)
        db.commit()
        return build_privacy_response(current_user.privacy)
    finally:
        db.close()


@app.get("/api/v2/users/search")
def search_private_follow_users(
    q: str,
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        normalized_query = normalize_search_query(q)
        matching_profiles = (
            db.query(PilotProfile)
            .join(PrivacySetting, PrivacySetting.user_id == PilotProfile.user_id)
            .filter(
                PilotProfile.user_id != current_user.user.id,
                PilotProfile.handle_normalized.isnot(None),
                PilotProfile.display_name.isnot(None),
                PrivacySetting.discoverability == DEFAULT_DISCOVERABILITY,
                PilotProfile.handle_normalized.like(f"%{normalized_query}%")
            )
            .all()
        )
        ordered_profiles = sorted(
            matching_profiles,
            key=lambda profile: (
                0 if profile.handle_normalized == normalized_query else 1,
                0 if profile.handle_normalized.startswith(normalized_query) else 1,
                profile.handle_normalized or "",
                profile.user_id
            )
        )[:SEARCH_RESULT_LIMIT]
        lookup = load_relationship_lookup(
            db,
            current_user.user.id,
            [profile.user_id for profile in ordered_profiles]
        )
        return {
            "users": [
                build_search_result_response(profile, lookup)
                for profile in ordered_profiles
            ]
        }
    finally:
        db.close()


@app.post("/api/v2/follow-requests")
def create_follow_request(
    request: FollowRequestCreateRequest,
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        ensure_profile_complete(
            current_user.profile,
            detail="complete your profile before sending follow requests"
        )
        target_user_id = request.target_user_id.strip()
        if not target_user_id:
            raise ApiHTTPException(
                status_code=422,
                code=ErrorCode.VALIDATION_ERROR,
                detail="target_user_id is required"
            )
        if target_user_id == current_user.user.id:
            raise ApiHTTPException(
                status_code=422,
                code=ErrorCode.FOLLOW_REQUEST_SELF,
                detail="cannot follow yourself"
            )

        target_profile, target_privacy = get_searchable_target_or_404(db, target_user_id)
        existing_edge = (
            db.query(FollowEdge)
            .filter(
                FollowEdge.follower_user_id == current_user.user.id,
                FollowEdge.followed_user_id == target_user_id
            )
            .first()
        )
        if existing_edge is not None:
            raise ApiHTTPException(
                status_code=409,
                code=ErrorCode.ALREADY_FOLLOWING,
                detail="already following"
            )
        if target_privacy.follow_policy == "closed":
            raise ApiHTTPException(
                status_code=409,
                code=ErrorCode.FOLLOW_REQUEST_CLOSED,
                detail="this pilot is not accepting followers"
            )

        follow_request = (
            db.query(FollowRequest)
            .filter(
                FollowRequest.requester_user_id == current_user.user.id,
                FollowRequest.target_user_id == target_user_id
            )
            .first()
        )
        if follow_request is not None and follow_request.status == FOLLOW_REQUEST_STATUS_PENDING:
            raise ApiHTTPException(
                status_code=409,
                code=ErrorCode.FOLLOW_REQUEST_ALREADY_EXISTS,
                detail="follow request already pending"
            )

        now = utcnow()
        final_status = (
            FOLLOW_REQUEST_STATUS_ACCEPTED
            if target_privacy.follow_policy == "auto_approve"
            else FOLLOW_REQUEST_STATUS_PENDING
        )
        responded_at = now if final_status == FOLLOW_REQUEST_STATUS_ACCEPTED else None
        if follow_request is None:
            follow_request = FollowRequest(
                id=str(uuid.uuid4()),
                requester_user_id=current_user.user.id,
                target_user_id=target_user_id,
                status=final_status,
                responded_at=responded_at,
                created_at=now,
                updated_at=now
            )
            db.add(follow_request)
        else:
            follow_request.status = final_status
            follow_request.responded_at = responded_at
            follow_request.created_at = now
            follow_request.updated_at = now

        if final_status == FOLLOW_REQUEST_STATUS_ACCEPTED:
            ensure_follow_edge(db, current_user.user.id, target_user_id, now)

        db.commit()
        lookup = load_relationship_lookup(db, current_user.user.id, [target_user_id])
        return build_follow_request_response(
            current_user.user.id,
            follow_request,
            target_profile,
            lookup
        )
    finally:
        db.close()


@app.get("/api/v2/follow-requests/incoming")
def list_incoming_follow_requests(
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        request_rows = (
            db.query(FollowRequest, PilotProfile)
            .join(PilotProfile, PilotProfile.user_id == FollowRequest.requester_user_id)
            .filter(
                FollowRequest.target_user_id == current_user.user.id,
                FollowRequest.status == FOLLOW_REQUEST_STATUS_PENDING
            )
            .order_by(FollowRequest.updated_at.desc(), FollowRequest.id.desc())
            .all()
        )
        counterpart_ids = [profile.user_id for _request, profile in request_rows]
        lookup = load_relationship_lookup(db, current_user.user.id, counterpart_ids)
        return {
            "requests": [
                build_follow_request_response(
                    current_user.user.id,
                    follow_request,
                    profile,
                    lookup
                )
                for follow_request, profile in request_rows
            ]
        }
    finally:
        db.close()


@app.get("/api/v2/follow-requests/outgoing")
def list_outgoing_follow_requests(
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        request_rows = (
            db.query(FollowRequest, PilotProfile)
            .join(PilotProfile, PilotProfile.user_id == FollowRequest.target_user_id)
            .filter(
                FollowRequest.requester_user_id == current_user.user.id,
                FollowRequest.status == FOLLOW_REQUEST_STATUS_PENDING
            )
            .order_by(FollowRequest.updated_at.desc(), FollowRequest.id.desc())
            .all()
        )
        counterpart_ids = [profile.user_id for _request, profile in request_rows]
        lookup = load_relationship_lookup(db, current_user.user.id, counterpart_ids)
        return {
            "requests": [
                build_follow_request_response(
                    current_user.user.id,
                    follow_request,
                    profile,
                    lookup
                )
                for follow_request, profile in request_rows
            ]
        }
    finally:
        db.close()


def get_follow_request_for_target_or_404(
    db,
    request_id: str,
    target_user_id: str
) -> FollowRequest:
    follow_request = (
        db.query(FollowRequest)
        .filter(
            FollowRequest.id == request_id,
            FollowRequest.target_user_id == target_user_id
        )
        .first()
    )
    if follow_request is None:
        raise ApiHTTPException(
            status_code=404,
            code=ErrorCode.FOLLOW_REQUEST_NOT_FOUND,
            detail="follow request not found"
        )
    return follow_request


@app.post("/api/v2/follow-requests/{request_id}/accept")
def accept_follow_request(
    request_id: str,
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        follow_request = get_follow_request_for_target_or_404(
            db,
            request_id,
            current_user.user.id
        )
        if follow_request.status != FOLLOW_REQUEST_STATUS_PENDING:
            raise ApiHTTPException(
                status_code=409,
                code=ErrorCode.FOLLOW_REQUEST_NOT_PENDING,
                detail="follow request is not pending"
            )

        now = utcnow()
        follow_request.status = FOLLOW_REQUEST_STATUS_ACCEPTED
        follow_request.responded_at = now
        follow_request.updated_at = now
        ensure_follow_edge(
            db,
            follow_request.requester_user_id,
            follow_request.target_user_id,
            now
        )
        db.commit()

        requester_profile, _requester_privacy = get_searchable_target_or_404(
            db,
            follow_request.requester_user_id
        )
        lookup = load_relationship_lookup(
            db,
            current_user.user.id,
            [follow_request.requester_user_id]
        )
        return build_follow_request_response(
            current_user.user.id,
            follow_request,
            requester_profile,
            lookup
        )
    finally:
        db.close()


@app.post("/api/v2/follow-requests/{request_id}/decline")
def decline_follow_request(
    request_id: str,
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        follow_request = get_follow_request_for_target_or_404(
            db,
            request_id,
            current_user.user.id
        )
        if follow_request.status != FOLLOW_REQUEST_STATUS_PENDING:
            raise ApiHTTPException(
                status_code=409,
                code=ErrorCode.FOLLOW_REQUEST_NOT_PENDING,
                detail="follow request is not pending"
            )

        now = utcnow()
        follow_request.status = FOLLOW_REQUEST_STATUS_DECLINED
        follow_request.responded_at = now
        follow_request.updated_at = now
        db.commit()

        requester_profile, _requester_privacy = get_searchable_target_or_404(
            db,
            follow_request.requester_user_id
        )
        lookup = load_relationship_lookup(
            db,
            current_user.user.id,
            [follow_request.requester_user_id]
        )
        return build_follow_request_response(
            current_user.user.id,
            follow_request,
            requester_profile,
            lookup
        )
    finally:
        db.close()


@app.get("/api/v2/live/following/active")
def get_following_active_live_sessions(
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        rows = (
            db.query(LiveSession, PilotProfile)
            .join(
                FollowEdge,
                FollowEdge.followed_user_id == LiveSession.owner_user_id
            )
            .outerjoin(PilotProfile, PilotProfile.user_id == LiveSession.owner_user_id)
            .filter(
                FollowEdge.follower_user_id == current_user.user.id,
                LiveSession.owner_user_id.isnot(None),
                LiveSession.owner_user_id != current_user.user.id,
                LiveSession.status != "ended",
                LiveSession.last_position_at.isnot(None),
                LiveSession.visibility.in_(
                    [LIVE_VISIBILITY_FOLLOWERS, LIVE_VISIBILITY_PUBLIC]
                )
            )
            .order_by(
                LiveSession.last_position_at.desc(),
                LiveSession.created_at.desc(),
                LiveSession.id.asc()
            )
            .all()
        )
        return {
            "items": [
                build_authorized_live_active_item(session, owner_profile)
                for session, owner_profile in rows
            ],
            "generated_at": to_iso_utc(utcnow())
        }
    finally:
        db.close()


@app.get("/api/v2/live/users/{user_id}")
def get_live_session_for_user(
    user_id: str,
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        session = select_latest_owned_live_session(db, user_id)
        if session is None or not can_user_view_live_session(db, current_user.user.id, session):
            raise ApiHTTPException(
                status_code=404,
                code=ErrorCode.SESSION_NOT_FOUND,
                detail="not found"
            )
        owner_profile = (
            db.query(PilotProfile)
            .filter(PilotProfile.user_id == session.owner_user_id)
            .first()
        )
        return build_live_response(db, session, owner_profile)
    finally:
        db.close()


@app.get("/api/v2/live/session/{session_id}")
def get_authenticated_live_session(
    session_id: str,
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        session = get_session_or_404(db, session_id)
        if not can_user_view_live_session(db, current_user.user.id, session):
            raise ApiHTTPException(
                status_code=404,
                code=ErrorCode.SESSION_NOT_FOUND,
                detail="not found"
            )
        owner_profile = (
            db.query(PilotProfile)
            .filter(PilotProfile.user_id == session.owner_user_id)
            .first()
        )
        return build_live_response(db, session, owner_profile)
    finally:
        db.close()


@app.get("/api/v1/live/active")
def get_active_live_sessions():
    db = SessionLocal()
    try:
        # Conservative inclusion: only sessions with at least one accepted position are listed.
        sessions = (
            db.query(LiveSession)
            .filter(
                LiveSession.status != "ended",
                LiveSession.visibility == LIVE_VISIBILITY_PUBLIC,
                LiveSession.last_position_at.isnot(None)
            )
            .order_by(
                LiveSession.last_position_at.desc(),
                LiveSession.created_at.desc(),
                LiveSession.id.asc()
            )
            .all()
        )

        return [build_live_active_item(session) for session in sessions]
    finally:
        db.close()


@app.get("/api/v1/live/{session_id}")
def get_live(session_id: str):
    db = SessionLocal()
    try:
        session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
        if not session or not is_public_live_session(session):
            raise ApiHTTPException(
                status_code=404,
                code=ErrorCode.SESSION_NOT_FOUND,
                detail="not found"
            )

        return build_live_response(db, session)
    finally:
        db.close()


@app.get("/api/v1/live/share/{share_code}")
def get_live_by_share_code(share_code: str):
    db = SessionLocal()
    try:
        session = (
            db.query(LiveSession)
            .filter(
                LiveSession.share_code == share_code,
                LiveSession.visibility == LIVE_VISIBILITY_PUBLIC
            )
            .first()
        )
        if not session:
            raise ApiHTTPException(
                status_code=404,
                code=ErrorCode.SESSION_NOT_FOUND,
                detail="not found"
            )

        return build_live_response(db, session)
    finally:
        db.close()
