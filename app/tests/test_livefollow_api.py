import unittest
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app import main as main_module


class FakeRedis:
    def __init__(self):
        self.values = {}

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value):
        self.values[key] = value


class MutableClock:
    def __init__(self, current: datetime):
        self.current = current

    def utcnow(self) -> datetime:
        return self.current

    def set(self, current: datetime):
        self.current = current

    def advance(self, **kwargs):
        self.current += timedelta(**kwargs)


class LiveFollowApiTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool
        )
        self.session_local = sessionmaker(bind=self.engine)
        main_module.Base.metadata.create_all(bind=self.engine)

        self.original_session_local = main_module.SessionLocal
        self.original_redis_client = main_module.redis_client
        self.original_utcnow = main_module.utcnow

        main_module.SessionLocal = self.session_local
        main_module.redis_client = FakeRedis()
        self.clock = MutableClock(datetime(2026, 3, 20, 12, 0, 0))
        main_module.utcnow = self.clock.utcnow

        self.client = TestClient(main_module.app)

    def tearDown(self):
        self.client.close()
        main_module.SessionLocal = self.original_session_local
        main_module.redis_client = self.original_redis_client
        main_module.utcnow = self.original_utcnow
        main_module.Base.metadata.drop_all(bind=self.engine)
        self.engine.dispose()

    def test_missing_token_returns_stable_error_code(self):
        session = self.start_session()

        response = self.client.post(
            "/api/v1/position",
            json=self.position_payload(session["session_id"])
        )

        self.assertEqual(401, response.status_code)
        self.assertEqual(
            {
                "code": main_module.ErrorCode.MISSING_SESSION_TOKEN,
                "detail": "missing X-Session-Token header"
            },
            response.json()
        )

    def test_session_not_found_returns_stable_error_code(self):
        response = self.client.post(
            "/api/v1/position",
            json=self.position_payload("missing-session"),
            headers={"X-Session-Token": "irrelevant"}
        )

        self.assertEqual(404, response.status_code)
        self.assertEqual(
            {
                "code": main_module.ErrorCode.SESSION_NOT_FOUND,
                "detail": "session not found"
            },
            response.json()
        )

    def test_invalid_session_token_returns_stable_error_code(self):
        session = self.start_session()

        response = self.client.post(
            "/api/v1/position",
            json=self.position_payload(session["session_id"]),
            headers={"X-Session-Token": "wrong-token"}
        )

        self.assertEqual(403, response.status_code)
        self.assertEqual(
            {
                "code": main_module.ErrorCode.INVALID_SESSION_TOKEN,
                "detail": "invalid session token"
            },
            response.json()
        )

    def test_ended_session_rejects_position_writes_and_end_is_idempotent(self):
        session = self.start_session()
        headers = self.write_headers(session)

        first_end = self.client.post(
            "/api/v1/session/end",
            json={"session_id": session["session_id"]},
            headers=headers
        )
        second_end = self.client.post(
            "/api/v1/session/end",
            json={"session_id": session["session_id"]},
            headers=headers
        )
        rejected_write = self.client.post(
            "/api/v1/position",
            json=self.position_payload(session["session_id"]),
            headers=headers
        )

        self.assertEqual(200, first_end.status_code)
        self.assertEqual(200, second_end.status_code)
        self.assertEqual("ended", first_end.json()["status"])
        self.assertEqual(first_end.json()["ended_at"], second_end.json()["ended_at"])
        self.assertEqual(409, rejected_write.status_code)
        self.assertEqual(
            {
                "code": main_module.ErrorCode.SESSION_ALREADY_ENDED,
                "detail": "session already ended"
            },
            rejected_write.json()
        )

    def test_position_ordering_and_duplicate_conflict_return_stable_codes(self):
        session = self.start_session()
        headers = self.write_headers(session)

        first_timestamp = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
        accepted = self.client.post(
            "/api/v1/position",
            json=self.position_payload(session["session_id"], timestamp=first_timestamp),
            headers=headers
        )
        out_of_order = self.client.post(
            "/api/v1/position",
            json=self.position_payload(
                session["session_id"],
                timestamp=first_timestamp - timedelta(seconds=1)
            ),
            headers=headers
        )
        conflicting_duplicate = self.client.post(
            "/api/v1/position",
            json=self.position_payload(
                session["session_id"],
                timestamp=first_timestamp,
                alt=501.0
            ),
            headers=headers
        )

        self.assertEqual(200, accepted.status_code)
        self.assertEqual(409, out_of_order.status_code)
        self.assertEqual(
            {
                "code": main_module.ErrorCode.POSITION_OUT_OF_ORDER,
                "detail": "out-of-order position timestamp"
            },
            out_of_order.json()
        )
        self.assertEqual(409, conflicting_duplicate.status_code)
        self.assertEqual(
            {
                "code": main_module.ErrorCode.POSITION_CONFLICTING_DUPLICATE_TIMESTAMP,
                "detail": "conflicting duplicate timestamp"
            },
            conflicting_duplicate.json()
        )

    def test_validation_errors_include_top_level_code_and_default_detail_shape(self):
        session = self.start_session()

        response = self.client.post(
            "/api/v1/position",
            json={
                "session_id": session["session_id"],
                "lat": -33.9,
                "lon": 151.2,
                "alt": 500.0,
                "speed": 12.5,
                "heading": 180.0
            },
            headers=self.write_headers(session)
        )

        self.assertEqual(422, response.status_code)
        body = response.json()
        self.assertEqual(main_module.ErrorCode.VALIDATION_ERROR, body["code"])
        self.assertIsInstance(body["detail"], list)
        self.assertTrue(any("timestamp" in str(item["loc"]) for item in body["detail"]))

    def test_task_validation_errors_include_stable_codes(self):
        session = self.start_session()
        headers = self.write_headers(session)

        test_cases = [
            (
                {
                    "session_id": session["session_id"],
                    "task_name": "",
                    "task": {
                        "turnpoints": [
                            {"name": "Start", "type": "start", "lat": -33.9, "lon": 151.2},
                            {"name": "Finish", "type": "finish", "lat": -33.8, "lon": 151.3}
                        ]
                    }
                },
                400,
                main_module.ErrorCode.TASK_NAME_REQUIRED,
                "task_name is required"
            ),
            (
                {
                    "session_id": session["session_id"],
                    "task_name": "Task",
                    "task": {
                        "turnpoints": [
                            {"name": "Only", "type": "turn", "lat": -33.9, "lon": 151.2}
                        ]
                    }
                },
                400,
                main_module.ErrorCode.TASK_TURNPOINTS_INVALID,
                "task.turnpoints must contain at least 2 items"
            )
        ]

        for payload, status_code, code, detail in test_cases:
            with self.subTest(code=code):
                response = self.client.post(
                    "/api/v1/task/upsert",
                    json=payload,
                    headers=headers
                )
                self.assertEqual(status_code, response.status_code)
                self.assertEqual({"code": code, "detail": detail}, response.json())

    def test_lifecycle_status_transitions_active_stale_ended(self):
        session = self.start_session()
        headers = self.write_headers(session)

        position_response = self.client.post(
            "/api/v1/position",
            json=self.position_payload(session["session_id"]),
            headers=headers
        )
        self.assertEqual(200, position_response.status_code)

        active = self.client.get(f"/api/v1/live/{session['session_id']}")
        self.assertEqual(200, active.status_code)
        self.assertEqual("active", active.json()["status"])

        self.clock.advance(seconds=main_module.STALE_AFTER_SECONDS + 1)
        stale = self.client.get(f"/api/v1/live/{session['session_id']}")
        self.assertEqual(200, stale.status_code)
        self.assertEqual("stale", stale.json()["status"])

        ended = self.client.post(
            "/api/v1/session/end",
            json={"session_id": session["session_id"]},
            headers=headers
        )
        self.assertEqual(200, ended.status_code)

        live_after_end = self.client.get(f"/api/v1/live/{session['session_id']}")
        self.assertEqual(200, live_after_end.status_code)
        self.assertEqual("ended", live_after_end.json()["status"])

    def test_active_pilots_list_returns_active_sessions_with_expected_fields(self):
        session = self.start_session()

        position_response = self.client.post(
            "/api/v1/position",
            json=self.position_payload(session["session_id"]),
            headers=self.write_headers(session)
        )
        self.assertEqual(200, position_response.status_code)

        response = self.client.get("/api/v1/live/active")
        self.assertEqual(200, response.status_code)

        body = response.json()
        self.assertEqual(1, len(body))

        item = body[0]
        self.assertEqual(
            {
                "session_id",
                "share_code",
                "status",
                "created_at",
                "last_position_at",
                "latest",
                "display_label"
            },
            set(item.keys())
        )
        self.assertEqual(session["session_id"], item["session_id"])
        self.assertEqual(session["share_code"], item["share_code"])
        self.assertEqual("active", item["status"])
        self.assertEqual(f"Live {session['share_code']}", item["display_label"])
        self.assertIsNotNone(item["created_at"])
        self.assertIsNotNone(item["last_position_at"])
        self.assertEqual(12.5, item["latest"]["speed"])
        self.assertEqual("2026-03-20T12:00:00+00:00", item["latest"]["timestamp"])

    def test_active_pilots_list_excludes_ended_and_never_started_sessions(self):
        idle_session = self.start_session()
        active_session = self.start_session()
        ended_session = self.start_session()

        active_position = self.client.post(
            "/api/v1/position",
            json=self.position_payload(active_session["session_id"]),
            headers=self.write_headers(active_session)
        )
        ended_position = self.client.post(
            "/api/v1/position",
            json=self.position_payload(ended_session["session_id"]),
            headers=self.write_headers(ended_session)
        )
        ended_response = self.client.post(
            "/api/v1/session/end",
            json={"session_id": ended_session["session_id"]},
            headers=self.write_headers(ended_session)
        )

        self.assertEqual(200, active_position.status_code)
        self.assertEqual(200, ended_position.status_code)
        self.assertEqual(200, ended_response.status_code)

        response = self.client.get("/api/v1/live/active")
        self.assertEqual(200, response.status_code)

        session_ids = [item["session_id"] for item in response.json()]
        self.assertIn(active_session["session_id"], session_ids)
        self.assertNotIn(idle_session["session_id"], session_ids)
        self.assertNotIn(ended_session["session_id"], session_ids)

    def test_active_pilots_list_preserves_stale_status(self):
        session = self.start_session()

        position_response = self.client.post(
            "/api/v1/position",
            json=self.position_payload(session["session_id"]),
            headers=self.write_headers(session)
        )
        self.assertEqual(200, position_response.status_code)

        self.clock.advance(seconds=main_module.STALE_AFTER_SECONDS + 1)

        response = self.client.get("/api/v1/live/active")
        self.assertEqual(200, response.status_code)
        self.assertEqual(1, len(response.json()))
        self.assertEqual("stale", response.json()[0]["status"])

    def test_active_pilots_list_keeps_session_when_latest_cache_is_missing(self):
        session = self.start_session()

        position_response = self.client.post(
            "/api/v1/position",
            json=self.position_payload(session["session_id"]),
            headers=self.write_headers(session)
        )
        self.assertEqual(200, position_response.status_code)

        main_module.redis_client.values.pop(f"live:latest:{session['session_id']}", None)

        response = self.client.get("/api/v1/live/active")
        self.assertEqual(200, response.status_code)
        self.assertEqual(1, len(response.json()))

        item = response.json()[0]
        self.assertEqual(session["session_id"], item["session_id"])
        self.assertIsNone(item["latest"])
        self.assertIsNotNone(item["last_position_at"])

    def test_position_wire_contract_preserves_ground_speed_ms_and_wall_clock_timestamp(self):
        session = self.start_session()
        headers = self.write_headers(session)

        response = self.client.post(
            "/api/v1/position",
            json=self.position_payload(
                session["session_id"],
                timestamp=datetime(2026, 3, 20, 13, 0, 0, tzinfo=timezone(timedelta(hours=1))),
                speed=12.5
            ),
            headers=headers
        )
        self.assertEqual(200, response.status_code)

        live = self.client.get(f"/api/v1/live/{session['session_id']}")
        self.assertEqual(200, live.status_code)
        self.assertEqual(12.5, live.json()["latest"]["speed"])
        self.assertEqual(
            "2026-03-20T12:00:00+00:00",
            live.json()["latest"]["timestamp"]
        )
        self.assertEqual(12.5, live.json()["positions"][0]["speed"])

    def test_position_rejects_client_monotonic_time_fields(self):
        session = self.start_session()

        response = self.client.post(
            "/api/v1/position",
            json=self.position_payload(
                session["session_id"],
                fix_mono_ms=123456789
            ),
            headers=self.write_headers(session)
        )

        self.assertEqual(422, response.status_code)
        body = response.json()
        self.assertEqual(main_module.ErrorCode.VALIDATION_ERROR, body["code"])
        self.assertIsInstance(body["detail"], list)
        self.assertTrue(
            any(
                "client monotonic time is not accepted on the wire" in item["msg"]
                for item in body["detail"]
            )
        )

    def test_live_routes_still_resolve_after_active_endpoint_addition(self):
        session = self.start_session()

        position_response = self.client.post(
            "/api/v1/position",
            json=self.position_payload(session["session_id"]),
            headers=self.write_headers(session)
        )
        self.assertEqual(200, position_response.status_code)

        active_list = self.client.get("/api/v1/live/active")
        by_session = self.client.get(f"/api/v1/live/{session['session_id']}")
        by_share = self.client.get(f"/api/v1/live/share/{session['share_code']}")

        self.assertEqual(200, active_list.status_code)
        self.assertIsInstance(active_list.json(), list)
        self.assertEqual(200, by_session.status_code)
        self.assertEqual(session["session_id"], by_session.json()["session"])
        self.assertEqual(200, by_share.status_code)
        self.assertEqual(session["share_code"], by_share.json()["share_code"])

    def start_session(self):
        response = self.client.post("/api/v1/session/start")
        self.assertEqual(200, response.status_code)
        return response.json()

    def write_headers(self, session):
        return {"X-Session-Token": session["write_token"]}

    def position_payload(
        self,
        session_id: str,
        timestamp: datetime | None = None,
        lat: float = -33.9,
        lon: float = 151.2,
        alt: float = 500.0,
        speed: float = 12.5,
        heading: float = 180.0,
        **extra
    ):
        timestamp = timestamp or datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
        payload = {
            "session_id": session_id,
            "lat": lat,
            "lon": lon,
            "alt": alt,
            "speed": speed,
            "heading": heading,
            "timestamp": timestamp.isoformat()
        }
        payload.update(extra)
        return payload


if __name__ == "__main__":
    unittest.main()
