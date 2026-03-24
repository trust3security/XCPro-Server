import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, inspect
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
        self.original_static_bearer_tokens = main_module.STATIC_BEARER_TOKENS
        self.original_google_server_client_ids = main_module.GOOGLE_SERVER_CLIENT_IDS
        self.original_google_id_token_verifier = main_module.GOOGLE_ID_TOKEN_VERIFIER
        self.original_private_follow_bearer_secret = main_module.PRIVATE_FOLLOW_BEARER_SECRET

        self.primary_bearer_token = "test-bearer-token-1"
        self.secondary_bearer_token = "test-bearer-token-2"
        self.tertiary_bearer_token = "test-bearer-token-3"
        self.google_id_token = "google-id-token-1"

        main_module.SessionLocal = self.session_local
        main_module.redis_client = FakeRedis()
        self.clock = MutableClock(datetime(2026, 3, 20, 12, 0, 0))
        main_module.utcnow = self.clock.utcnow
        main_module.STATIC_BEARER_TOKENS = {
            self.primary_bearer_token: main_module.ResolvedBearerIdentity(
                provider="static",
                provider_subject="pilot-1",
                email="pilot1@example.com",
                display_name="Pilot One"
            ),
            self.secondary_bearer_token: main_module.ResolvedBearerIdentity(
                provider="static",
                provider_subject="pilot-2",
                email="pilot2@example.com",
                display_name="Pilot Two"
            ),
            self.tertiary_bearer_token: main_module.ResolvedBearerIdentity(
                provider="static",
                provider_subject="pilot-3",
                email="pilot3@example.com",
                display_name="Pilot Three"
            ),
        }
        main_module.GOOGLE_SERVER_CLIENT_IDS = frozenset({"test-google-client-id"})
        main_module.GOOGLE_ID_TOKEN_VERIFIER = self.fake_google_id_token_verifier
        main_module.PRIVATE_FOLLOW_BEARER_SECRET = b"test-private-follow-secret"

        self.client = TestClient(main_module.app)

    def tearDown(self):
        self.client.close()
        main_module.SessionLocal = self.original_session_local
        main_module.redis_client = self.original_redis_client
        main_module.utcnow = self.original_utcnow
        main_module.STATIC_BEARER_TOKENS = self.original_static_bearer_tokens
        main_module.GOOGLE_SERVER_CLIENT_IDS = self.original_google_server_client_ids
        main_module.GOOGLE_ID_TOKEN_VERIFIER = self.original_google_id_token_verifier
        main_module.PRIVATE_FOLLOW_BEARER_SECRET = self.original_private_follow_bearer_secret
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
            ),
            (
                {
                    "session_id": session["session_id"],
                    "clear_task": True,
                    "task_name": "Task",
                    "task": {
                        "turnpoints": [
                            {"name": "Start", "type": "start", "lat": -33.9, "lon": 151.2},
                            {"name": "Finish", "type": "finish", "lat": -33.8, "lon": 151.3}
                        ]
                    }
                },
                400,
                main_module.ErrorCode.TASK_CLEAR_PAYLOAD_INVALID,
                "clear_task cannot be combined with task_name or task"
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

    def test_task_clear_returns_null_in_live_reads_and_readd_restores_task(self):
        session = self.start_session()
        headers = self.write_headers(session)

        upsert = self.client.post(
            "/api/v1/task/upsert",
            json=self.task_payload(session["session_id"], task_name="Task Alpha"),
            headers=headers
        )
        self.assertEqual(200, upsert.status_code)
        self.assertFalse(upsert.json()["cleared"])
        self.assertEqual(1, upsert.json()["revision"])

        initial_live = self.client.get(f"/api/v1/live/{session['session_id']}")
        self.assertEqual(200, initial_live.status_code)
        self.assertIsNotNone(initial_live.json()["task"])
        self.assertEqual(
            "Task Alpha",
            initial_live.json()["task"]["payload"]["task_name"]
        )

        cleared = self.client.post(
            "/api/v1/task/upsert",
            json={
                "session_id": session["session_id"],
                "clear_task": True
            },
            headers=headers
        )
        self.assertEqual(200, cleared.status_code)
        self.assertTrue(cleared.json()["cleared"])
        self.assertEqual(2, cleared.json()["revision"])

        by_session = self.client.get(f"/api/v1/live/{session['session_id']}")
        by_share = self.client.get(f"/api/v1/live/share/{session['share_code']}")
        self.assertEqual(200, by_session.status_code)
        self.assertEqual(200, by_share.status_code)
        self.assertIsNone(by_session.json()["task"])
        self.assertIsNone(by_share.json()["task"])

        repeated_clear = self.client.post(
            "/api/v1/task/upsert",
            json={
                "session_id": session["session_id"],
                "clear_task": True
            },
            headers=headers
        )
        self.assertEqual(200, repeated_clear.status_code)
        self.assertTrue(repeated_clear.json()["cleared"])
        self.assertTrue(repeated_clear.json()["deduped"])
        self.assertEqual(2, repeated_clear.json()["revision"])

        readd = self.client.post(
            "/api/v1/task/upsert",
            json=self.task_payload(session["session_id"], task_name="Task Bravo"),
            headers=headers
        )
        self.assertEqual(200, readd.status_code)
        self.assertFalse(readd.json()["cleared"])
        self.assertEqual(3, readd.json()["revision"])

        restored_live = self.client.get(f"/api/v1/live/{session['session_id']}")
        self.assertEqual(200, restored_live.status_code)
        self.assertIsNotNone(restored_live.json()["task"])
        self.assertEqual(
            "Task Bravo",
            restored_live.json()["task"]["payload"]["task_name"]
        )

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
        self.assertIsNone(item["latest"]["agl_meters"])
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
        self.assertIsNone(live.json()["latest"]["agl_meters"])
        self.assertEqual(12.5, live.json()["positions"][0]["speed"])
        self.assertIsNone(live.json()["positions"][0]["agl_meters"])

    def test_position_wire_contract_relays_optional_agl_meters_in_live_watch_payloads(self):
        session = self.start_session()
        headers = self.write_headers(session)

        response = self.client.post(
            "/api/v1/position",
            json=self.position_payload(
                session["session_id"],
                agl_meters=123.4
            ),
            headers=headers
        )
        self.assertEqual(200, response.status_code)

        by_session = self.client.get(f"/api/v1/live/{session['session_id']}")
        by_share = self.client.get(f"/api/v1/live/share/{session['share_code']}")
        active = self.client.get("/api/v1/live/active")

        self.assertEqual(200, by_session.status_code)
        self.assertEqual(123.4, by_session.json()["latest"]["agl_meters"])
        self.assertEqual(123.4, by_session.json()["positions"][0]["agl_meters"])

        self.assertEqual(200, by_share.status_code)
        self.assertEqual(123.4, by_share.json()["latest"]["agl_meters"])
        self.assertEqual(123.4, by_share.json()["positions"][0]["agl_meters"])

        self.assertEqual(200, active.status_code)
        self.assertEqual(1, len(active.json()))
        self.assertEqual(123.4, active.json()[0]["latest"]["agl_meters"])

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

    def test_get_me_requires_bearer_auth(self):
        missing = self.client.get("/api/v2/me")
        invalid = self.client.get(
            "/api/v2/me",
            headers={"Authorization": "Bearer wrong-token"}
        )

        self.assertEqual(401, missing.status_code)
        self.assertEqual(
            {
                "code": main_module.ErrorCode.UNAUTHENTICATED,
                "detail": "missing Authorization header"
            },
            missing.json()
        )
        self.assertEqual(401, invalid.status_code)
        self.assertEqual(
            {
                "code": main_module.ErrorCode.UNAUTHENTICATED,
                "detail": "invalid bearer token"
            },
            invalid.json()
        )

    def test_get_me_bootstraps_user_profile_and_default_privacy(self):
        response = self.client.get("/api/v2/me", headers=self.bearer_headers())

        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertIsNotNone(body["user_id"])
        self.assertIsNone(body["handle"])
        self.assertEqual("Pilot One", body["display_name"])
        self.assertIsNone(body["comp_number"])
        self.assertEqual("searchable", body["privacy"]["discoverability"])
        self.assertEqual("approval_required", body["privacy"]["follow_policy"])
        self.assertEqual("followers", body["privacy"]["default_live_visibility"])
        self.assertEqual("owner_only", body["privacy"]["connection_list_visibility"])

    def test_google_auth_exchange_issues_server_bearer_that_works_on_me(self):
        exchange_response = self.client.post(
            "/api/v2/auth/google/exchange",
            json={"google_id_token": self.google_id_token}
        )

        self.assertEqual(200, exchange_response.status_code)
        exchange_body = exchange_response.json()
        self.assertEqual("Bearer", exchange_body["token_type"])
        self.assertEqual("google", exchange_body["auth_method"])
        self.assertIsNotNone(exchange_body["access_token"])
        self.assertIsNotNone(exchange_body["user_id"])

        me_response = self.client.get(
            "/api/v2/me",
            headers=self.bearer_headers(exchange_body["access_token"])
        )
        self.assertEqual(200, me_response.status_code)
        me_body = me_response.json()
        self.assertEqual(exchange_body["user_id"], me_body["user_id"])
        self.assertEqual("Google Pilot", me_body["display_name"])
        self.assertEqual("searchable", me_body["privacy"]["discoverability"])

    def test_google_auth_exchange_rejects_invalid_google_token(self):
        response = self.client.post(
            "/api/v2/auth/google/exchange",
            json={"google_id_token": "wrong-google-token"}
        )

        self.assertEqual(401, response.status_code)
        self.assertEqual(main_module.ErrorCode.INVALID_GOOGLE_ID_TOKEN, response.json()["code"])

    def test_patch_me_profile_persists_and_enforces_case_insensitive_handle_uniqueness(self):
        first_response = self.client.patch(
            "/api/v2/me/profile",
            json={
                "handle": "Pilot.One",
                "display_name": "Pilot One Updated",
                "comp_number": "P1"
            },
            headers=self.bearer_headers()
        )
        duplicate_response = self.client.patch(
            "/api/v2/me/profile",
            json={
                "handle": "pilot.one",
                "display_name": "Pilot Two Updated"
            },
            headers=self.bearer_headers(self.secondary_bearer_token)
        )
        me_response = self.client.get("/api/v2/me", headers=self.bearer_headers())

        self.assertEqual(200, first_response.status_code)
        self.assertEqual("pilot.one", first_response.json()["handle"])
        self.assertEqual("Pilot One Updated", first_response.json()["display_name"])
        self.assertEqual("P1", first_response.json()["comp_number"])

        self.assertEqual(409, duplicate_response.status_code)
        self.assertEqual(
            {
                "code": main_module.ErrorCode.HANDLE_ALREADY_TAKEN,
                "detail": "handle already taken"
            },
            duplicate_response.json()
        )

        self.assertEqual(200, me_response.status_code)
        self.assertEqual("pilot.one", me_response.json()["handle"])
        self.assertEqual("Pilot One Updated", me_response.json()["display_name"])
        self.assertEqual("P1", me_response.json()["comp_number"])

    def test_patch_me_profile_rejects_invalid_handle(self):
        response = self.client.patch(
            "/api/v2/me/profile",
            json={
                "handle": "Bad Handle",
                "display_name": "Pilot One"
            },
            headers=self.bearer_headers()
        )

        self.assertEqual(422, response.status_code)
        self.assertEqual(main_module.ErrorCode.INVALID_HANDLE, response.json()["code"])

    def test_patch_me_privacy_persists_and_validates(self):
        success = self.client.patch(
            "/api/v2/me/privacy",
            json={
                "discoverability": "hidden",
                "follow_policy": "auto_approve",
                "default_live_visibility": "public",
                "connection_list_visibility": "mutuals_only"
            },
            headers=self.bearer_headers()
        )
        invalid = self.client.patch(
            "/api/v2/me/privacy",
            json={"follow_policy": "everyone"},
            headers=self.bearer_headers()
        )
        me_response = self.client.get("/api/v2/me", headers=self.bearer_headers())

        self.assertEqual(200, success.status_code)
        self.assertEqual("hidden", success.json()["discoverability"])
        self.assertEqual("auto_approve", success.json()["follow_policy"])
        self.assertEqual("public", success.json()["default_live_visibility"])
        self.assertEqual("mutuals_only", success.json()["connection_list_visibility"])

        self.assertEqual(422, invalid.status_code)
        self.assertEqual(main_module.ErrorCode.INVALID_PRIVACY_SETTING, invalid.json()["code"])

        self.assertEqual(200, me_response.status_code)
        self.assertEqual("hidden", me_response.json()["privacy"]["discoverability"])
        self.assertEqual("auto_approve", me_response.json()["privacy"]["follow_policy"])
        self.assertEqual("public", me_response.json()["privacy"]["default_live_visibility"])
        self.assertEqual("mutuals_only", me_response.json()["privacy"]["connection_list_visibility"])

    def test_search_users_matches_handle_case_insensitively_and_hides_hidden_profiles(self):
        pilot_one = self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        pilot_two = self.complete_profile(
            token=self.secondary_bearer_token,
            handle="pilot.target",
            display_name="Pilot Two"
        )
        self.complete_profile(
            token=self.tertiary_bearer_token,
            handle="pilot.hidden",
            display_name="Hidden Pilot"
        )
        self.patch_privacy(
            token=self.tertiary_bearer_token,
            discoverability="hidden"
        )

        response = self.client.get(
            "/api/v2/users/search",
            params={"q": "PILOT"},
            headers=self.bearer_headers()
        )

        self.assertEqual(200, response.status_code)
        users = response.json()["users"]
        self.assertEqual([pilot_two["user_id"]], [user["user_id"] for user in users])
        self.assertEqual("pilot.target", users[0]["handle"])
        self.assertEqual("Pilot Two", users[0]["display_name"])
        self.assertEqual("none", users[0]["relationship_state"])
        self.assertNotIn(pilot_one["user_id"], [user["user_id"] for user in users])

    def test_search_users_rejects_short_query(self):
        response = self.client.get(
            "/api/v2/users/search",
            params={"q": "p"},
            headers=self.bearer_headers()
        )

        self.assertEqual(422, response.status_code)
        self.assertEqual(main_module.ErrorCode.SEARCH_QUERY_TOO_SHORT, response.json()["code"])

    def test_follow_request_create_list_and_accept_persists_relationship(self):
        self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        target_profile = self.complete_profile(
            token=self.secondary_bearer_token,
            handle="pilot.two",
            display_name="Pilot Two"
        )

        create_response = self.client.post(
            "/api/v2/follow-requests",
            json={"target_user_id": target_profile["user_id"]},
            headers=self.bearer_headers()
        )
        self.assertEqual(200, create_response.status_code)
        created = create_response.json()
        self.assertEqual("pending", created["status"])
        self.assertEqual("outgoing", created["direction"])
        self.assertEqual("pilot.two", created["counterpart"]["handle"])
        self.assertEqual("outgoing_pending", created["relationship_state"])

        outgoing = self.client.get(
            "/api/v2/follow-requests/outgoing",
            headers=self.bearer_headers()
        )
        incoming = self.client.get(
            "/api/v2/follow-requests/incoming",
            headers=self.bearer_headers(self.secondary_bearer_token)
        )
        self.assertEqual(200, outgoing.status_code)
        self.assertEqual(200, incoming.status_code)
        self.assertEqual(1, len(outgoing.json()["requests"]))
        self.assertEqual(1, len(incoming.json()["requests"]))

        accept_response = self.client.post(
            f"/api/v2/follow-requests/{created['request_id']}/accept",
            headers=self.bearer_headers(self.secondary_bearer_token)
        )
        self.assertEqual(200, accept_response.status_code)
        self.assertEqual("accepted", accept_response.json()["status"])
        self.assertEqual("incoming", accept_response.json()["direction"])
        self.assertEqual("followed_by", accept_response.json()["relationship_state"])

        outgoing_after = self.client.get(
            "/api/v2/follow-requests/outgoing",
            headers=self.bearer_headers()
        )
        incoming_after = self.client.get(
            "/api/v2/follow-requests/incoming",
            headers=self.bearer_headers(self.secondary_bearer_token)
        )
        search_from_requester = self.client.get(
            "/api/v2/users/search",
            params={"q": "pilot.two"},
            headers=self.bearer_headers()
        )
        search_from_target = self.client.get(
            "/api/v2/users/search",
            params={"q": "pilot.one"},
            headers=self.bearer_headers(self.secondary_bearer_token)
        )
        self.assertEqual([], outgoing_after.json()["requests"])
        self.assertEqual([], incoming_after.json()["requests"])
        self.assertEqual("following", search_from_requester.json()["users"][0]["relationship_state"])
        self.assertEqual("followed_by", search_from_target.json()["users"][0]["relationship_state"])

    def test_follow_request_decline_clears_pending_and_allows_re_request(self):
        self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        target_profile = self.complete_profile(
            token=self.secondary_bearer_token,
            handle="pilot.two",
            display_name="Pilot Two"
        )

        create_response = self.client.post(
            "/api/v2/follow-requests",
            json={"target_user_id": target_profile["user_id"]},
            headers=self.bearer_headers()
        )
        request_id = create_response.json()["request_id"]

        decline_response = self.client.post(
            f"/api/v2/follow-requests/{request_id}/decline",
            headers=self.bearer_headers(self.secondary_bearer_token)
        )
        self.assertEqual(200, decline_response.status_code)
        self.assertEqual("declined", decline_response.json()["status"])

        outgoing_after = self.client.get(
            "/api/v2/follow-requests/outgoing",
            headers=self.bearer_headers()
        )
        search_after = self.client.get(
            "/api/v2/users/search",
            params={"q": "pilot.two"},
            headers=self.bearer_headers()
        )
        retry_response = self.client.post(
            "/api/v2/follow-requests",
            json={"target_user_id": target_profile["user_id"]},
            headers=self.bearer_headers()
        )
        self.assertEqual([], outgoing_after.json()["requests"])
        self.assertEqual("none", search_after.json()["users"][0]["relationship_state"])
        self.assertEqual(200, retry_response.status_code)
        self.assertEqual("pending", retry_response.json()["status"])

    def test_follow_request_rejects_self_duplicate_and_closed_policy(self):
        requester_profile = self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        target_profile = self.complete_profile(
            token=self.secondary_bearer_token,
            handle="pilot.two",
            display_name="Pilot Two"
        )

        self_request = self.client.post(
            "/api/v2/follow-requests",
            json={"target_user_id": requester_profile["user_id"]},
            headers=self.bearer_headers()
        )
        self.assertEqual(422, self_request.status_code)
        self.assertEqual(main_module.ErrorCode.FOLLOW_REQUEST_SELF, self_request.json()["code"])

        first_request = self.client.post(
            "/api/v2/follow-requests",
            json={"target_user_id": target_profile["user_id"]},
            headers=self.bearer_headers()
        )
        duplicate_request = self.client.post(
            "/api/v2/follow-requests",
            json={"target_user_id": target_profile["user_id"]},
            headers=self.bearer_headers()
        )
        self.assertEqual(200, first_request.status_code)
        self.assertEqual(409, duplicate_request.status_code)
        self.assertEqual(
            main_module.ErrorCode.FOLLOW_REQUEST_ALREADY_EXISTS,
            duplicate_request.json()["code"]
        )

        other_target = self.complete_profile(
            token=self.tertiary_bearer_token,
            handle="pilot.closed",
            display_name="Closed Pilot"
        )
        self.patch_privacy(
            token=self.tertiary_bearer_token,
            follow_policy="closed"
        )
        closed_response = self.client.post(
            "/api/v2/follow-requests",
            json={"target_user_id": other_target["user_id"]},
            headers=self.bearer_headers()
        )
        self.assertEqual(409, closed_response.status_code)
        self.assertEqual(main_module.ErrorCode.FOLLOW_REQUEST_CLOSED, closed_response.json()["code"])

    def test_follow_request_honors_auto_approve_policy(self):
        self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        target_profile = self.complete_profile(
            token=self.secondary_bearer_token,
            handle="pilot.auto",
            display_name="Pilot Auto"
        )
        self.patch_privacy(
            token=self.secondary_bearer_token,
            follow_policy="auto_approve"
        )

        create_response = self.client.post(
            "/api/v2/follow-requests",
            json={"target_user_id": target_profile["user_id"]},
            headers=self.bearer_headers()
        )
        outgoing = self.client.get(
            "/api/v2/follow-requests/outgoing",
            headers=self.bearer_headers()
        )
        search = self.client.get(
            "/api/v2/users/search",
            params={"q": "pilot.auto"},
            headers=self.bearer_headers()
        )

        self.assertEqual(200, create_response.status_code)
        self.assertEqual("accepted", create_response.json()["status"])
        self.assertEqual("following", create_response.json()["relationship_state"])
        self.assertEqual([], outgoing.json()["requests"])
        self.assertEqual("following", search.json()["users"][0]["relationship_state"])

    def test_authenticated_live_start_uses_owner_and_default_visibility(self):
        profile = self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        self.patch_privacy(
            token=self.primary_bearer_token,
            default_live_visibility="followers"
        )

        session = self.start_authenticated_session()
        stored = self.get_live_session_row(session["session_id"])

        self.assertEqual("followers", session["visibility"])
        self.assertEqual(profile["user_id"], session["owner_user_id"])
        self.assertIsNone(session["share_code"])
        self.assertEqual(profile["user_id"], stored.owner_user_id)
        self.assertEqual("followers", stored.visibility)
        self.assertIsNotNone(stored.share_code)

    def test_authenticated_live_start_ends_previous_owned_sessions(self):
        self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )

        first = self.start_authenticated_session()
        second = self.start_authenticated_session(visibility="public")

        rejected_write = self.client.post(
            "/api/v1/position",
            json=self.position_payload(first["session_id"]),
            headers={"X-Session-Token": first["write_token"]}
        )
        first_row = self.get_live_session_row(first["session_id"])
        second_row = self.get_live_session_row(second["session_id"])

        self.assertEqual(409, rejected_write.status_code)
        self.assertEqual("ended", first_row.status)
        self.assertEqual("active", second_row.status)

    def test_public_live_routes_hide_follower_only_session(self):
        self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )

        session = self.start_authenticated_session(visibility="followers")
        stored = self.get_live_session_row(session["session_id"])
        position_response = self.client.post(
            "/api/v1/position",
            json=self.position_payload(session["session_id"]),
            headers={"X-Session-Token": session["write_token"]}
        )
        active_response = self.client.get("/api/v1/live/active")
        by_session = self.client.get(f"/api/v1/live/{session['session_id']}")
        by_share = self.client.get(f"/api/v1/live/share/{stored.share_code}")

        self.assertEqual(200, position_response.status_code)
        self.assertEqual([], active_response.json())
        self.assertEqual(404, by_session.status_code)
        self.assertEqual(404, by_share.status_code)

    def test_authenticated_following_active_lists_followed_live_sessions(self):
        self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        owner_profile = self.complete_profile(
            token=self.secondary_bearer_token,
            handle="pilot.two",
            display_name="Pilot Two"
        )
        self.patch_privacy(
            token=self.secondary_bearer_token,
            follow_policy="auto_approve"
        )
        create_response = self.client.post(
            "/api/v2/follow-requests",
            json={"target_user_id": owner_profile["user_id"]},
            headers=self.bearer_headers()
        )
        self.assertEqual(200, create_response.status_code)

        session = self.start_authenticated_session(
            token=self.secondary_bearer_token,
            visibility="followers"
        )
        self.client.post(
            "/api/v1/position",
            json=self.position_payload(session["session_id"]),
            headers={"X-Session-Token": session["write_token"]}
        )

        response = self.client.get(
            "/api/v2/live/following/active",
            headers=self.bearer_headers()
        )

        self.assertEqual(200, response.status_code)
        items = response.json()["items"]
        self.assertEqual(1, len(items))
        self.assertEqual(session["session_id"], items[0]["session_id"])
        self.assertEqual(owner_profile["user_id"], items[0]["user_id"])
        self.assertEqual("followers", items[0]["visibility"])
        self.assertIsNone(items[0]["share_code"])
        self.assertEqual("Pilot Two", items[0]["display_label"])

    def test_authenticated_live_reads_and_user_lookup_enforce_follow_entitlement(self):
        self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        owner_profile = self.complete_profile(
            token=self.secondary_bearer_token,
            handle="pilot.two",
            display_name="Pilot Two"
        )
        self.patch_privacy(
            token=self.secondary_bearer_token,
            follow_policy="auto_approve"
        )
        self.client.post(
            "/api/v2/follow-requests",
            json={"target_user_id": owner_profile["user_id"]},
            headers=self.bearer_headers()
        )
        session = self.start_authenticated_session(
            token=self.secondary_bearer_token,
            visibility="followers"
        )
        self.client.post(
            "/api/v1/position",
            json=self.position_payload(session["session_id"]),
            headers={"X-Session-Token": session["write_token"]}
        )

        owner_read = self.client.get(
            f"/api/v2/live/session/{session['session_id']}",
            headers=self.bearer_headers(self.secondary_bearer_token)
        )
        follower_read = self.client.get(
            f"/api/v2/live/session/{session['session_id']}",
            headers=self.bearer_headers()
        )
        follower_lookup = self.client.get(
            f"/api/v2/live/users/{owner_profile['user_id']}",
            headers=self.bearer_headers()
        )
        outsider_read = self.client.get(
            f"/api/v2/live/session/{session['session_id']}",
            headers=self.bearer_headers(self.tertiary_bearer_token)
        )
        outsider_lookup = self.client.get(
            f"/api/v2/live/users/{owner_profile['user_id']}",
            headers=self.bearer_headers(self.tertiary_bearer_token)
        )

        self.assertEqual(200, owner_read.status_code)
        self.assertEqual(200, follower_read.status_code)
        self.assertEqual(200, follower_lookup.status_code)
        self.assertEqual("followers", follower_read.json()["visibility"])
        self.assertIsNone(follower_read.json()["share_code"])
        self.assertEqual(404, outsider_read.status_code)
        self.assertEqual(404, outsider_lookup.status_code)

    def test_visibility_patch_removes_public_v1_visibility_until_public_restored(self):
        self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )

        session = self.start_authenticated_session(visibility="public")
        stored = self.get_live_session_row(session["session_id"])
        self.client.post(
            "/api/v1/position",
            json=self.position_payload(session["session_id"]),
            headers={"X-Session-Token": session["write_token"]}
        )

        before_patch = self.client.get("/api/v1/live/active")
        patch_response = self.client.patch(
            f"/api/v2/live/session/{session['session_id']}/visibility",
            json={"visibility": "followers"},
            headers=self.bearer_headers()
        )
        hidden_active = self.client.get("/api/v1/live/active")
        hidden_by_session = self.client.get(f"/api/v1/live/{session['session_id']}")
        hidden_by_share = self.client.get(f"/api/v1/live/share/{stored.share_code}")
        restore_response = self.client.patch(
            f"/api/v2/live/session/{session['session_id']}/visibility",
            json={"visibility": "public"},
            headers=self.bearer_headers()
        )
        restored_by_share = self.client.get(f"/api/v1/live/share/{stored.share_code}")

        self.assertEqual(1, len(before_patch.json()))
        self.assertEqual(200, patch_response.status_code)
        self.assertEqual("followers", patch_response.json()["visibility"])
        self.assertIsNone(patch_response.json()["share_code"])
        self.assertEqual([], hidden_active.json())
        self.assertEqual(404, hidden_by_session.status_code)
        self.assertEqual(404, hidden_by_share.status_code)
        self.assertEqual(200, restore_response.status_code)
        self.assertEqual("public", restore_response.json()["visibility"])
        self.assertEqual(stored.share_code, restore_response.json()["share_code"])
        self.assertEqual(200, restored_by_share.status_code)

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

    def start_authenticated_session(
        self,
        token: str | None = None,
        visibility: str | None = None
    ):
        if visibility is None:
            response = self.client.post(
                "/api/v2/live/session/start",
                headers=self.bearer_headers(token)
            )
        else:
            response = self.client.post(
                "/api/v2/live/session/start",
                json={"visibility": visibility},
                headers=self.bearer_headers(token)
            )
        self.assertEqual(200, response.status_code)
        return response.json()

    def get_live_session_row(self, session_id: str):
        db = self.session_local()
        try:
            return (
                db.query(main_module.LiveSession)
                .filter(main_module.LiveSession.id == session_id)
                .first()
            )
        finally:
            db.close()

    def write_headers(self, session):
        return {"X-Session-Token": session["write_token"]}

    def bearer_headers(self, token: str | None = None):
        return {"Authorization": f"Bearer {token or self.primary_bearer_token}"}

    def complete_profile(
        self,
        token: str,
        handle: str,
        display_name: str,
        comp_number: str | None = None
    ):
        self.client.get("/api/v2/me", headers=self.bearer_headers(token))
        payload = {
            "handle": handle,
            "display_name": display_name,
            "comp_number": comp_number
        }
        response = self.client.patch(
            "/api/v2/me/profile",
            json=payload,
            headers=self.bearer_headers(token)
        )
        self.assertEqual(200, response.status_code)
        return response.json()

    def patch_privacy(
        self,
        token: str,
        discoverability: str = "searchable",
        follow_policy: str = "approval_required",
        default_live_visibility: str = "followers",
        connection_list_visibility: str = "owner_only"
    ):
        response = self.client.patch(
            "/api/v2/me/privacy",
            json={
                "discoverability": discoverability,
                "follow_policy": follow_policy,
                "default_live_visibility": default_live_visibility,
                "connection_list_visibility": connection_list_visibility
            },
            headers=self.bearer_headers(token)
        )
        self.assertEqual(200, response.status_code)
        return response.json()

    def fake_google_id_token_verifier(self, token: str):
        if token != self.google_id_token:
            return None
        return main_module.ResolvedBearerIdentity(
            provider="google",
            provider_subject="google-user-1",
            email="google@example.com",
            display_name="Google Pilot"
        )

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

    def task_payload(self, session_id: str, task_name: str = "Task"):
        return {
            "session_id": session_id,
            "task_name": task_name,
            "task": {
                "turnpoints": [
                    {
                        "name": "Start",
                        "type": "START_LINE",
                        "lat": -33.9,
                        "lon": 151.2,
                        "radius_m": 10000.0
                    },
                    {
                        "name": "TP1",
                        "type": "TURN_POINT_CYLINDER",
                        "lat": -33.8,
                        "lon": 151.3,
                        "radius_m": 500.0
                    },
                    {
                        "name": "Finish",
                        "type": "FINISH_CYLINDER",
                        "lat": -33.7,
                        "lon": 151.4,
                        "radius_m": 3000.0
                    }
                ],
                "start": {
                    "type": "START_LINE",
                    "radius_m": 10000.0
                },
                "finish": {
                    "type": "FINISH_CYLINDER",
                    "radius_m": 3000.0
                }
            }
        }


class PrivateFollowReleaseHardeningTest(unittest.TestCase):
    def test_runtime_safety_rejects_static_dev_bearer_outside_dev(self):
        config = main_module.build_private_follow_runtime_config(
            {
                "XCPRO_RUNTIME_ENV": "staging",
                "XCPRO_ALLOW_DEV_STATIC_BEARER_AUTH": "true",
                "XCPRO_STATIC_BEARER_TOKENS_JSON": json.dumps({"dev-token": "pilot-1"}),
            }
        )

        self.assertEqual(
            [
                "XCPRO_ALLOW_DEV_STATIC_BEARER_AUTH is only permitted when XCPRO_RUNTIME_ENV=dev",
                "XCPRO_STATIC_BEARER_TOKENS_JSON must not be set unless XCPRO_RUNTIME_ENV=dev",
            ],
            main_module.collect_private_follow_runtime_safety_errors(config),
        )

    def test_build_runtime_config_only_activates_static_tokens_with_explicit_dev_flag(self):
        env = {
            "XCPRO_RUNTIME_ENV": "dev",
            "XCPRO_STATIC_BEARER_TOKENS_JSON": json.dumps({"dev-token": "pilot-1"}),
        }

        disabled = main_module.build_private_follow_runtime_config(env)
        enabled = main_module.build_private_follow_runtime_config(
            {
                **env,
                "XCPRO_ALLOW_DEV_STATIC_BEARER_AUTH": "1",
            }
        )

        self.assertEqual({}, disabled.static_bearer_tokens)
        self.assertEqual(1, len(enabled.static_bearer_tokens))
        self.assertIn("dev-token", enabled.static_bearer_tokens)

    def test_preflight_requires_google_client_id_and_bearer_secret_in_prod(self):
        report = main_module.build_private_follow_preflight_report(
            main_module.build_private_follow_runtime_config(
                {
                    "XCPRO_RUNTIME_ENV": "prod",
                }
            )
        )

        self.assertFalse(report["ok"])
        self.assertEqual(
            [
                "Missing XCPRO_GOOGLE_SERVER_CLIENT_ID or XCPRO_GOOGLE_SERVER_CLIENT_IDS",
                "Missing XCPRO_PRIVATE_FOLLOW_BEARER_SECRET",
            ],
            report["errors"],
        )

    def test_fresh_db_alembic_upgrade_reaches_head(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            sqlite_path = temp_path / "private_follow_bootstrap.db"
            alembic_config = Config(
                str(Path(main_module.__file__).resolve().with_name("alembic.ini"))
            )
            alembic_config.set_main_option(
                "sqlalchemy.url",
                f"sqlite:///{sqlite_path.as_posix()}",
            )

            command.upgrade(alembic_config, "head")

            verification_engine = create_engine(f"sqlite:///{sqlite_path.as_posix()}")
            try:
                db_inspector = inspect(verification_engine)
                table_names = set(db_inspector.get_table_names())
                self.assertTrue(
                    {
                        "live_sessions",
                        "live_positions",
                        "live_tasks",
                        "live_task_revisions",
                        "users",
                        "auth_identities",
                        "pilot_profiles",
                        "privacy_settings",
                        "follow_requests",
                        "follow_edges",
                    }.issubset(table_names)
                )
                self.assertIn(
                    "agl_meters",
                    {column["name"] for column in db_inspector.get_columns("live_positions")},
                )
                live_session_columns = {
                    column["name"] for column in db_inspector.get_columns("live_sessions")
                }
                self.assertIn("owner_user_id", live_session_columns)
                self.assertIn("visibility", live_session_columns)
            finally:
                verification_engine.dispose()


if __name__ == "__main__":
    unittest.main()
