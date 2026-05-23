import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from dataclasses import replace
from pathlib import Path

from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app import main as main_module
from app.scripts import deliver_notifications


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


class FakeFcmSender:
    def __init__(self, failures_by_token: dict[str, Exception] | None = None):
        self.failures_by_token = failures_by_token or {}
        self.messages = []

    def send_message(self, token: str, data: dict[str, str]) -> None:
        self.messages.append({"token": token, "data": data})
        failure = self.failures_by_token.get(token)
        if failure is not None:
            raise failure


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
        self.original_private_follow_runtime_config = main_module.PRIVATE_FOLLOW_RUNTIME_CONFIG
        self.original_static_bearer_tokens = main_module.STATIC_BEARER_TOKENS
        self.original_google_server_client_ids = main_module.GOOGLE_SERVER_CLIENT_IDS
        self.original_google_id_token_verifier = main_module.GOOGLE_ID_TOKEN_VERIFIER
        self.original_private_follow_bearer_secret = main_module.PRIVATE_FOLLOW_BEARER_SECRET
        self.original_push_token_encryption_secret = main_module.PUSH_TOKEN_ENCRYPTION_SECRET

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
        main_module.PUSH_TOKEN_ENCRYPTION_SECRET = b"test-push-token-encryption-secret"

        self.client = TestClient(main_module.app)

    def tearDown(self):
        self.client.close()
        main_module.SessionLocal = self.original_session_local
        main_module.redis_client = self.original_redis_client
        main_module.utcnow = self.original_utcnow
        main_module.PRIVATE_FOLLOW_RUNTIME_CONFIG = self.original_private_follow_runtime_config
        main_module.STATIC_BEARER_TOKENS = self.original_static_bearer_tokens
        main_module.GOOGLE_SERVER_CLIENT_IDS = self.original_google_server_client_ids
        main_module.GOOGLE_ID_TOKEN_VERIFIER = self.original_google_id_token_verifier
        main_module.PRIVATE_FOLLOW_BEARER_SECRET = self.original_private_follow_bearer_secret
        main_module.PUSH_TOKEN_ENCRYPTION_SECRET = self.original_push_token_encryption_secret
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
        self.assertEqual(
            {
                "following_count": 0,
                "max_following": 1,
                "status": "under_limit",
            },
            body["relationship_limits"]
        )

    def test_get_me_relationship_limits_return_paid_verified_caps(self):
        cases = [
            ("BASIC", "xcpro_basic", 4),
            ("SOARING", "xcpro_soaring", 15),
            ("XC", "xcpro_xc", 50),
            ("PRO", "xcpro_pro", 100),
        ]
        for tier, product_id, expected_cap in cases:
            with self.subTest(tier=tier):
                token = self.add_static_bearer_token(
                    f"paid-token-{tier.lower()}",
                    f"paid-{tier.lower()}",
                    f"Paid {tier}"
                )
                self.upsert_entitlement_snapshot(
                    token=token,
                    tier=tier,
                    billing_period="MONTHLY",
                    status="ACTIVE",
                    verification_state="VERIFIED",
                    product_id=product_id,
                    base_plan_id="monthly",
                )

                response = self.client.get(
                    "/api/v2/me",
                    headers=self.bearer_headers(token)
                )

                self.assertEqual(200, response.status_code)
                self.assertEqual(
                    {
                        "following_count": 0,
                        "max_following": expected_cap,
                        "status": "under_limit",
                    },
                    response.json()["relationship_limits"]
                )
                self.assert_response_has_no_purchase_token(response.json())

    def test_get_me_relationship_limits_report_at_and_over_limit_for_existing_edges(self):
        self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        user_id = self.user_id_for_token()
        self.seed_following_edges(user_id, 1, "me-at")

        at_limit = self.client.get("/api/v2/me", headers=self.bearer_headers())
        self.seed_following_edges(user_id, 1, "me-over")
        over_limit = self.client.get("/api/v2/me", headers=self.bearer_headers())

        self.assertEqual(200, at_limit.status_code)
        self.assertEqual(
            {
                "following_count": 1,
                "max_following": 1,
                "status": "at_limit",
            },
            at_limit.json()["relationship_limits"]
        )
        self.assertEqual(200, over_limit.status_code)
        self.assertEqual(
            {
                "following_count": 2,
                "max_following": 1,
                "status": "over_limit",
            },
            over_limit.json()["relationship_limits"]
        )

    def test_get_me_relationship_limits_fail_closed_for_non_verified_paid_snapshots(self):
        cases = [
            {
                "name": "denied",
                "status": "ON_HOLD",
                "verification_state": "VERIFIED",
                "product_id": "xcpro_pro",
                "valid_until_ms": 1777777777000,
            },
            {
                "name": "pending",
                "status": "PENDING",
                "verification_state": "UNVERIFIED",
                "product_id": "xcpro_pro",
                "valid_until_ms": 1777777777000,
            },
            {
                "name": "recovery",
                "status": "RECOVERY_REQUIRED",
                "verification_state": "ACCOUNT_MISMATCH",
                "product_id": "xcpro_pro",
                "valid_until_ms": 1777777777000,
                "recovery_action": "CHOOSE_CORRECT_ACCOUNT",
            },
            {
                "name": "unverified_active",
                "status": "ACTIVE",
                "verification_state": "UNVERIFIED",
                "product_id": "xcpro_pro",
                "valid_until_ms": 1777777777000,
            },
            {
                "name": "malformed_product",
                "status": "ACTIVE",
                "verification_state": "VERIFIED",
                "product_id": "xcpro_basic",
                "valid_until_ms": 1777777777000,
            },
            {
                "name": "unknown_status",
                "status": "ALIEN_ACTIVE",
                "verification_state": "VERIFIED",
                "product_id": "xcpro_pro",
                "valid_until_ms": 1777777777000,
            },
        ]
        for case in cases:
            with self.subTest(case=case["name"]):
                token = self.add_static_bearer_token(
                    f"nonverified-token-{case['name']}",
                    f"nonverified-{case['name']}",
                    f"Nonverified {case['name']}"
                )
                self.upsert_entitlement_snapshot(
                    token=token,
                    tier="PRO",
                    billing_period="MONTHLY",
                    status=case["status"],
                    verification_state=case["verification_state"],
                    product_id=case["product_id"],
                    base_plan_id="monthly",
                    valid_until_ms=case["valid_until_ms"],
                    recovery_action=case.get("recovery_action", "NONE"),
                )

                response = self.client.get(
                    "/api/v2/me",
                    headers=self.bearer_headers(token)
                )

                self.assertEqual(200, response.status_code)
                self.assertEqual(
                    {
                        "following_count": 0,
                        "max_following": 1,
                        "status": "under_limit",
                    },
                    response.json()["relationship_limits"]
                )

    def test_subscription_entitlement_read_requires_bearer_auth(self):
        missing = self.client.get(
            "/api/v1/subscriptions/entitlements",
            headers=self.entitlement_headers_without_bearer()
        )
        invalid = self.client.get(
            "/api/v1/subscriptions/entitlements",
            headers=self.entitlement_headers("wrong-token")
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

    def test_subscription_entitlement_read_returns_canonical_free(self):
        response = self.client.get(
            "/api/v1/subscriptions/entitlements",
            headers=self.entitlement_headers()
        )

        self.assertEqual(200, response.status_code)
        body = response.json()
        entitlement = body["entitlement"]

        self.assertIsNone(body["auditId"])
        self.assertIsNotNone(entitlement["accountSubject"])
        self.assertEqual("FREE", entitlement["tier"])
        self.assertEqual("NONE", entitlement["billingPeriod"])
        self.assertEqual("FREE_ACTIVE", entitlement["status"])
        self.assertEqual("NONE", entitlement["source"])
        self.assertEqual("FREE_CANONICAL", entitlement["verificationState"])
        self.assertEqual([], entitlement["grantedFeatures"])
        self.assertIsNone(entitlement["productId"])
        self.assertIsNone(entitlement["basePlanId"])
        self.assertIsNone(entitlement["validUntilMs"])
        self.assertEqual(main_module.FREE_ENTITLEMENT_STALE_AFTER_MS, entitlement["staleAfterMs"])
        self.assertEqual(
            main_module.FREE_ENTITLEMENT_HARD_REFRESH_AFTER_MS,
            entitlement["hardRefreshAfterMs"]
        )
        self.assertEqual("NONE", entitlement["recoveryAction"])
        self.assertEqual(
            {
                "accountState": "UNKNOWN",
                "verifiedAtMs": None,
                "validUntilMs": None,
                "errorCode": None
            },
            entitlement["providerStates"]["skySight"]
        )
        self.assertEqual("UNKNOWN", entitlement["providerStates"]["pureTrack"]["userAccess"])

    def test_subscription_entitlement_read_rejects_expired_bearer(self):
        token = main_module.issue_private_follow_bearer(
            main_module.ResolvedBearerIdentity(
                provider="google",
                provider_subject="expired-pilot",
                email="expired@example.com",
                display_name="Expired Pilot"
            )
        )
        self.clock.advance(days=31)

        response = self.client.get(
            "/api/v1/subscriptions/entitlements",
            headers=self.entitlement_headers(token)
        )

        self.assertEqual(401, response.status_code)
        self.assertEqual(
            {
                "code": main_module.ErrorCode.UNAUTHENTICATED,
                "detail": "invalid bearer token"
            },
            response.json()
        )

    def test_subscription_entitlement_read_rejects_invalid_package_in_production(self):
        self.override_private_follow_runtime_config(
            runtime_env=main_module.RUNTIME_ENV_PROD,
            allow_debug_entitlement_package=False
        )

        unknown = self.client.get(
            "/api/v1/subscriptions/entitlements",
            headers=self.entitlement_headers(package_name="com.example.other")
        )
        debug = self.client.get(
            "/api/v1/subscriptions/entitlements",
            headers=self.entitlement_headers(package_name=main_module.XCPRO_DEBUG_PACKAGE_NAME)
        )

        self.assertEqual(400, unknown.status_code)
        self.assertEqual(
            {
                "code": main_module.ErrorCode.INVALID_PACKAGE,
                "detail": "invalid package name"
            },
            unknown.json()
        )
        self.assertEqual(400, debug.status_code)
        self.assertEqual(main_module.ErrorCode.INVALID_PACKAGE, debug.json()["code"])

    def test_subscription_entitlement_read_allows_debug_package_with_explicit_opt_in(self):
        self.override_private_follow_runtime_config(
            runtime_env=main_module.RUNTIME_ENV_PROD,
            allow_debug_entitlement_package=True
        )

        response = self.client.get(
            "/api/v1/subscriptions/entitlements",
            headers=self.entitlement_headers(package_name=main_module.XCPRO_DEBUG_PACKAGE_NAME)
        )

        self.assertEqual(200, response.status_code)
        self.assertEqual("FREE", response.json()["entitlement"]["tier"])

    def test_subscription_entitlement_dto_vocabulary_is_frozen(self):
        self.assertEqual(
            {"FREE", "BASIC", "SOARING", "XC", "PRO"},
            main_module.PLAN_TIER_VALUES
        )
        self.assertEqual({"NONE", "MONTHLY", "ANNUAL"}, main_module.BILLING_PERIOD_VALUES)
        self.assertEqual({"NONE", "GOOGLE_PLAY"}, main_module.ENTITLEMENT_SOURCE_VALUES)
        self.assertEqual(
            {
                "FREE_ACTIVE",
                "PENDING",
                "ACTIVE",
                "GRACE_PERIOD",
                "CANCELED_BUT_ACTIVE",
                "ON_HOLD",
                "PAUSED",
                "SUSPENDED",
                "EXPIRED",
                "REVOKED",
                "RECOVERY_REQUIRED",
                "ERROR",
            },
            main_module.SUBSCRIPTION_STATUS_VALUES
        )
        self.assertEqual(
            {
                "VERIFIED",
                "FREE_CANONICAL",
                "STALE_CACHE",
                "UNVERIFIED",
                "ACCOUNT_MISMATCH",
                "RECOVERY_REQUIRED",
                "ERROR",
            },
            main_module.VERIFICATION_STATE_VALUES
        )
        self.assertEqual(
            {
                "NONE",
                "SIGN_IN_REQUIRED",
                "CONTACT_SUPPORT",
                "CHOOSE_CORRECT_ACCOUNT",
                "OPEN_PLAY_SUBSCRIPTIONS",
                "RETRY_LATER",
            },
            main_module.RECOVERY_ACTION_VALUES
        )

    def test_subscription_entitlement_read_returns_paid_continuity_states(self):
        for status in ("ACTIVE", "GRACE_PERIOD", "CANCELED_BUT_ACTIVE"):
            with self.subTest(status=status):
                valid_until_ms = 1777777777000
                self.upsert_entitlement_snapshot(
                    tier="SOARING",
                    billing_period="MONTHLY",
                    status=status,
                    verification_state="VERIFIED",
                    product_id="xcpro_soaring",
                    base_plan_id="monthly",
                    valid_until_ms=valid_until_ms,
                    expiry_time_ms=valid_until_ms,
                    auto_renewing=status != "CANCELED_BUT_ACTIVE"
                )

                response = self.client.get(
                    "/api/v1/subscriptions/entitlements",
                    headers=self.entitlement_headers()
                )

                self.assertEqual(200, response.status_code)
                entitlement = response.json()["entitlement"]
                self.assertEqual("SOARING", entitlement["tier"])
                self.assertEqual("MONTHLY", entitlement["billingPeriod"])
                self.assertEqual(status, entitlement["status"])
                self.assertEqual("GOOGLE_PLAY", entitlement["source"])
                self.assertEqual("VERIFIED", entitlement["verificationState"])
                self.assertEqual("xcpro_soaring", entitlement["productId"])
                self.assertEqual("monthly", entitlement["basePlanId"])
                self.assertEqual(valid_until_ms, entitlement["validUntilMs"])
                self.assertEqual(
                    main_module.PAID_CONTINUITY_STALE_AFTER_MS,
                    entitlement["staleAfterMs"]
                )
                self.assertEqual(
                    main_module.PAID_CONTINUITY_HARD_REFRESH_AFTER_MS,
                    entitlement["hardRefreshAfterMs"]
                )

    def test_subscription_entitlement_read_denied_lifecycle_states_do_not_return_valid_until(self):
        for status in ("PENDING", "ON_HOLD", "PAUSED", "SUSPENDED", "EXPIRED", "REVOKED"):
            with self.subTest(status=status):
                self.upsert_entitlement_snapshot(
                    tier="PRO",
                    billing_period="ANNUAL",
                    status=status,
                    verification_state="UNVERIFIED" if status == "PENDING" else "VERIFIED",
                    product_id="xcpro_pro",
                    base_plan_id="annual",
                    valid_until_ms=1777777777000,
                    expiry_time_ms=1777777777000,
                )

                response = self.client.get(
                    "/api/v1/subscriptions/entitlements",
                    headers=self.entitlement_headers()
                )

                self.assertEqual(200, response.status_code)
                entitlement = response.json()["entitlement"]
                self.assertEqual(status, entitlement["status"])
                self.assertIsNone(entitlement["validUntilMs"])
                self.assertEqual(
                    main_module.DENIED_ENTITLEMENT_STALE_AFTER_MS,
                    entitlement["staleAfterMs"]
                )
                self.assertEqual(
                    main_module.DENIED_ENTITLEMENT_HARD_REFRESH_AFTER_MS,
                    entitlement["hardRefreshAfterMs"]
                )

    def test_subscription_entitlement_read_recovery_required_never_grants_access(self):
        self.upsert_entitlement_snapshot(
            tier="PRO",
            billing_period="MONTHLY",
            status="RECOVERY_REQUIRED",
            verification_state="ACCOUNT_MISMATCH",
            product_id="xcpro_pro",
            base_plan_id="monthly",
            valid_until_ms=1777777777000,
            recovery_action="CHOOSE_CORRECT_ACCOUNT",
        )

        response = self.client.get(
            "/api/v1/subscriptions/entitlements",
            headers=self.entitlement_headers()
        )

        self.assertEqual(200, response.status_code)
        entitlement = response.json()["entitlement"]
        self.assertEqual("RECOVERY_REQUIRED", entitlement["status"])
        self.assertEqual("ACCOUNT_MISMATCH", entitlement["verificationState"])
        self.assertEqual("CHOOSE_CORRECT_ACCOUNT", entitlement["recoveryAction"])
        self.assertIsNone(entitlement["validUntilMs"])

    def test_subscription_entitlement_read_unknown_stored_enum_fails_closed(self):
        self.upsert_entitlement_snapshot(status="ALIEN_ACTIVE")

        response = self.client.get(
            "/api/v1/subscriptions/entitlements",
            headers=self.entitlement_headers()
        )

        self.assertEqual(500, response.status_code)
        self.assertEqual(
            {
                "code": main_module.ErrorCode.ENTITLEMENT_STATE_INVALID,
                "detail": "stored entitlement status is invalid"
            },
            response.json()
        )

    def test_subscription_entitlement_read_product_base_plan_mismatch_fails_closed(self):
        self.upsert_entitlement_snapshot(
            tier="PRO",
            billing_period="MONTHLY",
            product_id="xcpro_basic",
            base_plan_id="monthly"
        )

        response = self.client.get(
            "/api/v1/subscriptions/entitlements",
            headers=self.entitlement_headers()
        )

        self.assertEqual(500, response.status_code)
        self.assertEqual(main_module.ErrorCode.ENTITLEMENT_STATE_INVALID, response.json()["code"])

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

    def test_owner_can_read_followers_and_following_lists_with_counts_and_states(self):
        current_profile = self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        follower_only = self.seed_profile_user(
            handle="follower.only",
            display_name="Follower Only"
        )
        followed_only = self.seed_profile_user(
            handle="followed.only",
            display_name="Followed Only"
        )
        mutual = self.seed_profile_user(
            handle="mutual.pilot",
            display_name="Mutual Pilot"
        )
        self.seed_follow_edge(follower_only["user_id"], current_profile["user_id"])
        self.seed_follow_edge(current_profile["user_id"], followed_only["user_id"])
        self.seed_follow_edge(mutual["user_id"], current_profile["user_id"])
        self.seed_follow_edge(current_profile["user_id"], mutual["user_id"])

        followers = self.client.get(
            f"/api/v2/users/{current_profile['user_id']}/followers",
            headers=self.bearer_headers()
        )
        following = self.client.get(
            f"/api/v2/users/{current_profile['user_id']}/following",
            headers=self.bearer_headers()
        )

        self.assertEqual(200, followers.status_code)
        self.assertEqual(200, following.status_code)
        self.assertEqual(2, followers.json()["total"])
        self.assertEqual(2, following.json()["total"])
        followers_by_handle = {
            item["handle"]: item
            for item in followers.json()["items"]
        }
        following_by_handle = {
            item["handle"]: item
            for item in following.json()["items"]
        }
        self.assertEqual("followed_by", followers_by_handle["follower.only"]["relationship_state"])
        self.assertEqual(0, followers_by_handle["follower.only"]["followers_count"])
        self.assertEqual(1, followers_by_handle["follower.only"]["following_count"])
        self.assertEqual("mutual", followers_by_handle["mutual.pilot"]["relationship_state"])
        self.assertEqual(1, followers_by_handle["mutual.pilot"]["followers_count"])
        self.assertEqual(1, followers_by_handle["mutual.pilot"]["following_count"])
        self.assertEqual("following", following_by_handle["followed.only"]["relationship_state"])
        self.assertEqual("mutual", following_by_handle["mutual.pilot"]["relationship_state"])

    def test_owner_only_relationship_lists_reject_non_owner(self):
        current_profile = self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        self.complete_profile(
            token=self.secondary_bearer_token,
            handle="pilot.two",
            display_name="Pilot Two"
        )

        followers = self.client.get(
            f"/api/v2/users/{current_profile['user_id']}/followers",
            headers=self.bearer_headers(self.secondary_bearer_token)
        )
        following = self.client.get(
            f"/api/v2/users/{current_profile['user_id']}/following",
            headers=self.bearer_headers(self.secondary_bearer_token)
        )

        self.assertEqual(403, followers.status_code)
        self.assertEqual(
            main_module.ErrorCode.NOT_AUTHORIZED_TO_VIEW_FOLLOWERS,
            followers.json()["code"]
        )
        self.assertEqual(403, following.status_code)
        self.assertEqual(
            main_module.ErrorCode.NOT_AUTHORIZED_TO_VIEW_FOLLOWING,
            following.json()["code"]
        )

    def test_public_relationship_lists_allow_signed_in_non_owner(self):
        target_profile = self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        self.patch_privacy(
            token=self.primary_bearer_token,
            connection_list_visibility="public"
        )
        viewer_profile = self.complete_profile(
            token=self.secondary_bearer_token,
            handle="pilot.two",
            display_name="Pilot Two"
        )
        followed = self.seed_profile_user(
            handle="public.followed",
            display_name="Public Followed"
        )
        self.seed_follow_edge(viewer_profile["user_id"], target_profile["user_id"])
        self.seed_follow_edge(target_profile["user_id"], followed["user_id"])

        followers = self.client.get(
            f"/api/v2/users/{target_profile['user_id']}/followers",
            headers=self.bearer_headers(self.secondary_bearer_token)
        )
        following = self.client.get(
            f"/api/v2/users/{target_profile['user_id']}/following",
            headers=self.bearer_headers(self.secondary_bearer_token)
        )

        self.assertEqual(200, followers.status_code)
        self.assertEqual(200, following.status_code)
        self.assertEqual(["pilot.two"], [item["handle"] for item in followers.json()["items"]])
        self.assertEqual(["public.followed"], [item["handle"] for item in following.json()["items"]])

    def test_mutuals_only_relationship_lists_allow_mutual_and_reject_non_mutual(self):
        target_profile = self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        self.patch_privacy(
            token=self.primary_bearer_token,
            connection_list_visibility="mutuals_only"
        )
        mutual_viewer = self.complete_profile(
            token=self.secondary_bearer_token,
            handle="pilot.two",
            display_name="Pilot Two"
        )
        non_mutual_viewer = self.complete_profile(
            token=self.tertiary_bearer_token,
            handle="pilot.three",
            display_name="Pilot Three"
        )
        self.seed_follow_edge(mutual_viewer["user_id"], target_profile["user_id"])
        self.seed_follow_edge(target_profile["user_id"], mutual_viewer["user_id"])
        self.seed_follow_request(non_mutual_viewer["user_id"], target_profile["user_id"])

        allowed = self.client.get(
            f"/api/v2/users/{target_profile['user_id']}/followers",
            headers=self.bearer_headers(self.secondary_bearer_token)
        )
        rejected = self.client.get(
            f"/api/v2/users/{target_profile['user_id']}/followers",
            headers=self.bearer_headers(self.tertiary_bearer_token)
        )

        self.assertEqual(200, allowed.status_code)
        self.assertEqual(["pilot.two"], [item["handle"] for item in allowed.json()["items"]])
        self.assertEqual(403, rejected.status_code)
        self.assertEqual(
            main_module.ErrorCode.NOT_AUTHORIZED_TO_VIEW_FOLLOWERS,
            rejected.json()["code"]
        )

    def test_relationship_lists_exclude_pending_requests_from_totals_and_counts(self):
        current_profile = self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        pending_follower = self.seed_profile_user(
            handle="pending.follower",
            display_name="Pending Follower"
        )
        pending_followed = self.seed_profile_user(
            handle="pending.followed",
            display_name="Pending Followed"
        )
        self.seed_follow_request(pending_follower["user_id"], current_profile["user_id"])
        self.seed_follow_request(current_profile["user_id"], pending_followed["user_id"])

        followers = self.client.get(
            f"/api/v2/users/{current_profile['user_id']}/followers",
            headers=self.bearer_headers()
        )
        following = self.client.get(
            f"/api/v2/users/{current_profile['user_id']}/following",
            headers=self.bearer_headers()
        )

        self.assertEqual(200, followers.status_code)
        self.assertEqual(200, following.status_code)
        self.assertEqual(0, followers.json()["total"])
        self.assertEqual([], followers.json()["items"])
        self.assertEqual(0, following.json()["total"])
        self.assertEqual([], following.json()["items"])

    def test_relationship_lists_use_offset_cursor_and_validate_cursor(self):
        current_profile = self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        old = self.seed_profile_user(handle="page.old", display_name="Page Old")
        self.seed_follow_edge(current_profile["user_id"], old["user_id"])
        self.clock.advance(seconds=1)
        middle = self.seed_profile_user(handle="page.middle", display_name="Page Middle")
        self.seed_follow_edge(current_profile["user_id"], middle["user_id"])
        self.clock.advance(seconds=1)
        newest = self.seed_profile_user(handle="page.new", display_name="Page New")
        self.seed_follow_edge(current_profile["user_id"], newest["user_id"])

        first_page = self.client.get(
            f"/api/v2/users/{current_profile['user_id']}/following",
            params={"limit": 2},
            headers=self.bearer_headers()
        )
        second_page = self.client.get(
            f"/api/v2/users/{current_profile['user_id']}/following",
            params={"limit": 2, "cursor": first_page.json()["next_cursor"]},
            headers=self.bearer_headers()
        )
        invalid_cursor = self.client.get(
            f"/api/v2/users/{current_profile['user_id']}/following",
            params={"cursor": "not-an-offset"},
            headers=self.bearer_headers()
        )

        self.assertEqual(200, first_page.status_code)
        self.assertEqual(3, first_page.json()["total"])
        self.assertEqual(
            ["page.new", "page.middle"],
            [item["handle"] for item in first_page.json()["items"]]
        )
        self.assertEqual("2", first_page.json()["next_cursor"])
        self.assertEqual(200, second_page.status_code)
        self.assertEqual(["page.old"], [item["handle"] for item in second_page.json()["items"]])
        self.assertIsNone(second_page.json()["next_cursor"])
        self.assertEqual(422, invalid_cursor.status_code)
        self.assertEqual(main_module.ErrorCode.VALIDATION_ERROR, invalid_cursor.json()["code"])

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

    def test_follow_request_cancel_clears_pending_and_allows_re_request(self):
        current_profile = self.complete_profile(
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
        request_id = create_response.json()["request_id"]

        cancel_response = self.client.delete(
            f"/api/v2/follow-requests/{request_id}",
            headers=self.bearer_headers()
        )
        outgoing_after = self.client.get(
            "/api/v2/follow-requests/outgoing",
            headers=self.bearer_headers()
        )
        incoming_after = self.client.get(
            "/api/v2/follow-requests/incoming",
            headers=self.bearer_headers(self.secondary_bearer_token)
        )
        search_after = self.client.get(
            "/api/v2/users/search",
            params={"q": "pilot.two"},
            headers=self.bearer_headers()
        )
        canceled_row = self.get_follow_request_row(
            current_profile["user_id"],
            target_profile["user_id"]
        )
        retry_response = self.client.post(
            "/api/v2/follow-requests",
            json={"target_user_id": target_profile["user_id"]},
            headers=self.bearer_headers()
        )

        self.assertEqual(200, cancel_response.status_code)
        self.assertEqual({"ok": True, "request_id": request_id}, cancel_response.json())
        self.assertEqual([], outgoing_after.json()["requests"])
        self.assertEqual([], incoming_after.json()["requests"])
        self.assertEqual("none", search_after.json()["users"][0]["relationship_state"])
        self.assertIsNone(canceled_row)
        self.assertEqual(200, retry_response.status_code)
        self.assertEqual("pending", retry_response.json()["status"])

    def test_follow_request_cancel_requires_requester_owner(self):
        self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        self.complete_profile(
            token=self.secondary_bearer_token,
            handle="pilot.two",
            display_name="Pilot Two"
        )
        target_profile = self.complete_profile(
            token=self.tertiary_bearer_token,
            handle="pilot.three",
            display_name="Pilot Three"
        )

        create_response = self.client.post(
            "/api/v2/follow-requests",
            json={"target_user_id": target_profile["user_id"]},
            headers=self.bearer_headers()
        )
        self.assertEqual(200, create_response.status_code)
        request_id = create_response.json()["request_id"]

        target_cancel_response = self.client.delete(
            f"/api/v2/follow-requests/{request_id}",
            headers=self.bearer_headers(self.tertiary_bearer_token)
        )
        other_user_cancel_response = self.client.delete(
            f"/api/v2/follow-requests/{request_id}",
            headers=self.bearer_headers(self.secondary_bearer_token)
        )
        outgoing_after = self.client.get(
            "/api/v2/follow-requests/outgoing",
            headers=self.bearer_headers()
        )

        self.assertEqual(404, target_cancel_response.status_code)
        self.assertEqual(
            main_module.ErrorCode.FOLLOW_REQUEST_NOT_FOUND,
            target_cancel_response.json()["code"]
        )
        self.assertEqual(404, other_user_cancel_response.status_code)
        self.assertEqual(
            main_module.ErrorCode.FOLLOW_REQUEST_NOT_FOUND,
            other_user_cancel_response.json()["code"]
        )
        self.assertEqual(1, len(outgoing_after.json()["requests"]))

    def test_follow_request_cancel_rejects_non_pending_request(self):
        current_profile = self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        target_profile = self.complete_profile(
            token=self.secondary_bearer_token,
            handle="pilot.two",
            display_name="Pilot Two"
        )
        declined_target_profile = self.complete_profile(
            token=self.tertiary_bearer_token,
            handle="pilot.three",
            display_name="Pilot Three"
        )
        self.patch_privacy(
            token=self.secondary_bearer_token,
            follow_policy="auto_approve"
        )

        accepted_response = self.client.post(
            "/api/v2/follow-requests",
            json={"target_user_id": target_profile["user_id"]},
            headers=self.bearer_headers()
        )
        self.assertEqual(200, accepted_response.status_code)
        self.assertEqual("accepted", accepted_response.json()["status"])

        self.seed_follow_request(
            current_profile["user_id"],
            declined_target_profile["user_id"],
            status=main_module.FOLLOW_REQUEST_STATUS_DECLINED
        )
        declined_row = self.get_follow_request_row(
            current_profile["user_id"],
            declined_target_profile["user_id"]
        )
        self.assertIsNotNone(declined_row)

        accepted_cancel_response = self.client.delete(
            f"/api/v2/follow-requests/{accepted_response.json()['request_id']}",
            headers=self.bearer_headers()
        )
        declined_cancel_response = self.client.delete(
            f"/api/v2/follow-requests/{declined_row.id}",
            headers=self.bearer_headers()
        )

        self.assertEqual(409, accepted_cancel_response.status_code)
        self.assertEqual(
            main_module.ErrorCode.FOLLOW_REQUEST_NOT_PENDING,
            accepted_cancel_response.json()["code"]
        )
        self.assertEqual(409, declined_cancel_response.status_code)
        self.assertEqual(
            main_module.ErrorCode.FOLLOW_REQUEST_NOT_PENDING,
            declined_cancel_response.json()["code"]
        )
        self.assertTrue(
            self.follow_edge_exists(
                current_profile["user_id"],
                target_profile["user_id"]
            )
        )

    def test_unfollow_removes_only_caller_edge_and_recomputes_relationship_state(self):
        current_profile = self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        target_profile = self.complete_profile(
            token=self.secondary_bearer_token,
            handle="pilot.two",
            display_name="Pilot Two"
        )
        self.seed_follow_edge(current_profile["user_id"], target_profile["user_id"])
        self.seed_follow_edge(target_profile["user_id"], current_profile["user_id"])

        response = self.client.delete(
            f"/api/v2/users/{target_profile['user_id']}/follow",
            headers=self.bearer_headers()
        )
        followers_after = self.client.get(
            f"/api/v2/users/{current_profile['user_id']}/followers",
            headers=self.bearer_headers()
        )
        following_after = self.client.get(
            f"/api/v2/users/{current_profile['user_id']}/following",
            headers=self.bearer_headers()
        )

        self.assertEqual(200, response.status_code)
        self.assertEqual(
            {
                "ok": True,
                "removed": True,
                "relationship_state": "followed_by",
            },
            response.json()
        )
        self.assertFalse(
            self.follow_edge_exists(current_profile["user_id"], target_profile["user_id"])
        )
        self.assertTrue(
            self.follow_edge_exists(target_profile["user_id"], current_profile["user_id"])
        )
        self.assertEqual(0, self.following_count(current_profile["user_id"]))
        self.assertEqual(1, self.following_count(target_profile["user_id"]))
        self.assertEqual(["pilot.two"], [item["handle"] for item in followers_after.json()["items"]])
        self.assertEqual([], following_after.json()["items"])

    def test_unfollow_is_idempotent_when_caller_is_not_following_target(self):
        current_profile = self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        target_profile = self.complete_profile(
            token=self.secondary_bearer_token,
            handle="pilot.two",
            display_name="Pilot Two"
        )
        self.seed_follow_edge(target_profile["user_id"], current_profile["user_id"])

        response = self.client.delete(
            f"/api/v2/users/{target_profile['user_id']}/follow",
            headers=self.bearer_headers()
        )
        repeat_response = self.client.delete(
            f"/api/v2/users/{target_profile['user_id']}/follow",
            headers=self.bearer_headers()
        )

        self.assertEqual(200, response.status_code)
        self.assertEqual(False, response.json()["removed"])
        self.assertEqual("followed_by", response.json()["relationship_state"])
        self.assertEqual(200, repeat_response.status_code)
        self.assertEqual(False, repeat_response.json()["removed"])
        self.assertTrue(
            self.follow_edge_exists(target_profile["user_id"], current_profile["user_id"])
        )

    def test_unfollow_revokes_follower_only_live_access_immediately(self):
        current_profile = self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        owner_profile = self.complete_profile(
            token=self.secondary_bearer_token,
            handle="pilot.two",
            display_name="Pilot Two"
        )
        self.seed_follow_edge(current_profile["user_id"], owner_profile["user_id"])
        session = self.start_authenticated_session(
            token=self.secondary_bearer_token,
            visibility="followers"
        )
        self.client.post(
            "/api/v1/position",
            json=self.position_payload(session["session_id"]),
            headers={"X-Session-Token": session["write_token"]}
        )

        read_before = self.client.get(
            f"/api/v2/live/session/{session['session_id']}",
            headers=self.bearer_headers()
        )
        active_before = self.client.get(
            "/api/v2/live/following/active",
            headers=self.bearer_headers()
        )
        unfollow_response = self.client.delete(
            f"/api/v2/users/{owner_profile['user_id']}/follow",
            headers=self.bearer_headers()
        )
        read_after = self.client.get(
            f"/api/v2/live/session/{session['session_id']}",
            headers=self.bearer_headers()
        )
        lookup_after = self.client.get(
            f"/api/v2/live/users/{owner_profile['user_id']}",
            headers=self.bearer_headers()
        )
        active_after = self.client.get(
            "/api/v2/live/following/active",
            headers=self.bearer_headers()
        )

        self.assertEqual(200, read_before.status_code)
        self.assertEqual(1, len(active_before.json()["items"]))
        self.assertEqual(200, unfollow_response.status_code)
        self.assertEqual(True, unfollow_response.json()["removed"])
        self.assertEqual(404, read_after.status_code)
        self.assertEqual(404, lookup_after.status_code)
        self.assertEqual([], active_after.json()["items"])

    def test_remove_follower_removes_only_inbound_edge_and_recomputes_relationship_state(self):
        current_profile = self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        follower_profile = self.complete_profile(
            token=self.secondary_bearer_token,
            handle="pilot.two",
            display_name="Pilot Two"
        )
        self.seed_follow_edge(current_profile["user_id"], follower_profile["user_id"])
        self.seed_follow_edge(follower_profile["user_id"], current_profile["user_id"])

        response = self.client.delete(
            f"/api/v2/me/followers/{follower_profile['user_id']}",
            headers=self.bearer_headers()
        )
        followers_after = self.client.get(
            f"/api/v2/users/{current_profile['user_id']}/followers",
            headers=self.bearer_headers()
        )
        following_after = self.client.get(
            f"/api/v2/users/{current_profile['user_id']}/following",
            headers=self.bearer_headers()
        )

        self.assertEqual(200, response.status_code)
        self.assertEqual(
            {
                "ok": True,
                "removed": True,
                "relationship_state": "following",
            },
            response.json()
        )
        self.assertTrue(
            self.follow_edge_exists(current_profile["user_id"], follower_profile["user_id"])
        )
        self.assertFalse(
            self.follow_edge_exists(follower_profile["user_id"], current_profile["user_id"])
        )
        self.assertEqual(1, self.following_count(current_profile["user_id"]))
        self.assertEqual(0, self.following_count(follower_profile["user_id"]))
        self.assertEqual([], followers_after.json()["items"])
        self.assertEqual(["pilot.two"], [item["handle"] for item in following_after.json()["items"]])

    def test_remove_follower_is_idempotent_when_user_is_not_a_follower(self):
        current_profile = self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        other_profile = self.complete_profile(
            token=self.secondary_bearer_token,
            handle="pilot.two",
            display_name="Pilot Two"
        )
        self.seed_follow_edge(current_profile["user_id"], other_profile["user_id"])

        response = self.client.delete(
            f"/api/v2/me/followers/{other_profile['user_id']}",
            headers=self.bearer_headers()
        )
        repeat_response = self.client.delete(
            f"/api/v2/me/followers/{other_profile['user_id']}",
            headers=self.bearer_headers()
        )

        self.assertEqual(200, response.status_code)
        self.assertEqual(False, response.json()["removed"])
        self.assertEqual("following", response.json()["relationship_state"])
        self.assertEqual(200, repeat_response.status_code)
        self.assertEqual(False, repeat_response.json()["removed"])
        self.assertTrue(
            self.follow_edge_exists(current_profile["user_id"], other_profile["user_id"])
        )

    def test_remove_follower_revokes_follower_only_live_access_immediately(self):
        owner_profile = self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        follower_profile = self.complete_profile(
            token=self.secondary_bearer_token,
            handle="pilot.two",
            display_name="Pilot Two"
        )
        self.seed_follow_edge(follower_profile["user_id"], owner_profile["user_id"])
        session = self.start_authenticated_session(visibility="followers")
        self.client.post(
            "/api/v1/position",
            json=self.position_payload(session["session_id"]),
            headers={"X-Session-Token": session["write_token"]}
        )

        read_before = self.client.get(
            f"/api/v2/live/session/{session['session_id']}",
            headers=self.bearer_headers(self.secondary_bearer_token)
        )
        active_before = self.client.get(
            "/api/v2/live/following/active",
            headers=self.bearer_headers(self.secondary_bearer_token)
        )
        remove_response = self.client.delete(
            f"/api/v2/me/followers/{follower_profile['user_id']}",
            headers=self.bearer_headers()
        )
        read_after = self.client.get(
            f"/api/v2/live/session/{session['session_id']}",
            headers=self.bearer_headers(self.secondary_bearer_token)
        )
        lookup_after = self.client.get(
            f"/api/v2/live/users/{owner_profile['user_id']}",
            headers=self.bearer_headers(self.secondary_bearer_token)
        )
        active_after = self.client.get(
            "/api/v2/live/following/active",
            headers=self.bearer_headers(self.secondary_bearer_token)
        )

        self.assertEqual(200, read_before.status_code)
        self.assertEqual(1, len(active_before.json()["items"]))
        self.assertEqual(200, remove_response.status_code)
        self.assertEqual(True, remove_response.json()["removed"])
        self.assertEqual(404, read_after.status_code)
        self.assertEqual(404, lookup_after.status_code)
        self.assertEqual([], active_after.json()["items"])

    def test_block_creates_row_and_duplicate_block_is_idempotent(self):
        current_profile = self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        target_profile = self.complete_profile(
            token=self.secondary_bearer_token,
            handle="pilot.two",
            display_name="Pilot Two"
        )

        first = self.client.post(
            "/api/v2/blocks",
            json={"target_user_id": target_profile["user_id"]},
            headers=self.bearer_headers()
        )
        second = self.client.post(
            "/api/v2/blocks",
            json={"target_user_id": target_profile["user_id"]},
            headers=self.bearer_headers()
        )

        self.assertEqual(200, first.status_code)
        self.assertEqual(
            {
                "ok": True,
                "blocked": True,
                "target_user_id": target_profile["user_id"],
            },
            first.json()
        )
        self.assertEqual(200, second.status_code)
        self.assertEqual(first.json(), second.json())
        self.assertEqual(1, self.block_count(current_profile["user_id"], target_profile["user_id"]))

    def test_block_rejects_self(self):
        current_profile = self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )

        response = self.client.post(
            "/api/v2/blocks",
            json={"target_user_id": current_profile["user_id"]},
            headers=self.bearer_headers()
        )

        self.assertEqual(422, response.status_code)
        self.assertEqual(main_module.ErrorCode.BLOCK_SELF, response.json()["code"])
        self.assertEqual(0, self.block_count(current_profile["user_id"], current_profile["user_id"]))

    def test_block_removes_follow_edges_both_directions(self):
        current_profile = self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        target_profile = self.complete_profile(
            token=self.secondary_bearer_token,
            handle="pilot.two",
            display_name="Pilot Two"
        )
        self.seed_follow_edge(current_profile["user_id"], target_profile["user_id"])
        self.seed_follow_edge(target_profile["user_id"], current_profile["user_id"])

        response = self.client.post(
            "/api/v2/blocks",
            json={"target_user_id": target_profile["user_id"]},
            headers=self.bearer_headers()
        )

        self.assertEqual(200, response.status_code)
        self.assertTrue(self.block_exists(current_profile["user_id"], target_profile["user_id"]))
        self.assertFalse(
            self.follow_edge_exists(current_profile["user_id"], target_profile["user_id"])
        )
        self.assertFalse(
            self.follow_edge_exists(target_profile["user_id"], current_profile["user_id"])
        )

    def test_block_removes_pending_follow_requests_both_directions(self):
        current_profile = self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        target_profile = self.complete_profile(
            token=self.secondary_bearer_token,
            handle="pilot.two",
            display_name="Pilot Two"
        )
        self.seed_follow_request(current_profile["user_id"], target_profile["user_id"])
        self.seed_follow_request(target_profile["user_id"], current_profile["user_id"])

        response = self.client.post(
            "/api/v2/blocks",
            json={"target_user_id": target_profile["user_id"]},
            headers=self.bearer_headers()
        )

        self.assertEqual(200, response.status_code)
        self.assertIsNone(
            self.get_follow_request_row(current_profile["user_id"], target_profile["user_id"])
        )
        self.assertIsNone(
            self.get_follow_request_row(target_profile["user_id"], current_profile["user_id"])
        )

    def test_block_keeps_closed_follow_request_history(self):
        current_profile = self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        target_profile = self.complete_profile(
            token=self.secondary_bearer_token,
            handle="pilot.two",
            display_name="Pilot Two"
        )
        self.seed_follow_request(
            current_profile["user_id"],
            target_profile["user_id"],
            status=main_module.FOLLOW_REQUEST_STATUS_ACCEPTED
        )
        self.seed_follow_request(
            target_profile["user_id"],
            current_profile["user_id"],
            status=main_module.FOLLOW_REQUEST_STATUS_DECLINED
        )

        response = self.client.post(
            "/api/v2/blocks",
            json={"target_user_id": target_profile["user_id"]},
            headers=self.bearer_headers()
        )

        self.assertEqual(200, response.status_code)
        self.assertEqual(
            main_module.FOLLOW_REQUEST_STATUS_ACCEPTED,
            self.get_follow_request_row(
                current_profile["user_id"],
                target_profile["user_id"]
            ).status
        )
        self.assertEqual(
            main_module.FOLLOW_REQUEST_STATUS_DECLINED,
            self.get_follow_request_row(
                target_profile["user_id"],
                current_profile["user_id"]
            ).status
        )

    def test_unblock_removes_block_row_and_restores_nothing(self):
        current_profile = self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        target_profile = self.complete_profile(
            token=self.secondary_bearer_token,
            handle="pilot.two",
            display_name="Pilot Two"
        )
        self.seed_follow_edge(current_profile["user_id"], target_profile["user_id"])
        self.seed_follow_request(target_profile["user_id"], current_profile["user_id"])
        block_response = self.client.post(
            "/api/v2/blocks",
            json={"target_user_id": target_profile["user_id"]},
            headers=self.bearer_headers()
        )
        self.assertEqual(200, block_response.status_code)

        first_unblock = self.client.delete(
            f"/api/v2/blocks/{target_profile['user_id']}",
            headers=self.bearer_headers()
        )
        second_unblock = self.client.delete(
            f"/api/v2/blocks/{target_profile['user_id']}",
            headers=self.bearer_headers()
        )

        self.assertEqual(200, first_unblock.status_code)
        self.assertEqual(
            {
                "ok": True,
                "removed": True,
                "target_user_id": target_profile["user_id"],
            },
            first_unblock.json()
        )
        self.assertEqual(200, second_unblock.status_code)
        self.assertEqual(
            {
                "ok": True,
                "removed": False,
                "target_user_id": target_profile["user_id"],
            },
            second_unblock.json()
        )
        self.assertFalse(self.block_exists(current_profile["user_id"], target_profile["user_id"]))
        self.assertFalse(
            self.follow_edge_exists(current_profile["user_id"], target_profile["user_id"])
        )
        self.assertIsNone(
            self.get_follow_request_row(target_profile["user_id"], current_profile["user_id"])
        )

    def test_blocked_pair_is_hidden_from_search_for_blocker_and_blocked_user(self):
        current_profile = self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        target_profile = self.complete_profile(
            token=self.secondary_bearer_token,
            handle="pilot.two",
            display_name="Pilot Two"
        )

        self.seed_block(current_profile["user_id"], target_profile["user_id"])
        blocker_search = self.client.get(
            "/api/v2/users/search",
            params={"q": "pilot.two"},
            headers=self.bearer_headers()
        )
        self.clear_block(current_profile["user_id"], target_profile["user_id"])
        self.seed_block(target_profile["user_id"], current_profile["user_id"])
        blocked_user_search = self.client.get(
            "/api/v2/users/search",
            params={"q": "pilot.two"},
            headers=self.bearer_headers()
        )

        self.assertEqual(200, blocker_search.status_code)
        self.assertEqual([], blocker_search.json()["users"])
        self.assertEqual(200, blocked_user_search.status_code)
        self.assertEqual([], blocked_user_search.json()["users"])

    def test_blocked_followers_and_following_rows_are_hidden_from_lists_and_totals(self):
        current_profile = self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        visible_follower = self.seed_profile_user(
            handle="visible.follower",
            display_name="Visible Follower"
        )
        blocked_follower = self.seed_profile_user(
            handle="blocked.follower",
            display_name="Blocked Follower"
        )
        visible_following = self.seed_profile_user(
            handle="visible.following",
            display_name="Visible Following"
        )
        blocked_following = self.seed_profile_user(
            handle="blocked.following",
            display_name="Blocked Following"
        )
        self.seed_follow_edge(visible_follower["user_id"], current_profile["user_id"])
        self.seed_follow_edge(blocked_follower["user_id"], current_profile["user_id"])
        self.seed_follow_edge(current_profile["user_id"], visible_following["user_id"])
        self.seed_follow_edge(current_profile["user_id"], blocked_following["user_id"])
        self.seed_block(current_profile["user_id"], blocked_follower["user_id"])
        self.seed_block(blocked_following["user_id"], current_profile["user_id"])

        followers = self.client.get(
            f"/api/v2/users/{current_profile['user_id']}/followers",
            headers=self.bearer_headers()
        )
        following = self.client.get(
            f"/api/v2/users/{current_profile['user_id']}/following",
            headers=self.bearer_headers()
        )

        self.assertEqual(200, followers.status_code)
        self.assertEqual(1, followers.json()["total"])
        self.assertEqual(
            ["visible.follower"],
            [item["handle"] for item in followers.json()["items"]]
        )
        self.assertEqual(200, following.status_code)
        self.assertEqual(1, following.json()["total"])
        self.assertEqual(
            ["visible.following"],
            [item["handle"] for item in following.json()["items"]]
        )

    def test_blocked_pending_requests_are_hidden_from_incoming_and_outgoing_lists(self):
        current_profile = self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        visible_incoming = self.seed_profile_user(
            handle="visible.incoming",
            display_name="Visible Incoming"
        )
        blocked_incoming = self.seed_profile_user(
            handle="blocked.incoming",
            display_name="Blocked Incoming"
        )
        visible_outgoing = self.seed_profile_user(
            handle="visible.outgoing",
            display_name="Visible Outgoing"
        )
        blocked_outgoing = self.seed_profile_user(
            handle="blocked.outgoing",
            display_name="Blocked Outgoing"
        )
        self.seed_follow_request(visible_incoming["user_id"], current_profile["user_id"])
        self.seed_follow_request(blocked_incoming["user_id"], current_profile["user_id"])
        self.seed_follow_request(current_profile["user_id"], visible_outgoing["user_id"])
        self.seed_follow_request(current_profile["user_id"], blocked_outgoing["user_id"])
        self.seed_block(current_profile["user_id"], blocked_incoming["user_id"])
        self.seed_block(blocked_outgoing["user_id"], current_profile["user_id"])

        incoming = self.client.get(
            "/api/v2/follow-requests/incoming",
            headers=self.bearer_headers()
        )
        outgoing = self.client.get(
            "/api/v2/follow-requests/outgoing",
            headers=self.bearer_headers()
        )

        self.assertEqual(200, incoming.status_code)
        self.assertEqual(
            ["visible.incoming"],
            [request["counterpart"]["handle"] for request in incoming.json()["requests"]]
        )
        self.assertEqual(200, outgoing.status_code)
        self.assertEqual(
            ["visible.outgoing"],
            [request["counterpart"]["handle"] for request in outgoing.json()["requests"]]
        )

    def test_push_token_register_requires_auth(self):
        response = self.client.post(
            "/api/v2/me/push-tokens",
            json=self.push_token_payload()
        )

        self.assertEqual(401, response.status_code)
        self.assertEqual(main_module.ErrorCode.UNAUTHENTICATED, response.json()["code"])

    def test_push_token_revoke_requires_auth(self):
        response = self.client.delete("/api/v2/me/push-tokens/device-1")

        self.assertEqual(401, response.status_code)
        self.assertEqual(main_module.ErrorCode.UNAUTHENTICATED, response.json()["code"])

    def test_push_token_register_requires_encryption_secret(self):
        main_module.PUSH_TOKEN_ENCRYPTION_SECRET = None

        response = self.client.post(
            "/api/v2/me/push-tokens",
            json=self.push_token_payload(),
            headers=self.bearer_headers()
        )

        self.assertEqual(503, response.status_code)
        self.assertEqual(
            main_module.ErrorCode.PUSH_TOKEN_ENCRYPTION_UNAVAILABLE,
            response.json()["code"]
        )

    def test_push_token_register_stores_hash_and_encrypted_secret_only(self):
        raw_token = "fake-fcm-token-register-1"
        user_id = self.user_id_for_token()

        response = self.client.post(
            "/api/v2/me/push-tokens",
            json=self.push_token_payload(token=raw_token),
            headers=self.bearer_headers()
        )

        self.assertEqual(200, response.status_code)
        self.assert_response_has_no_push_token(response.json(), raw_token)
        rows = self.get_device_push_token_rows()
        self.assertEqual(1, len(rows))
        row = rows[0]
        self.assertEqual(user_id, row.user_id)
        self.assertEqual("android", row.platform)
        self.assertEqual("fcm", row.provider)
        self.assertEqual("device-1", row.device_id)
        self.assertEqual("1.2.3", row.app_version)
        self.assertEqual(main_module.hash_push_token(raw_token), row.token_hash)
        self.assertNotEqual(raw_token, row.token_ciphertext)
        self.assertNotIn(raw_token, row.token_ciphertext)
        self.assertEqual(raw_token, main_module.decrypt_push_token(row.token_ciphertext))
        self.assertIsNone(row.revoked_at)

    def test_push_token_duplicate_same_device_updates_existing_row(self):
        self.client.post(
            "/api/v2/me/push-tokens",
            json=self.push_token_payload(token="fake-fcm-token-old", app_version="1.0.0"),
            headers=self.bearer_headers()
        )
        self.clock.advance(seconds=1)

        response = self.client.post(
            "/api/v2/me/push-tokens",
            json=self.push_token_payload(token="fake-fcm-token-new", app_version="2.0.0"),
            headers=self.bearer_headers()
        )

        self.assertEqual(200, response.status_code)
        rows = self.get_device_push_token_rows()
        self.assertEqual(1, len(rows))
        self.assertEqual("2.0.0", rows[0].app_version)
        self.assertEqual(main_module.hash_push_token("fake-fcm-token-new"), rows[0].token_hash)
        self.assertEqual("fake-fcm-token-new", main_module.decrypt_push_token(rows[0].token_ciphertext))

    def test_push_token_reregister_clears_revoked_at(self):
        self.client.post(
            "/api/v2/me/push-tokens",
            json=self.push_token_payload(token="fake-fcm-token-reregister"),
            headers=self.bearer_headers()
        )
        revoke = self.client.delete(
            "/api/v2/me/push-tokens/device-1",
            headers=self.bearer_headers()
        )
        self.assertEqual(200, revoke.status_code)
        self.assertTrue(revoke.json()["revoked"])
        self.assertIsNotNone(self.get_device_push_token_rows()[0].revoked_at)

        response = self.client.post(
            "/api/v2/me/push-tokens",
            json=self.push_token_payload(token="fake-fcm-token-reregister"),
            headers=self.bearer_headers()
        )

        self.assertEqual(200, response.status_code)
        row = self.get_device_push_token_rows()[0]
        self.assertIsNone(row.revoked_at)

    def test_push_token_revoke_is_idempotent(self):
        self.client.post(
            "/api/v2/me/push-tokens",
            json=self.push_token_payload(token="fake-fcm-token-revoke"),
            headers=self.bearer_headers()
        )

        first = self.client.delete(
            "/api/v2/me/push-tokens/device-1",
            headers=self.bearer_headers()
        )
        second = self.client.delete(
            "/api/v2/me/push-tokens/device-1",
            headers=self.bearer_headers()
        )

        self.assertEqual(200, first.status_code)
        self.assertEqual(200, second.status_code)
        self.assertTrue(first.json()["revoked"])
        self.assertFalse(second.json()["revoked"])
        self.assertIsNotNone(self.get_device_push_token_rows()[0].revoked_at)

    def test_push_token_register_rejects_unsupported_platform_and_provider(self):
        cases = [
            {"platform": "ios"},
            {"provider": "apns"},
        ]
        for override in cases:
            with self.subTest(override=override):
                payload = self.push_token_payload()
                payload.update(override)

                response = self.client.post(
                    "/api/v2/me/push-tokens",
                    json=payload,
                    headers=self.bearer_headers()
                )

                self.assertEqual(422, response.status_code)
                self.assertEqual(main_module.ErrorCode.INVALID_PUSH_TOKEN, response.json()["code"])

    def test_push_token_same_token_on_another_user_revokes_previous_active_row(self):
        shared_token = "fake-fcm-token-shared-device"
        primary_user_id = self.user_id_for_token(self.primary_bearer_token)
        secondary_user_id = self.user_id_for_token(self.secondary_bearer_token)
        self.client.post(
            "/api/v2/me/push-tokens",
            json=self.push_token_payload(token=shared_token, device_id="device-primary"),
            headers=self.bearer_headers(self.primary_bearer_token)
        )

        response = self.client.post(
            "/api/v2/me/push-tokens",
            json=self.push_token_payload(token=shared_token, device_id="device-secondary"),
            headers=self.bearer_headers(self.secondary_bearer_token)
        )

        self.assertEqual(200, response.status_code)
        rows = self.get_device_push_token_rows()
        self.assertEqual(2, len(rows))
        rows_by_user = {row.user_id: row for row in rows}
        self.assertIsNotNone(rows_by_user[primary_user_id].revoked_at)
        self.assertIsNone(rows_by_user[secondary_user_id].revoked_at)
        self.assertEqual(
            main_module.hash_push_token(shared_token),
            rows_by_user[secondary_user_id].token_hash
        )

    def test_follow_request_create_enqueues_received_notification_for_target(self):
        requester_profile = self.complete_profile(
            token=self.primary_bearer_token,
            handle="notify.requester",
            display_name="Notify Requester"
        )
        target_profile = self.complete_profile(
            token=self.secondary_bearer_token,
            handle="notify.target",
            display_name="Notify Target"
        )

        response = self.client.post(
            "/api/v2/follow-requests",
            json={"target_user_id": target_profile["user_id"]},
            headers=self.bearer_headers(self.primary_bearer_token)
        )

        self.assertEqual(200, response.status_code)
        events = self.get_notification_outbox_rows()
        self.assertEqual(1, len(events))
        event = events[0]
        self.assertEqual(
            main_module.NOTIFICATION_EVENT_FOLLOW_REQUEST_RECEIVED,
            event.event_type
        )
        self.assertEqual(target_profile["user_id"], event.recipient_user_id)
        self.assertEqual(requester_profile["user_id"], event.actor_user_id)
        self.assertEqual(response.json()["request_id"], event.follow_request_id)
        self.assertEqual(main_module.NOTIFICATION_OUTBOX_STATUS_PENDING, event.status)
        self.assertEqual(
            {
                "event_type": main_module.NOTIFICATION_EVENT_FOLLOW_REQUEST_RECEIVED,
                "recipient_user_id": target_profile["user_id"],
                "actor_user_id": requester_profile["user_id"],
                "follow_request_id": response.json()["request_id"],
            },
            json.loads(event.payload_json)
        )

    def test_follow_request_accept_enqueues_accepted_notification_for_requester(self):
        requester_profile = self.complete_profile(
            token=self.primary_bearer_token,
            handle="accept.notify.requester",
            display_name="Accept Notify Requester"
        )
        target_profile = self.complete_profile(
            token=self.secondary_bearer_token,
            handle="accept.notify.target",
            display_name="Accept Notify Target"
        )
        create_response = self.client.post(
            "/api/v2/follow-requests",
            json={"target_user_id": target_profile["user_id"]},
            headers=self.bearer_headers(self.primary_bearer_token)
        )
        self.assertEqual(200, create_response.status_code)

        accept_response = self.client.post(
            f"/api/v2/follow-requests/{create_response.json()['request_id']}/accept",
            headers=self.bearer_headers(self.secondary_bearer_token)
        )

        self.assertEqual(200, accept_response.status_code)
        accepted_events = [
            event for event in self.get_notification_outbox_rows()
            if event.event_type == main_module.NOTIFICATION_EVENT_FOLLOW_REQUEST_ACCEPTED
        ]
        self.assertEqual(1, len(accepted_events))
        event = accepted_events[0]
        self.assertEqual(requester_profile["user_id"], event.recipient_user_id)
        self.assertEqual(target_profile["user_id"], event.actor_user_id)
        self.assertEqual(create_response.json()["request_id"], event.follow_request_id)
        self.assertEqual(main_module.NOTIFICATION_OUTBOX_STATUS_PENDING, event.status)

    def test_auto_approved_follow_request_enqueues_new_follower_notification_for_target(self):
        requester_profile = self.complete_profile(
            token=self.primary_bearer_token,
            handle="auto.notify.requester",
            display_name="Auto Notify Requester"
        )
        target_profile = self.seed_profile_user(
            handle="auto.notify.target",
            display_name="Auto Notify Target",
            follow_policy="auto_approve"
        )

        response = self.client.post(
            "/api/v2/follow-requests",
            json={"target_user_id": target_profile["user_id"]},
            headers=self.bearer_headers(self.primary_bearer_token)
        )

        self.assertEqual(200, response.status_code)
        self.assertEqual("accepted", response.json()["status"])
        events = self.get_notification_outbox_rows()
        self.assertEqual(1, len(events))
        event = events[0]
        self.assertEqual(
            main_module.NOTIFICATION_EVENT_FOLLOW_NEW_FOLLOWER,
            event.event_type
        )
        self.assertEqual(target_profile["user_id"], event.recipient_user_id)
        self.assertEqual(requester_profile["user_id"], event.actor_user_id)
        self.assertEqual(response.json()["request_id"], event.follow_request_id)
        self.assertNotEqual(requester_profile["user_id"], event.recipient_user_id)

    def test_auto_approved_reciprocal_follow_request_enqueues_mutual_notification_for_target(self):
        requester_profile = self.complete_profile(
            token=self.primary_bearer_token,
            handle="auto.mutual.requester",
            display_name="Auto Mutual Requester"
        )
        target_profile = self.seed_profile_user(
            handle="auto.mutual.target",
            display_name="Auto Mutual Target",
            follow_policy="auto_approve"
        )
        self.seed_follow_edge(target_profile["user_id"], requester_profile["user_id"])

        response = self.client.post(
            "/api/v2/follow-requests",
            json={"target_user_id": target_profile["user_id"]},
            headers=self.bearer_headers(self.primary_bearer_token)
        )

        self.assertEqual(200, response.status_code)
        self.assertEqual("accepted", response.json()["status"])
        events = self.get_notification_outbox_rows()
        self.assertEqual(1, len(events))
        event = events[0]
        self.assertEqual(main_module.NOTIFICATION_EVENT_FOLLOW_MUTUAL, event.event_type)
        self.assertEqual(target_profile["user_id"], event.recipient_user_id)
        self.assertEqual(requester_profile["user_id"], event.actor_user_id)
        self.assertEqual(response.json()["request_id"], event.follow_request_id)

    def test_blocked_accept_creates_no_accepted_notification_event(self):
        requester_profile = self.complete_profile(
            token=self.primary_bearer_token,
            handle="blocked.notify.requester",
            display_name="Blocked Notify Requester"
        )
        target_profile = self.complete_profile(
            token=self.secondary_bearer_token,
            handle="blocked.notify.target",
            display_name="Blocked Notify Target"
        )
        create_response = self.client.post(
            "/api/v2/follow-requests",
            json={"target_user_id": target_profile["user_id"]},
            headers=self.bearer_headers(self.primary_bearer_token)
        )
        self.assertEqual(200, create_response.status_code)
        self.seed_block(target_profile["user_id"], requester_profile["user_id"])

        accept_response = self.client.post(
            f"/api/v2/follow-requests/{create_response.json()['request_id']}/accept",
            headers=self.bearer_headers(self.secondary_bearer_token)
        )

        self.assertEqual(409, accept_response.status_code)
        self.assertEqual(
            [],
            [
                event for event in self.get_notification_outbox_rows()
                if event.event_type == main_module.NOTIFICATION_EVENT_FOLLOW_REQUEST_ACCEPTED
            ]
        )

    def test_blocked_auto_approved_follow_creates_no_new_follower_or_mutual_notification(self):
        requester_profile = self.complete_profile(
            token=self.primary_bearer_token,
            handle="blocked.auto.requester",
            display_name="Blocked Auto Requester"
        )
        target_profile = self.seed_profile_user(
            handle="blocked.auto.target",
            display_name="Blocked Auto Target",
            follow_policy="auto_approve"
        )
        self.seed_block(target_profile["user_id"], requester_profile["user_id"])

        response = self.client.post(
            "/api/v2/follow-requests",
            json={"target_user_id": target_profile["user_id"]},
            headers=self.bearer_headers(self.primary_bearer_token)
        )

        self.assertEqual(409, response.status_code)
        self.assertEqual(
            [],
            [
                event for event in self.get_notification_outbox_rows()
                if event.event_type in {
                    main_module.NOTIFICATION_EVENT_FOLLOW_NEW_FOLLOWER,
                    main_module.NOTIFICATION_EVENT_FOLLOW_MUTUAL,
                }
            ]
        )

    def test_follow_request_accept_enqueues_mutual_instead_of_accepted_when_reciprocal_edge_exists(self):
        requester_profile = self.complete_profile(
            token=self.primary_bearer_token,
            handle="accept.mutual.requester",
            display_name="Accept Mutual Requester"
        )
        target_profile = self.complete_profile(
            token=self.secondary_bearer_token,
            handle="accept.mutual.target",
            display_name="Accept Mutual Target"
        )
        create_response = self.client.post(
            "/api/v2/follow-requests",
            json={"target_user_id": target_profile["user_id"]},
            headers=self.bearer_headers(self.primary_bearer_token)
        )
        self.assertEqual(200, create_response.status_code)
        self.seed_follow_edge(target_profile["user_id"], requester_profile["user_id"])

        accept_response = self.client.post(
            f"/api/v2/follow-requests/{create_response.json()['request_id']}/accept",
            headers=self.bearer_headers(self.secondary_bearer_token)
        )

        self.assertEqual(200, accept_response.status_code)
        events = self.get_notification_outbox_rows()
        accepted_events = [
            event for event in events
            if event.event_type == main_module.NOTIFICATION_EVENT_FOLLOW_REQUEST_ACCEPTED
        ]
        mutual_events = [
            event for event in events
            if event.event_type == main_module.NOTIFICATION_EVENT_FOLLOW_MUTUAL
        ]
        self.assertEqual([], accepted_events)
        self.assertEqual(1, len(mutual_events))
        event = mutual_events[0]
        self.assertEqual(requester_profile["user_id"], event.recipient_user_id)
        self.assertEqual(target_profile["user_id"], event.actor_user_id)
        self.assertEqual(create_response.json()["request_id"], event.follow_request_id)

    def test_duplicate_accept_does_not_enqueue_duplicate_accepted_notification(self):
        requester_profile = self.complete_profile(
            token=self.primary_bearer_token,
            handle="dup.notify.requester",
            display_name="Duplicate Notify Requester"
        )
        target_profile = self.complete_profile(
            token=self.secondary_bearer_token,
            handle="dup.notify.target",
            display_name="Duplicate Notify Target"
        )
        create_response = self.client.post(
            "/api/v2/follow-requests",
            json={"target_user_id": target_profile["user_id"]},
            headers=self.bearer_headers(self.primary_bearer_token)
        )
        self.assertEqual(200, create_response.status_code)
        accept_url = f"/api/v2/follow-requests/{create_response.json()['request_id']}/accept"

        first_accept = self.client.post(
            accept_url,
            headers=self.bearer_headers(self.secondary_bearer_token)
        )
        second_accept = self.client.post(
            accept_url,
            headers=self.bearer_headers(self.secondary_bearer_token)
        )

        self.assertEqual(200, first_accept.status_code)
        self.assertEqual(409, second_accept.status_code)
        self.assertEqual(
            1,
            len(
                [
                    event for event in self.get_notification_outbox_rows()
                    if event.event_type == main_module.NOTIFICATION_EVENT_FOLLOW_REQUEST_ACCEPTED
                ]
            )
        )
        self.assertNotEqual(requester_profile["user_id"], target_profile["user_id"])

    def test_notification_delivery_sends_pending_event_to_active_fcm_token(self):
        raw_token = "[TEST_PUSH_TOKEN_DELIVERY_ACTIVE]"
        requester_profile = self.complete_profile(
            token=self.primary_bearer_token,
            handle="deliver.notify.requester",
            display_name="Deliver Notify Requester"
        )
        target_profile = self.complete_profile(
            token=self.secondary_bearer_token,
            handle="deliver.notify.target",
            display_name="Deliver Notify Target"
        )
        register_response = self.client.post(
            "/api/v2/me/push-tokens",
            json=self.push_token_payload(token=raw_token),
            headers=self.bearer_headers(self.secondary_bearer_token)
        )
        self.assertEqual(200, register_response.status_code)
        create_response = self.client.post(
            "/api/v2/follow-requests",
            json={"target_user_id": target_profile["user_id"]},
            headers=self.bearer_headers(self.primary_bearer_token)
        )
        self.assertEqual(200, create_response.status_code)

        fake_sender = FakeFcmSender()
        summary = main_module.deliver_pending_notification_events(sender=fake_sender)

        self.assertEqual(
            {
                "events_attempted": 1,
                "events_sent": 1,
                "events_retryable_failed": 0,
                "events_failed": 0,
                "token_attempts": 1,
                "tokens_sent": 1,
            },
            summary
        )
        self.assertEqual(1, len(fake_sender.messages))
        self.assertEqual(raw_token, fake_sender.messages[0]["token"])
        self.assertEqual(
            {
                "event_type": main_module.NOTIFICATION_EVENT_FOLLOW_REQUEST_RECEIVED,
                "recipient_user_id": target_profile["user_id"],
                "actor_user_id": requester_profile["user_id"],
                "follow_request_id": create_response.json()["request_id"],
            },
            fake_sender.messages[0]["data"]
        )
        event = self.get_notification_outbox_rows()[0]
        self.assertEqual(main_module.NOTIFICATION_OUTBOX_STATUS_SENT, event.status)
        self.assertEqual(1, event.attempt_count)
        self.assertIsNotNone(event.last_attempt_at)
        self.assertIsNotNone(event.sent_at)
        self.assertIsNone(event.last_error)
        self.assertNotIn(raw_token, event.payload_json)
        self.assertNotIn(raw_token, event.dedupe_key)

    def test_notification_delivery_skips_revoked_fcm_tokens(self):
        raw_token = "[TEST_PUSH_TOKEN_DELIVERY_REVOKED]"
        target_profile = self.complete_profile(
            token=self.secondary_bearer_token,
            handle="revoked.notify.target",
            display_name="Revoked Notify Target"
        )
        self.complete_profile(
            token=self.primary_bearer_token,
            handle="revoked.notify.requester",
            display_name="Revoked Notify Requester"
        )
        register_response = self.client.post(
            "/api/v2/me/push-tokens",
            json=self.push_token_payload(token=raw_token, device_id="revoked-device"),
            headers=self.bearer_headers(self.secondary_bearer_token)
        )
        self.assertEqual(200, register_response.status_code)
        revoke_response = self.client.delete(
            "/api/v2/me/push-tokens/revoked-device",
            headers=self.bearer_headers(self.secondary_bearer_token)
        )
        self.assertEqual(200, revoke_response.status_code)
        create_response = self.client.post(
            "/api/v2/follow-requests",
            json={"target_user_id": target_profile["user_id"]},
            headers=self.bearer_headers(self.primary_bearer_token)
        )
        self.assertEqual(200, create_response.status_code)

        fake_sender = FakeFcmSender()
        summary = main_module.deliver_pending_notification_events(sender=fake_sender)

        self.assertEqual(0, len(fake_sender.messages))
        self.assertEqual(1, summary["events_failed"])
        event = self.get_notification_outbox_rows()[0]
        self.assertEqual(main_module.NOTIFICATION_OUTBOX_STATUS_FAILED, event.status)
        self.assertEqual("no active push tokens", event.last_error)
        self.assertFalse(event.last_error_retryable)
        self.assertNotIn(raw_token, event.last_error)

    def test_notification_delivery_retryable_provider_failure_remains_retryable(self):
        raw_token = "[TEST_PUSH_TOKEN_DELIVERY_RETRYABLE]"
        target_profile = self.complete_profile(
            token=self.secondary_bearer_token,
            handle="retry.notify.target",
            display_name="Retry Notify Target"
        )
        self.complete_profile(
            token=self.primary_bearer_token,
            handle="retry.notify.requester",
            display_name="Retry Notify Requester"
        )
        self.client.post(
            "/api/v2/me/push-tokens",
            json=self.push_token_payload(token=raw_token),
            headers=self.bearer_headers(self.secondary_bearer_token)
        )
        self.client.post(
            "/api/v2/follow-requests",
            json={"target_user_id": target_profile["user_id"]},
            headers=self.bearer_headers(self.primary_bearer_token)
        )

        fake_sender = FakeFcmSender(
            failures_by_token={
                raw_token: main_module.FcmDeliveryTemporarilyUnavailable(
                    "temporary provider outage"
                )
            }
        )
        summary = main_module.deliver_pending_notification_events(sender=fake_sender)

        self.assertEqual(1, summary["events_retryable_failed"])
        event = self.get_notification_outbox_rows()[0]
        self.assertEqual(main_module.NOTIFICATION_OUTBOX_STATUS_RETRYABLE_FAILED, event.status)
        self.assertTrue(event.last_error_retryable)
        self.assertEqual("temporary provider outage", event.last_error)
        self.assertNotIn(raw_token, event.last_error)

    def test_notification_delivery_non_retryable_provider_failure_is_terminal(self):
        raw_token = "[TEST_PUSH_TOKEN_DELIVERY_TERMINAL]"
        target_profile = self.complete_profile(
            token=self.secondary_bearer_token,
            handle="terminal.notify.target",
            display_name="Terminal Notify Target"
        )
        self.complete_profile(
            token=self.primary_bearer_token,
            handle="terminal.notify.req",
            display_name="Terminal Notify Requester"
        )
        self.client.post(
            "/api/v2/me/push-tokens",
            json=self.push_token_payload(token=raw_token),
            headers=self.bearer_headers(self.secondary_bearer_token)
        )
        self.client.post(
            "/api/v2/follow-requests",
            json={"target_user_id": target_profile["user_id"]},
            headers=self.bearer_headers(self.primary_bearer_token)
        )

        fake_sender = FakeFcmSender(
            failures_by_token={
                raw_token: main_module.FcmDeliveryRejected("invalid registration")
            }
        )
        summary = main_module.deliver_pending_notification_events(sender=fake_sender)

        self.assertEqual(1, summary["events_failed"])
        event = self.get_notification_outbox_rows()[0]
        self.assertEqual(main_module.NOTIFICATION_OUTBOX_STATUS_FAILED, event.status)
        self.assertFalse(event.last_error_retryable)
        self.assertEqual("invalid registration", event.last_error)
        self.assertNotIn(raw_token, event.last_error)

    def test_notification_delivery_missing_fcm_config_is_retryable(self):
        raw_token = "[TEST_PUSH_TOKEN_DELIVERY_MISSING_CONFIG]"
        target_profile = self.complete_profile(
            token=self.secondary_bearer_token,
            handle="config.notify.target",
            display_name="Config Notify Target"
        )
        self.complete_profile(
            token=self.primary_bearer_token,
            handle="config.notify.requester",
            display_name="Config Notify Requester"
        )
        self.client.post(
            "/api/v2/me/push-tokens",
            json=self.push_token_payload(token=raw_token),
            headers=self.bearer_headers(self.secondary_bearer_token)
        )
        self.client.post(
            "/api/v2/follow-requests",
            json={"target_user_id": target_profile["user_id"]},
            headers=self.bearer_headers(self.primary_bearer_token)
        )

        sender = main_module.FcmHttpV1Sender(
            config=main_module.FcmRuntimeConfig(
                project_id=None,
                service_account_json_path=None,
            )
        )
        summary = main_module.deliver_pending_notification_events(sender=sender)

        self.assertEqual(1, summary["events_retryable_failed"])
        event = self.get_notification_outbox_rows()[0]
        self.assertEqual(main_module.NOTIFICATION_OUTBOX_STATUS_RETRYABLE_FAILED, event.status)
        self.assertTrue(event.last_error_retryable)
        self.assertIn("missing XCPRO_FCM_PROJECT_ID", event.last_error)
        self.assertNotIn(raw_token, event.last_error)

    def test_notification_outbox_event_type_constraints_include_follow_back_events(self):
        expected_event_types = {
            main_module.NOTIFICATION_EVENT_FOLLOW_REQUEST_RECEIVED,
            main_module.NOTIFICATION_EVENT_FOLLOW_REQUEST_ACCEPTED,
            main_module.NOTIFICATION_EVENT_FOLLOW_NEW_FOLLOWER,
            main_module.NOTIFICATION_EVENT_FOLLOW_MUTUAL,
        }
        model_constraint = next(
            constraint
            for constraint in main_module.NotificationOutboxEvent.__table__.constraints
            if constraint.name == "ck_notification_outbox_events_event_type"
        )
        model_constraint_sql = str(model_constraint.sqltext)
        initial_migration_path = (
            Path(main_module.__file__).resolve().parent
            / "alembic"
            / "versions"
            / "1d9e6c4b8a2f_add_notification_outbox_events.py"
        )
        expansion_migration_path = (
            Path(main_module.__file__).resolve().parent
            / "alembic"
            / "versions"
            / "6b7c8d9e0f1a_expand_notification_outbox_event_types.py"
        )
        initial_migration_text = initial_migration_path.read_text(encoding="utf-8")
        expansion_migration_text = expansion_migration_path.read_text(encoding="utf-8")

        for event_type in expected_event_types:
            self.assertIn(event_type, model_constraint_sql)
            self.assertIn(event_type, initial_migration_text)
            self.assertIn(event_type, expansion_migration_text)

    def test_notification_delivery_script_requires_confirmation(self):
        args = deliver_notifications.parse_args([])

        with self.assertRaises(deliver_notifications.NotificationDeliveryScriptError):
            deliver_notifications.run(args, sender=FakeFcmSender())

    def test_notification_delivery_script_returns_aggregate_summary_without_token(self):
        raw_token = "[TEST_PUSH_TOKEN_DELIVERY_SCRIPT]"
        target_profile = self.complete_profile(
            token=self.secondary_bearer_token,
            handle="script.notify.target",
            display_name="Script Notify Target"
        )
        self.complete_profile(
            token=self.primary_bearer_token,
            handle="script.notify.req",
            display_name="Script Notify Requester"
        )
        self.client.post(
            "/api/v2/me/push-tokens",
            json=self.push_token_payload(token=raw_token),
            headers=self.bearer_headers(self.secondary_bearer_token)
        )
        self.client.post(
            "/api/v2/follow-requests",
            json={"target_user_id": target_profile["user_id"]},
            headers=self.bearer_headers(self.primary_bearer_token)
        )

        result = deliver_notifications.run(
            deliver_notifications.parse_args(["--confirm-send", "--limit", "10"]),
            sender=FakeFcmSender()
        )

        self.assertEqual(True, result["ok"])
        self.assertEqual(1, result["events_attempted"])
        self.assertEqual(1, result["events_sent"])
        self.assertNotIn(raw_token, json.dumps(result, sort_keys=True))

    def test_follow_request_create_rejects_blocked_relationship_in_either_direction(self):
        cases = [
            ("blocker", self.primary_bearer_token, self.secondary_bearer_token),
            ("blocked_user", self.secondary_bearer_token, self.primary_bearer_token),
        ]
        for label, requester_token, target_token in cases:
            with self.subTest(label=label):
                requester_profile = self.complete_profile(
                    token=requester_token,
                    handle=f"{label}.requester",
                    display_name=f"{label} Requester"
                )
                target_profile = self.complete_profile(
                    token=target_token,
                    handle=f"{label}.target",
                    display_name=f"{label} Target"
                )
                self.seed_block(
                    requester_profile["user_id"],
                    target_profile["user_id"]
                )
                if label == "blocked_user":
                    self.clear_block(
                        requester_profile["user_id"],
                        target_profile["user_id"]
                    )
                    self.seed_block(
                        target_profile["user_id"],
                        requester_profile["user_id"]
                    )

                response = self.client.post(
                    "/api/v2/follow-requests",
                    json={"target_user_id": target_profile["user_id"]},
                    headers=self.bearer_headers(requester_token)
                )

                self.assertEqual(409, response.status_code)
                self.assertEqual(
                    main_module.ErrorCode.BLOCKED_RELATIONSHIP,
                    response.json()["code"]
                )
                self.assertIsNone(
                    self.get_follow_request_row(
                        requester_profile["user_id"],
                        target_profile["user_id"]
                    )
                )

    def test_follow_request_accept_rejects_blocked_relationship_and_preserves_pending(self):
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
        create_response = self.client.post(
            "/api/v2/follow-requests",
            json={"target_user_id": target_profile["user_id"]},
            headers=self.bearer_headers()
        )
        self.assertEqual(200, create_response.status_code)
        self.seed_block(target_profile["user_id"], requester_profile["user_id"])

        accept_response = self.client.post(
            f"/api/v2/follow-requests/{create_response.json()['request_id']}/accept",
            headers=self.bearer_headers(self.secondary_bearer_token)
        )
        pending_row = self.get_follow_request_row(
            requester_profile["user_id"],
            target_profile["user_id"]
        )

        self.assertEqual(409, accept_response.status_code)
        self.assertEqual(
            main_module.ErrorCode.BLOCKED_RELATIONSHIP,
            accept_response.json()["code"]
        )
        self.assertEqual(main_module.FOLLOW_REQUEST_STATUS_PENDING, pending_row.status)
        self.assertFalse(
            self.follow_edge_exists(requester_profile["user_id"], target_profile["user_id"])
        )

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

    def test_free_user_at_following_cap_cannot_create_another_follow_request(self):
        self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        requester_user_id = self.user_id_for_token()
        self.seed_following_edges(requester_user_id, 1, "free-existing")
        target_profile = self.seed_profile_user(
            handle="free.target",
            display_name="Free Target"
        )

        response = self.client.post(
            "/api/v2/follow-requests",
            json={"target_user_id": target_profile["user_id"]},
            headers=self.bearer_headers()
        )

        self.assertEqual(409, response.status_code)
        self.assertEqual(
            main_module.ErrorCode.LIVEFOLLOW_FOLLOWING_LIMIT_EXCEEDED,
            response.json()["code"]
        )
        self.assert_response_has_no_purchase_token(response.json())
        self.assertEqual(1, self.following_count(requester_user_id))
        self.assertIsNone(
            self.get_follow_request_row(requester_user_id, target_profile["user_id"])
        )

    def test_basic_user_can_reach_four_and_is_blocked_on_fifth_follow(self):
        self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        self.upsert_entitlement_snapshot(
            tier="BASIC",
            billing_period="MONTHLY",
            status="ACTIVE",
            verification_state="VERIFIED",
            product_id="xcpro_basic",
            base_plan_id="monthly",
        )
        requester_user_id = self.user_id_for_token()

        for index in range(4):
            target = self.seed_profile_user(
                handle=f"basic.{index}",
                display_name=f"Basic Target {index}",
                follow_policy="auto_approve"
            )
            response = self.client.post(
                "/api/v2/follow-requests",
                json={"target_user_id": target["user_id"]},
                headers=self.bearer_headers()
            )
            self.assertEqual(200, response.status_code)
            self.assertEqual("accepted", response.json()["status"])

        blocked_target = self.seed_profile_user(
            handle="basic.blocked",
            display_name="Basic Blocked",
            follow_policy="auto_approve"
        )
        blocked = self.client.post(
            "/api/v2/follow-requests",
            json={"target_user_id": blocked_target["user_id"]},
            headers=self.bearer_headers()
        )

        self.assertEqual(4, self.following_count(requester_user_id))
        self.assertEqual(409, blocked.status_code)
        self.assertEqual(
            main_module.ErrorCode.LIVEFOLLOW_FOLLOWING_LIMIT_EXCEEDED,
            blocked.json()["code"]
        )

    def test_soaring_user_can_reach_fifteen_and_is_blocked_on_sixteenth_follow(self):
        self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        self.upsert_entitlement_snapshot(
            tier="SOARING",
            billing_period="MONTHLY",
            status="ACTIVE",
            verification_state="VERIFIED",
            product_id="xcpro_soaring",
            base_plan_id="monthly",
        )
        requester_user_id = self.user_id_for_token()

        for index in range(15):
            target = self.seed_profile_user(
                handle=f"soar.{index}",
                display_name=f"Soaring Target {index}",
                follow_policy="auto_approve"
            )
            response = self.client.post(
                "/api/v2/follow-requests",
                json={"target_user_id": target["user_id"]},
                headers=self.bearer_headers()
            )
            self.assertEqual(200, response.status_code)
            self.assertEqual("accepted", response.json()["status"])

        blocked_target = self.seed_profile_user(
            handle="soar.blocked",
            display_name="Soaring Blocked",
            follow_policy="auto_approve"
        )
        blocked = self.client.post(
            "/api/v2/follow-requests",
            json={"target_user_id": blocked_target["user_id"]},
            headers=self.bearer_headers()
        )

        self.assertEqual(15, self.following_count(requester_user_id))
        self.assertEqual(409, blocked.status_code)
        self.assertEqual(
            main_module.ErrorCode.LIVEFOLLOW_FOLLOWING_LIMIT_EXCEEDED,
            blocked.json()["code"]
        )

    def test_xc_and_pro_caps_use_seeded_follow_edges_without_large_http_loops(self):
        cases = [
            ("XC", "xcpro_xc", 50),
            ("PRO", "xcpro_pro", 100),
        ]
        for tier, product_id, cap in cases:
            with self.subTest(tier=tier):
                token = self.add_static_bearer_token(
                    f"limit-token-{tier.lower()}",
                    f"limit-{tier.lower()}",
                    f"Limit {tier}"
                )
                self.complete_profile(
                    token=token,
                    handle=f"limit.{tier.lower()}",
                    display_name=f"Limit {tier}"
                )
                self.upsert_entitlement_snapshot(
                    token=token,
                    tier=tier,
                    billing_period="MONTHLY",
                    status="ACTIVE",
                    verification_state="VERIFIED",
                    product_id=product_id,
                    base_plan_id="monthly",
                )
                requester_user_id = self.user_id_for_token(token)
                self.seed_following_edges(
                    requester_user_id,
                    cap - 1,
                    f"{tier.lower()}-seed"
                )

                allowed_target = self.seed_profile_user(
                    handle=f"{tier.lower()}.allowed",
                    display_name=f"{tier} Allowed",
                    follow_policy="auto_approve"
                )
                allowed = self.client.post(
                    "/api/v2/follow-requests",
                    json={"target_user_id": allowed_target["user_id"]},
                    headers=self.bearer_headers(token)
                )
                blocked_target = self.seed_profile_user(
                    handle=f"{tier.lower()}.blocked",
                    display_name=f"{tier} Blocked",
                    follow_policy="auto_approve"
                )
                blocked = self.client.post(
                    "/api/v2/follow-requests",
                    json={"target_user_id": blocked_target["user_id"]},
                    headers=self.bearer_headers(token)
                )

                self.assertEqual(200, allowed.status_code)
                self.assertEqual("accepted", allowed.json()["status"])
                self.assertEqual(cap, self.following_count(requester_user_id))
                self.assertEqual(409, blocked.status_code)
                self.assertEqual(
                    main_module.ErrorCode.LIVEFOLLOW_FOLLOWING_LIMIT_EXCEEDED,
                    blocked.json()["code"]
                )

    def test_auto_approve_checks_capacity_before_creating_request_or_edge(self):
        self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        requester_user_id = self.user_id_for_token()
        self.seed_following_edges(requester_user_id, 1, "auto-existing")
        target_profile = self.seed_profile_user(
            handle="auto.blocked",
            display_name="Auto Blocked",
            follow_policy="auto_approve"
        )

        response = self.client.post(
            "/api/v2/follow-requests",
            json={"target_user_id": target_profile["user_id"]},
            headers=self.bearer_headers()
        )

        self.assertEqual(409, response.status_code)
        self.assertEqual(
            main_module.ErrorCode.LIVEFOLLOW_FOLLOWING_LIMIT_EXCEEDED,
            response.json()["code"]
        )
        self.assertIsNone(
            self.get_follow_request_row(requester_user_id, target_profile["user_id"])
        )
        self.assertFalse(
            self.follow_edge_exists(requester_user_id, target_profile["user_id"])
        )

    def test_accept_checks_requester_capacity_not_accepting_target_capacity(self):
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
        create_response = self.client.post(
            "/api/v2/follow-requests",
            json={"target_user_id": target_profile["user_id"]},
            headers=self.bearer_headers()
        )
        self.assertEqual(200, create_response.status_code)
        self.seed_following_edges(requester_profile["user_id"], 1, "accept-requester")

        blocked_accept = self.client.post(
            f"/api/v2/follow-requests/{create_response.json()['request_id']}/accept",
            headers=self.bearer_headers(self.secondary_bearer_token)
        )

        self.assertEqual(409, blocked_accept.status_code)
        self.assertEqual(
            main_module.ErrorCode.LIVEFOLLOW_FOLLOWING_LIMIT_EXCEEDED,
            blocked_accept.json()["code"]
        )
        pending_row = self.get_follow_request_row(
            requester_profile["user_id"],
            target_profile["user_id"]
        )
        self.assertEqual("pending", pending_row.status)
        self.assertFalse(
            self.follow_edge_exists(requester_profile["user_id"], target_profile["user_id"])
        )

        requester_token = self.add_static_bearer_token(
            "accept-requester-token",
            "accept-requester",
            "Accept Requester"
        )
        target_token = self.add_static_bearer_token(
            "accept-target-token",
            "accept-target",
            "Accept Target"
        )
        requester_two = self.complete_profile(
            token=requester_token,
            handle="accept.req",
            display_name="Accept Requester"
        )
        target_two = self.complete_profile(
            token=target_token,
            handle="accept.target",
            display_name="Accept Target"
        )
        self.seed_following_edges(target_two["user_id"], 1, "accept-target")
        allowed_request = self.client.post(
            "/api/v2/follow-requests",
            json={"target_user_id": target_two["user_id"]},
            headers=self.bearer_headers(requester_token)
        )
        self.assertEqual(200, allowed_request.status_code)

        allowed_accept = self.client.post(
            f"/api/v2/follow-requests/{allowed_request.json()['request_id']}/accept",
            headers=self.bearer_headers(target_token)
        )

        self.assertEqual(200, allowed_accept.status_code)
        self.assertEqual("accepted", allowed_accept.json()["status"])
        self.assertTrue(
            self.follow_edge_exists(requester_two["user_id"], target_two["user_id"])
        )

    def test_follow_request_rejects_already_following_before_capacity(self):
        self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        requester_user_id = self.user_id_for_token()
        target_profile = self.seed_profile_user(
            handle="already.following",
            display_name="Already Following"
        )
        self.seed_follow_edge(requester_user_id, target_profile["user_id"])

        response = self.client.post(
            "/api/v2/follow-requests",
            json={"target_user_id": target_profile["user_id"]},
            headers=self.bearer_headers()
        )

        self.assertEqual(409, response.status_code)
        self.assertEqual(
            main_module.ErrorCode.ALREADY_FOLLOWING,
            response.json()["code"]
        )

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

    def test_blocked_follower_cannot_discover_or_read_follower_only_live_session(self):
        follower_profile = self.complete_profile(
            token=self.primary_bearer_token,
            handle="pilot.one",
            display_name="Pilot One"
        )
        owner_profile = self.complete_profile(
            token=self.secondary_bearer_token,
            handle="pilot.two",
            display_name="Pilot Two"
        )
        self.seed_follow_edge(follower_profile["user_id"], owner_profile["user_id"])
        self.seed_block(owner_profile["user_id"], follower_profile["user_id"])
        session = self.start_authenticated_session(
            token=self.secondary_bearer_token,
            visibility="followers"
        )
        self.client.post(
            "/api/v1/position",
            json=self.position_payload(session["session_id"]),
            headers={"X-Session-Token": session["write_token"]}
        )

        active = self.client.get(
            "/api/v2/live/following/active",
            headers=self.bearer_headers()
        )
        read = self.client.get(
            f"/api/v2/live/session/{session['session_id']}",
            headers=self.bearer_headers()
        )
        lookup = self.client.get(
            f"/api/v2/live/users/{owner_profile['user_id']}",
            headers=self.bearer_headers()
        )

        self.assertEqual(200, active.status_code)
        self.assertEqual([], active.json()["items"])
        self.assertEqual(404, read.status_code)
        self.assertEqual(404, lookup.status_code)

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

    def entitlement_headers(
        self,
        token: str | None = None,
        package_name: str = main_module.XCPRO_RELEASE_PACKAGE_NAME
    ):
        headers = self.bearer_headers(token)
        headers["X-XCPro-Package-Name"] = package_name
        return headers

    def entitlement_headers_without_bearer(
        self,
        package_name: str = main_module.XCPRO_RELEASE_PACKAGE_NAME
    ):
        return {"X-XCPro-Package-Name": package_name}

    def override_private_follow_runtime_config(self, **overrides):
        main_module.PRIVATE_FOLLOW_RUNTIME_CONFIG = replace(
            main_module.PRIVATE_FOLLOW_RUNTIME_CONFIG,
            **overrides
        )

    def add_static_bearer_token(
        self,
        token: str,
        subject: str,
        display_name: str
    ) -> str:
        main_module.STATIC_BEARER_TOKENS[token] = main_module.ResolvedBearerIdentity(
            provider="static",
            provider_subject=subject,
            email=f"{subject}@example.com",
            display_name=display_name
        )
        return token

    def user_id_for_token(self, token: str | None = None) -> str:
        bearer_token = token or self.primary_bearer_token
        self.client.get("/api/v2/me", headers=self.bearer_headers(bearer_token))
        identity = main_module.STATIC_BEARER_TOKENS[bearer_token]
        db = self.session_local()
        try:
            auth_identity = (
                db.query(main_module.AuthIdentity)
                .filter(
                    main_module.AuthIdentity.provider == identity.provider,
                    main_module.AuthIdentity.provider_subject == identity.provider_subject
                )
                .first()
            )
            self.assertIsNotNone(auth_identity)
            return auth_identity.user_id
        finally:
            db.close()

    def seed_profile_user(
        self,
        handle: str,
        display_name: str,
        follow_policy: str = "approval_required"
    ):
        now = self.clock.utcnow()
        user_id = str(main_module.uuid.uuid4())
        db = self.session_local()
        try:
            db.add(
                main_module.User(
                    id=user_id,
                    created_at=now,
                    updated_at=now,
                )
            )
            db.add(
                main_module.PilotProfile(
                    user_id=user_id,
                    handle=handle,
                    handle_normalized=handle.lower(),
                    display_name=display_name,
                    comp_number=None,
                    created_at=now,
                    updated_at=now,
                )
            )
            db.add(
                main_module.PrivacySetting(
                    user_id=user_id,
                    discoverability="searchable",
                    follow_policy=follow_policy,
                    default_live_visibility="followers",
                    connection_list_visibility="owner_only",
                    created_at=now,
                    updated_at=now,
                )
            )
            db.commit()
        finally:
            db.close()
        return {
            "user_id": user_id,
            "handle": handle,
            "display_name": display_name,
            "comp_number": None,
        }

    def seed_follow_edge(self, follower_user_id: str, followed_user_id: str) -> None:
        now = self.clock.utcnow()
        db = self.session_local()
        try:
            db.merge(
                main_module.FollowEdge(
                    follower_user_id=follower_user_id,
                    followed_user_id=followed_user_id,
                    created_at=now,
                    updated_at=now,
                )
            )
            db.commit()
        finally:
            db.close()

    def seed_follow_request(
        self,
        requester_user_id: str,
        target_user_id: str,
        status: str = main_module.FOLLOW_REQUEST_STATUS_PENDING
    ) -> None:
        now = self.clock.utcnow()
        db = self.session_local()
        try:
            db.add(
                main_module.FollowRequest(
                    id=str(main_module.uuid.uuid4()),
                    requester_user_id=requester_user_id,
                    target_user_id=target_user_id,
                    status=status,
                    responded_at=None,
                    created_at=now,
                    updated_at=now,
                )
            )
            db.commit()
        finally:
            db.close()

    def seed_following_edges(
        self,
        follower_user_id: str,
        count: int,
        handle_prefix: str
    ) -> None:
        for index in range(count):
            target = self.seed_profile_user(
                handle=f"{handle_prefix}.{index}",
                display_name=f"{handle_prefix} {index}",
            )
            self.seed_follow_edge(follower_user_id, target["user_id"])

    def seed_block(self, blocker_user_id: str, blocked_user_id: str) -> None:
        now = self.clock.utcnow()
        db = self.session_local()
        try:
            db.merge(
                main_module.UserBlock(
                    blocker_user_id=blocker_user_id,
                    blocked_user_id=blocked_user_id,
                    created_at=now,
                )
            )
            db.commit()
        finally:
            db.close()

    def clear_block(self, blocker_user_id: str, blocked_user_id: str) -> None:
        db = self.session_local()
        try:
            block = (
                db.query(main_module.UserBlock)
                .filter(
                    main_module.UserBlock.blocker_user_id == blocker_user_id,
                    main_module.UserBlock.blocked_user_id == blocked_user_id,
                )
                .first()
            )
            if block is not None:
                db.delete(block)
                db.commit()
        finally:
            db.close()

    def following_count(self, user_id: str) -> int:
        db = self.session_local()
        try:
            return (
                db.query(main_module.FollowEdge)
                .filter(main_module.FollowEdge.follower_user_id == user_id)
                .count()
            )
        finally:
            db.close()

    def get_follow_request_row(self, requester_user_id: str, target_user_id: str):
        db = self.session_local()
        try:
            return (
                db.query(main_module.FollowRequest)
                .filter(
                    main_module.FollowRequest.requester_user_id == requester_user_id,
                    main_module.FollowRequest.target_user_id == target_user_id,
                )
                .first()
            )
        finally:
            db.close()

    def follow_edge_exists(self, follower_user_id: str, followed_user_id: str) -> bool:
        db = self.session_local()
        try:
            return (
                db.query(main_module.FollowEdge)
                .filter(
                    main_module.FollowEdge.follower_user_id == follower_user_id,
                    main_module.FollowEdge.followed_user_id == followed_user_id,
                )
                .first()
                is not None
            )
        finally:
            db.close()

    def block_exists(self, blocker_user_id: str, blocked_user_id: str) -> bool:
        return self.block_count(blocker_user_id, blocked_user_id) > 0

    def block_count(self, blocker_user_id: str, blocked_user_id: str) -> int:
        db = self.session_local()
        try:
            return (
                db.query(main_module.UserBlock)
                .filter(
                    main_module.UserBlock.blocker_user_id == blocker_user_id,
                    main_module.UserBlock.blocked_user_id == blocked_user_id,
                )
                .count()
            )
        finally:
            db.close()

    def assert_response_has_no_purchase_token(self, body) -> None:
        serialized = json.dumps(body, sort_keys=True)
        self.assertNotIn("purchaseToken", serialized)
        self.assertNotIn("purchase_token", serialized)

    def assert_response_has_no_push_token(self, body, raw_token: str) -> None:
        serialized = json.dumps(body, sort_keys=True)
        self.assertNotIn(raw_token, serialized)
        self.assertNotIn("token_hash", serialized)
        self.assertNotIn("tokenHash", serialized)
        self.assertNotIn("token_ciphertext", serialized)
        self.assertNotIn("tokenCiphertext", serialized)

    def push_token_payload(
        self,
        token: str = "fake-fcm-token-1",
        device_id: str = "device-1",
        platform: str = "android",
        provider: str = "fcm",
        app_version: str | None = "1.2.3"
    ) -> dict:
        payload = {
            "token": token,
            "device_id": device_id,
            "platform": platform,
            "provider": provider,
        }
        if app_version is not None:
            payload["app_version"] = app_version
        return payload

    def get_device_push_token_rows(self):
        db = self.session_local()
        try:
            return (
                db.query(main_module.DevicePushToken)
                .order_by(main_module.DevicePushToken.created_at)
                .all()
            )
        finally:
            db.close()

    def get_notification_outbox_rows(self):
        db = self.session_local()
        try:
            return (
                db.query(main_module.NotificationOutboxEvent)
                .order_by(main_module.NotificationOutboxEvent.created_at)
                .all()
            )
        finally:
            db.close()

    def upsert_entitlement_snapshot(
        self,
        token: str | None = None,
        tier: str = "PRO",
        billing_period: str = "MONTHLY",
        status: str = "ACTIVE",
        source: str = "GOOGLE_PLAY",
        verification_state: str = "VERIFIED",
        product_id: str | None = "xcpro_pro",
        base_plan_id: str | None = "monthly",
        expiry_time_ms: int | None = 1777777777000,
        auto_renewing: bool | None = True,
        will_lose_access_at_ms: int | None = None,
        verified_at_ms: int | None = 1777000000000,
        fetched_at_ms: int = 1777000000000,
        valid_until_ms: int | None = 1777777777000,
        stale_after_ms: int | None = None,
        hard_refresh_after_ms: int | None = None,
        recovery_action: str = "NONE"
    ):
        user_id = self.user_id_for_token(token)
        now = self.clock.utcnow()
        db = self.session_local()
        try:
            db.merge(
                main_module.AccountEntitlementSnapshot(
                    user_id=user_id,
                    tier=tier,
                    billing_period=billing_period,
                    status=status,
                    source=source,
                    verification_state=verification_state,
                    product_id=product_id,
                    base_plan_id=base_plan_id,
                    expiry_time_ms=expiry_time_ms,
                    auto_renewing=auto_renewing,
                    will_lose_access_at_ms=will_lose_access_at_ms,
                    verified_at_ms=verified_at_ms,
                    fetched_at_ms=fetched_at_ms,
                    valid_until_ms=valid_until_ms,
                    stale_after_ms=stale_after_ms,
                    hard_refresh_after_ms=hard_refresh_after_ms,
                    recovery_action=recovery_action,
                    created_at=now,
                    updated_at=now,
                )
            )
            db.commit()
        finally:
            db.close()

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
                "Missing XCPRO_PUSH_TOKEN_ENCRYPTION_SECRET",
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
                        "blocks",
                        "device_push_tokens",
                        "notification_outbox_events",
                        "billing_google_purchases",
                        "billing_google_events",
                        "billing_audit_records",
                    }.issubset(table_names)
                )
                push_token_columns = {
                    column["name"] for column in db_inspector.get_columns("device_push_tokens")
                }
                self.assertTrue(
                    {
                        "id",
                        "user_id",
                        "platform",
                        "provider",
                        "token_hash",
                        "token_ciphertext",
                        "device_id",
                        "app_version",
                        "created_at",
                        "updated_at",
                        "revoked_at",
                    }.issubset(push_token_columns)
                )
                notification_outbox_columns = {
                    column["name"]
                    for column in db_inspector.get_columns("notification_outbox_events")
                }
                self.assertTrue(
                    {
                        "id",
                        "event_type",
                        "recipient_user_id",
                        "actor_user_id",
                        "follow_request_id",
                        "dedupe_key",
                        "status",
                        "attempt_count",
                        "last_attempt_at",
                        "sent_at",
                        "last_error",
                        "last_error_retryable",
                        "payload_json",
                        "created_at",
                        "updated_at",
                    }.issubset(notification_outbox_columns)
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
