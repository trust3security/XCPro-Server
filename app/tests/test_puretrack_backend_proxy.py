import json
import unittest
from datetime import datetime

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app import main as main_module


class MutableClock:
    def __init__(self, current: datetime):
        self.current = current

    def utcnow(self) -> datetime:
        return self.current


class FakePureTrackProviderClient:
    def __init__(self):
        self.result = main_module.PureTrackProviderLoginResult(
            result=main_module.PURETRACK_CONNECT_RESULT_CONNECTED,
            user_access=main_module.PURETRACK_PROVIDER_ACCESS_PREMIUM,
            provider_session_secret="provider-session-secret",
        )
        self.calls = []

    def login(
        self,
        email: str,
        password: str,
        config: main_module.PureTrackRuntimeConfig,
    ) -> main_module.PureTrackProviderLoginResult:
        self.calls.append({
            "email": email,
            "passwordLength": len(password),
            "hasAppKey": config.app_key is not None,
        })
        return self.result


class PureTrackBackendProxyTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        self.session_local = sessionmaker(bind=self.engine)
        main_module.Base.metadata.create_all(bind=self.engine)

        self.original_session_local = main_module.SessionLocal
        self.original_utcnow = main_module.utcnow
        self.original_private_follow_runtime_config = main_module.PRIVATE_FOLLOW_RUNTIME_CONFIG
        self.original_static_bearer_tokens = main_module.STATIC_BEARER_TOKENS
        self.original_puretrack_runtime_config = main_module.PURETRACK_RUNTIME_CONFIG
        self.original_puretrack_provider_client = main_module.PURETRACK_PROVIDER_CLIENT

        self.clock = MutableClock(datetime(2026, 6, 18, 8, 0, 0))
        self.primary_bearer = "puretrack-xcpro-bearer-1"
        main_module.SessionLocal = self.session_local
        main_module.utcnow = self.clock.utcnow
        main_module.PRIVATE_FOLLOW_RUNTIME_CONFIG = main_module.PrivateFollowRuntimeConfig(
            runtime_env=main_module.RUNTIME_ENV_DEV,
            allow_static_dev_bearer_auth=True,
            allow_debug_entitlement_package=True,
            has_static_bearer_tokens_env=False,
            static_bearer_tokens={},
            google_server_client_ids=frozenset(),
            private_follow_bearer_secret=None,
            push_token_encryption_secret=None,
            private_follow_bearer_ttl_seconds=main_module.DEFAULT_PRIVATE_FOLLOW_BEARER_TTL_SECONDS,
        )
        main_module.STATIC_BEARER_TOKENS = {}
        self.add_static_bearer(self.primary_bearer, "pilot-1")
        main_module.PURETRACK_RUNTIME_CONFIG = main_module.PureTrackRuntimeConfig(
            app_key="server-side-app-key",
            api_base_url="https://puretrack.example",
            timeout_seconds=2.0,
        )
        self.provider = FakePureTrackProviderClient()
        main_module.PURETRACK_PROVIDER_CLIENT = self.provider
        self.client = TestClient(main_module.app)

    def tearDown(self):
        self.client.close()
        main_module.SessionLocal = self.original_session_local
        main_module.utcnow = self.original_utcnow
        main_module.PRIVATE_FOLLOW_RUNTIME_CONFIG = self.original_private_follow_runtime_config
        main_module.STATIC_BEARER_TOKENS = self.original_static_bearer_tokens
        main_module.PURETRACK_RUNTIME_CONFIG = self.original_puretrack_runtime_config
        main_module.PURETRACK_PROVIDER_CLIENT = self.original_puretrack_provider_client
        main_module.Base.metadata.drop_all(bind=self.engine)
        self.engine.dispose()

    def test_required_routes_and_error_symbols_are_available(self):
        self.assertEqual(
            "puretrack_app_key_unconfigured",
            main_module.ErrorCode.PURETRACK_APP_KEY_UNCONFIGURED,
        )
        self.assertEqual(
            "puretrack_provider_unavailable",
            main_module.ErrorCode.PURETRACK_PROVIDER_UNAVAILABLE,
        )
        self.assertEqual(
            "puretrack_rate_limited",
            main_module.ErrorCode.PURETRACK_RATE_LIMITED,
        )
        self.assertEqual(
            "puretrack_state_invalid",
            main_module.ErrorCode.PURETRACK_STATE_INVALID,
        )
        methods_by_path = {
            getattr(route, "path", None): getattr(route, "methods", set())
            for route in main_module.app.routes
        }
        self.assertIn("GET", methods_by_path["/api/v1/puretrack/status"])
        self.assertIn("POST", methods_by_path["/api/v1/puretrack/connect"])
        self.assertIn("POST", methods_by_path["/api/v1/puretrack/disconnect"])

    def test_env_config_loader_uses_contract_defaults_and_values(self):
        default_config = main_module.load_puretrack_runtime_config({})
        self.assertIsNone(default_config.app_key)
        self.assertEqual(main_module.PURETRACK_DEFAULT_API_BASE_URL, default_config.api_base_url)
        self.assertEqual(10.0, default_config.timeout_seconds)

        configured = main_module.load_puretrack_runtime_config({
            "XCPRO_PURETRACK_APP_KEY": " app-key-value ",
            "XCPRO_PURETRACK_API_BASE_URL": "https://puretrack.test/",
            "XCPRO_PURETRACK_TIMEOUT_SECONDS": "3.5",
        })
        self.assertEqual("app-key-value", configured.app_key)
        self.assertEqual("https://puretrack.test", configured.api_base_url)
        self.assertEqual(3.5, configured.timeout_seconds)

    def test_status_requires_valid_xcpro_bearer_and_package(self):
        missing_auth = self.client.get(
            "/api/v1/puretrack/status",
            headers=self.headers(include_auth=False),
        )
        invalid_auth = self.client.get(
            "/api/v1/puretrack/status",
            headers=self.headers(token="unknown-bearer"),
        )
        invalid_package = self.client.get(
            "/api/v1/puretrack/status",
            headers=self.headers(package_name="com.example.other"),
        )

        self.assertEqual(401, missing_auth.status_code)
        self.assertEqual(main_module.ErrorCode.UNAUTHENTICATED, missing_auth.json()["code"])
        self.assertEqual(401, invalid_auth.status_code)
        self.assertEqual(main_module.ErrorCode.UNAUTHENTICATED, invalid_auth.json()["code"])
        self.assertEqual(400, invalid_package.status_code)
        self.assertEqual(main_module.ErrorCode.INVALID_PACKAGE, invalid_package.json()["code"])

    def test_status_without_app_key_reports_disabled_and_disconnected(self):
        main_module.PURETRACK_RUNTIME_CONFIG = main_module.PureTrackRuntimeConfig(
            app_key=None,
            api_base_url="https://puretrack.example",
            timeout_seconds=2.0,
        )

        response = self.client.get(
            "/api/v1/puretrack/status",
            headers=self.headers(),
        )

        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertEqual(self.status_keys(), set(body.keys()))
        self.assertIs(False, body["connected"])
        self.assertIs(False, body["appKeyConfigured"])
        self.assertIs(False, body["trafficApiAllowed"])
        self.assertIs(False, body["insertApiConfigured"])
        self.assertEqual(main_module.PURETRACK_PROVIDER_ACCESS_UNKNOWN, body["userAccess"])
        self.assertIsNone(body["verifiedAtMs"])
        self.assertIsNone(body["validUntilMs"])
        self.assertIsNone(body["accountLabel"])
        self.assertIsNone(body["errorCode"])
        self.assertIsNone(body["retryAfterMs"])
        self.assertIsNone(body["auditId"])

    def test_status_disconnected_default_with_app_key_configured(self):
        response = self.client.get(
            "/api/v1/puretrack/status",
            headers=self.headers(),
        )

        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertEqual(self.status_keys(), set(body.keys()))
        self.assertIs(False, body["connected"])
        self.assertIs(True, body["appKeyConfigured"])
        self.assertIs(False, body["trafficApiAllowed"])
        self.assertIs(False, body["insertApiConfigured"])
        self.assertEqual(main_module.PURETRACK_PROVIDER_ACCESS_UNKNOWN, body["userAccess"])
        self.assertIsNone(body["verifiedAtMs"])
        self.assertIsNone(body["validUntilMs"])
        self.assertIsNone(body["accountLabel"])
        self.assertIsNone(body["errorCode"])
        self.assertIsNone(body["retryAfterMs"])
        self.assertIsNone(body["auditId"])

    def test_connect_rejects_invalid_request_shapes(self):
        cases = (
            {},
            {"email": "", "password": "provider-password"},
            {"email": "   ", "password": "provider-password"},
            {"email": "a" * 321, "password": "provider-password"},
            {"email": "pilot@example.com", "password": ""},
            {"email": "pilot@example.com", "password": "p" * 1025},
            {
                "email": "pilot@example.com",
                "password": "provider-password",
                "unexpected": True,
            },
        )
        for payload in cases:
            with self.subTest(payload_keys=sorted(payload.keys())):
                response = self.client.post(
                    "/api/v1/puretrack/connect",
                    json=payload,
                    headers=self.headers(),
                )

                self.assertEqual(422, response.status_code)
                self.assertEqual(
                    main_module.ErrorCode.VALIDATION_ERROR,
                    response.json()["code"],
                )
        self.assertEqual([], self.provider.calls)

    def test_connect_success_premium_with_pro_entitlement_allows_traffic_and_redacts(self):
        self.upsert_entitlement_snapshot(tier="PRO")
        password = "provider-password-1"
        response = self.client.post(
            "/api/v1/puretrack/connect",
            json={"email": "pilot@example.com", "password": password},
            headers=self.headers(),
        )

        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertEqual(main_module.PURETRACK_CONNECT_RESULT_CONNECTED, body["result"])
        status = body["status"]
        self.assertIs(True, status["connected"])
        self.assertIs(True, status["appKeyConfigured"])
        self.assertIs(True, status["trafficApiAllowed"])
        self.assertIs(False, status["insertApiConfigured"])
        self.assertEqual(main_module.PURETRACK_PROVIDER_ACCESS_PREMIUM, status["userAccess"])
        self.assertEqual("p***@example.com", status["accountLabel"])
        self.assertIsNone(status["errorCode"])
        self.assertEqual(self.now_ms(), status["verifiedAtMs"])
        self.assertEqual(
            self.now_ms() + main_module.PURETRACK_PROVIDER_STATUS_CACHE_MS,
            status["validUntilMs"],
        )
        self.assertTrue(status["auditId"].startswith("pt_"))
        self.assertEqual([{
            "email": "pilot@example.com",
            "passwordLength": len(password),
            "hasAppKey": True,
        }], self.provider.calls)
        self.assert_no_secret_leakage(body, password=password)
        db = self.session_local()
        try:
            row = db.query(main_module.PureTrackProviderSession).one()
            self.assertEqual(main_module.hash_token("provider-session-secret"), row.provider_session_hash)
            self.assertNotEqual("provider-session-secret", row.provider_session_hash)
            self.assertEqual("p***@example.com", row.account_label)
        finally:
            db.close()

    def test_connect_success_free_provider_does_not_allow_traffic(self):
        self.upsert_entitlement_snapshot(tier="PRO")
        self.provider.result = main_module.PureTrackProviderLoginResult(
            result=main_module.PURETRACK_CONNECT_RESULT_CONNECTED,
            user_access=main_module.PURETRACK_PROVIDER_ACCESS_FREE,
            provider_session_secret="free-provider-session-secret",
        )

        response = self.client.post(
            "/api/v1/puretrack/connect",
            json={"email": "pilot@example.com", "password": "provider-password"},
            headers=self.headers(),
        )

        self.assertEqual(200, response.status_code)
        status = response.json()["status"]
        self.assertIs(True, status["connected"])
        self.assertEqual(main_module.PURETRACK_PROVIDER_ACCESS_FREE, status["userAccess"])
        self.assertIs(False, status["trafficApiAllowed"])

    def test_traffic_api_allowed_requires_app_key_xcpro_pro_and_premium_provider(self):
        cases = (
            ("all_gates", "server-side-app-key", "PRO", main_module.PURETRACK_PROVIDER_ACCESS_PREMIUM, True),
            ("missing_app_key", None, "PRO", main_module.PURETRACK_PROVIDER_ACCESS_PREMIUM, False),
            ("xcpro_xc", "server-side-app-key", "XC", main_module.PURETRACK_PROVIDER_ACCESS_PREMIUM, False),
            ("provider_free", "server-side-app-key", "PRO", main_module.PURETRACK_PROVIDER_ACCESS_FREE, False),
        )
        for name, app_key, tier, user_access, expected_allowed in cases:
            with self.subTest(name=name):
                token = f"puretrack-bearer-{name}"
                self.add_static_bearer(token, name)
                main_module.PURETRACK_RUNTIME_CONFIG = main_module.PureTrackRuntimeConfig(
                    app_key=app_key,
                    api_base_url="https://puretrack.example",
                    timeout_seconds=2.0,
                )
                self.upsert_entitlement_snapshot(token=token, tier=tier)
                self.upsert_provider_session(token=token, user_access=user_access)

                response = self.client.get(
                    "/api/v1/puretrack/status",
                    headers=self.headers(token=token),
                )

                self.assertEqual(200, response.status_code)
                self.assertIs(expected_allowed, response.json()["trafficApiAllowed"])

    def test_connect_maps_provider_results_without_exposing_secrets(self):
        cases = (
            (
                main_module.PureTrackProviderLoginResult(
                    result=main_module.PURETRACK_CONNECT_RESULT_AUTH_REJECTED,
                    user_access=main_module.PURETRACK_PROVIDER_ACCESS_NONE,
                    error_code="auth_rejected",
                ),
                main_module.PURETRACK_CONNECT_RESULT_AUTH_REJECTED,
                "auth_rejected",
                None,
            ),
            (
                main_module.PureTrackProviderLoginResult(
                    result=main_module.PURETRACK_CONNECT_RESULT_PROVIDER_UNAVAILABLE,
                    user_access=main_module.PURETRACK_PROVIDER_ACCESS_ERROR,
                    error_code=main_module.ErrorCode.PURETRACK_PROVIDER_UNAVAILABLE,
                ),
                main_module.PURETRACK_CONNECT_RESULT_PROVIDER_UNAVAILABLE,
                main_module.ErrorCode.PURETRACK_PROVIDER_UNAVAILABLE,
                None,
            ),
            (
                main_module.PureTrackProviderLoginResult(
                    result=main_module.PURETRACK_CONNECT_RESULT_RATE_LIMITED,
                    user_access=main_module.PURETRACK_PROVIDER_ACCESS_ERROR,
                    error_code=main_module.ErrorCode.PURETRACK_RATE_LIMITED,
                    retry_after_ms=30_000,
                ),
                main_module.PURETRACK_CONNECT_RESULT_RATE_LIMITED,
                main_module.ErrorCode.PURETRACK_RATE_LIMITED,
                30_000,
            ),
            (
                main_module.PureTrackProviderLoginResult(
                    result=main_module.PURETRACK_CONNECT_RESULT_ERROR,
                    user_access=main_module.PURETRACK_PROVIDER_ACCESS_ERROR,
                    error_code="malformed_provider_response",
                ),
                main_module.PURETRACK_CONNECT_RESULT_ERROR,
                "malformed_provider_response",
                None,
            ),
        )
        for provider_result, expected_result, expected_error, expected_retry in cases:
            with self.subTest(expected_result=expected_result):
                self.provider.result = provider_result
                password = "provider-password"
                response = self.client.post(
                    "/api/v1/puretrack/connect",
                    json={"email": "pilot@example.com", "password": password},
                    headers=self.headers(),
                )

                self.assertEqual(200, response.status_code)
                body = response.json()
                self.assertEqual(expected_result, body["result"])
                self.assertIs(False, body["status"]["connected"])
                self.assertEqual(expected_error, body["status"]["errorCode"])
                self.assertEqual(expected_retry, body["status"]["retryAfterMs"])
                self.assert_no_secret_leakage(body, password=password)

        main_module.PURETRACK_RUNTIME_CONFIG = main_module.PureTrackRuntimeConfig(
            app_key=None,
            api_base_url="https://puretrack.example",
            timeout_seconds=2.0,
        )
        call_count_before = len(self.provider.calls)
        response = self.client.post(
            "/api/v1/puretrack/connect",
            json={"email": "pilot@example.com", "password": "provider-password"},
            headers=self.headers(),
        )

        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertEqual(main_module.PURETRACK_CONNECT_RESULT_APP_KEY_UNCONFIGURED, body["result"])
        self.assertEqual(
            main_module.ErrorCode.PURETRACK_APP_KEY_UNCONFIGURED,
            body["status"]["errorCode"],
        )
        self.assertEqual(call_count_before, len(self.provider.calls))

    def test_disconnect_handles_connected_and_not_connected_states(self):
        self.upsert_entitlement_snapshot(tier="PRO")
        connect = self.client.post(
            "/api/v1/puretrack/connect",
            json={"email": "pilot@example.com", "password": "provider-password"},
            headers=self.headers(),
        )
        self.assertEqual(200, connect.status_code)
        self.assertIs(True, connect.json()["status"]["connected"])

        disconnected = self.client.post(
            "/api/v1/puretrack/disconnect",
            json={},
            headers=self.headers(),
        )
        not_connected = self.client.post(
            "/api/v1/puretrack/disconnect",
            json={},
            headers=self.headers(),
        )

        self.assertEqual(200, disconnected.status_code)
        disconnected_body = disconnected.json()
        self.assertEqual(
            main_module.PURETRACK_DISCONNECT_RESULT_DISCONNECTED,
            disconnected_body["result"],
        )
        self.assertIs(False, disconnected_body["status"]["connected"])
        self.assertIs(False, disconnected_body["status"]["trafficApiAllowed"])
        self.assertEqual(
            main_module.PURETRACK_PROVIDER_ACCESS_NONE,
            disconnected_body["status"]["userAccess"],
        )
        self.assertEqual(200, not_connected.status_code)
        self.assertEqual(
            main_module.PURETRACK_DISCONNECT_RESULT_NOT_CONNECTED,
            not_connected.json()["result"],
        )
        db = self.session_local()
        try:
            self.assertEqual(0, db.query(main_module.PureTrackProviderSession).count())
        finally:
            db.close()

    def test_disconnect_rejects_unknown_fields(self):
        response = self.client.post(
            "/api/v1/puretrack/disconnect",
            json={"unexpected": True},
            headers=self.headers(),
        )

        self.assertEqual(422, response.status_code)
        self.assertEqual(main_module.ErrorCode.VALIDATION_ERROR, response.json()["code"])

    def headers(
        self,
        token: str | None = None,
        package_name: str = main_module.XCPRO_RELEASE_PACKAGE_NAME,
        include_auth: bool = True,
    ):
        headers = {"X-XCPro-Package-Name": package_name}
        if include_auth:
            headers["Authorization"] = f"Bearer {token or self.primary_bearer}"
        return headers

    def add_static_bearer(self, token: str, subject: str):
        main_module.STATIC_BEARER_TOKENS[token] = main_module.ResolvedBearerIdentity(
            provider="static",
            provider_subject=subject,
            email=f"{subject}@example.com",
            display_name=f"Pilot {subject}",
        )

    def user_id_for_token(self, token: str | None = None) -> str:
        db = self.session_local()
        try:
            current_user = main_module.ensure_current_user_record(
                db,
                self.headers(token=token)["Authorization"],
            )
            return current_user.user.id
        finally:
            db.close()

    def upsert_entitlement_snapshot(
        self,
        token: str | None = None,
        tier: str = "PRO",
    ):
        user_id = self.user_id_for_token(token)
        now = self.clock.utcnow()
        now_ms = self.now_ms()
        db = self.session_local()
        try:
            db.merge(
                main_module.AccountEntitlementSnapshot(
                    user_id=user_id,
                    tier=tier,
                    billing_period="MONTHLY",
                    status="ACTIVE",
                    source="GOOGLE_PLAY",
                    verification_state="VERIFIED",
                    product_id=main_module.PRODUCT_ID_BY_TIER[tier],
                    base_plan_id=main_module.BASE_PLAN_BY_PERIOD["MONTHLY"],
                    expiry_time_ms=now_ms + 86_400_000,
                    auto_renewing=True,
                    will_lose_access_at_ms=None,
                    verified_at_ms=now_ms,
                    fetched_at_ms=now_ms,
                    valid_until_ms=now_ms + 86_400_000,
                    stale_after_ms=None,
                    hard_refresh_after_ms=None,
                    recovery_action="NONE",
                    created_at=now,
                    updated_at=now,
                )
            )
            db.commit()
        finally:
            db.close()

    def upsert_provider_session(
        self,
        token: str,
        user_access: str,
    ):
        user_id = self.user_id_for_token(token)
        now = self.clock.utcnow()
        now_ms = self.now_ms()
        db = self.session_local()
        try:
            db.merge(
                main_module.PureTrackProviderSession(
                    user_id=user_id,
                    provider_session_hash=main_module.hash_token(f"session-{token}"),
                    user_access=user_access,
                    account_label="p***@example.com",
                    verified_at_ms=now_ms,
                    valid_until_ms=now_ms + main_module.PURETRACK_PROVIDER_STATUS_CACHE_MS,
                    error_code=None,
                    retry_after_ms=None,
                    audit_id="pt_test",
                    created_at=now,
                    updated_at=now,
                )
            )
            db.commit()
        finally:
            db.close()

    def assert_no_secret_leakage(self, body: dict, password: str):
        serialized = json.dumps(body, sort_keys=True)
        self.assertNotIn(password, serialized)
        self.assertNotIn("provider-session-secret", serialized)
        self.assertNotIn("free-provider-session-secret", serialized)
        self.assertNotIn("server-side-app-key", serialized)
        self.assertNotIn("pilot@example.com", serialized)

    def now_ms(self) -> int:
        return main_module.to_epoch_ms(self.clock.utcnow())

    @staticmethod
    def status_keys() -> set[str]:
        return {
            "connected",
            "appKeyConfigured",
            "trafficApiAllowed",
            "insertApiConfigured",
            "userAccess",
            "verifiedAtMs",
            "validUntilMs",
            "accountLabel",
            "errorCode",
            "retryAfterMs",
            "auditId",
        }


if __name__ == "__main__":
    unittest.main()
