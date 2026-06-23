import json
import unittest
from datetime import datetime

from fastapi.testclient import TestClient
from pydantic import ValidationError
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


class FakePureTrackInsertClient:
    def __init__(self):
        self.result = main_module.PureTrackInsertProviderResult(
            result=main_module.PURETRACK_INSERT_RESULT_ACCEPTED,
            provider_inserted_point_count=2,
        )
        self.calls = []

    def publish(
        self,
        trackers: list[dict],
        config: main_module.PureTrackRuntimeConfig,
    ) -> main_module.PureTrackInsertProviderResult:
        self.calls.append({
            "trackers": trackers,
            "hasInsertKey": config.insert_key is not None,
        })
        return self.result


class FakePureTrackTrafficClient:
    def __init__(self):
        self.result = main_module.PureTrackTrafficProviderResult(
            rows=[
                (
                    "T1713592586,L-37.78174,G174.88159,A4685,C338,S144.05,"
                    "V-13.31,O56,U12,EZK-MZE,mANZ118M,KY-ZK-MZE"
                )
            ],
            provider_row_count=1,
        )
        self.calls = []

    def fetch(
        self,
        bbox: main_module.PureTrackTrafficBbox,
        filters: main_module.PureTrackTrafficFilters,
        provider_session_secret: str,
        config: main_module.PureTrackRuntimeConfig,
    ) -> main_module.PureTrackTrafficProviderResult:
        self.calls.append({
            "bbox": main_module.pydantic_model_to_dict(bbox),
            "filters": main_module.pydantic_model_to_dict(filters),
            "providerSessionSecret": provider_session_secret,
            "hasAppKey": config.app_key is not None,
            "apiBaseUrl": config.api_base_url,
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
        self.original_puretrack_insert_client = main_module.PURETRACK_INSERT_CLIENT
        self.original_puretrack_traffic_client = main_module.PURETRACK_TRAFFIC_CLIENT
        self.original_puretrack_traffic_evidence_enabled = (
            main_module.PURETRACK_TRAFFIC_EVIDENCE_ENABLED
        )
        self.original_puretrack_traffic_evidence_sink = (
            main_module.PURETRACK_TRAFFIC_EVIDENCE_SINK
        )

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
            provider_session_encryption_secret=b"puretrack-provider-session-test-secret",
        )
        self.provider = FakePureTrackProviderClient()
        main_module.PURETRACK_PROVIDER_CLIENT = self.provider
        self.insert_client = FakePureTrackInsertClient()
        main_module.PURETRACK_INSERT_CLIENT = self.insert_client
        self.traffic_client = FakePureTrackTrafficClient()
        main_module.PURETRACK_TRAFFIC_CLIENT = self.traffic_client
        self.traffic_evidence_events = []
        main_module.PURETRACK_TRAFFIC_EVIDENCE_ENABLED = False
        main_module.PURETRACK_TRAFFIC_EVIDENCE_SINK = (
            self.traffic_evidence_events.append
        )
        main_module._puretrack_traffic_cache.clear()
        main_module._puretrack_traffic_rate_limits.clear()
        self.client = TestClient(main_module.app)

    def tearDown(self):
        self.client.close()
        main_module.SessionLocal = self.original_session_local
        main_module.utcnow = self.original_utcnow
        main_module.PRIVATE_FOLLOW_RUNTIME_CONFIG = self.original_private_follow_runtime_config
        main_module.STATIC_BEARER_TOKENS = self.original_static_bearer_tokens
        main_module.PURETRACK_RUNTIME_CONFIG = self.original_puretrack_runtime_config
        main_module.PURETRACK_PROVIDER_CLIENT = self.original_puretrack_provider_client
        main_module.PURETRACK_INSERT_CLIENT = self.original_puretrack_insert_client
        main_module.PURETRACK_TRAFFIC_CLIENT = self.original_puretrack_traffic_client
        main_module.PURETRACK_TRAFFIC_EVIDENCE_ENABLED = (
            self.original_puretrack_traffic_evidence_enabled
        )
        main_module.PURETRACK_TRAFFIC_EVIDENCE_SINK = (
            self.original_puretrack_traffic_evidence_sink
        )
        main_module._puretrack_traffic_cache.clear()
        main_module._puretrack_traffic_rate_limits.clear()
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
        self.assertEqual(
            "puretrack_insert_key_unconfigured",
            main_module.ErrorCode.PURETRACK_INSERT_KEY_UNCONFIGURED,
        )
        self.assertEqual(
            "puretrack_insert_rejected",
            main_module.ErrorCode.PURETRACK_INSERT_REJECTED,
        )
        self.assertEqual(
            "puretrack_provider_not_connected",
            main_module.ErrorCode.PURETRACK_PROVIDER_NOT_CONNECTED,
        )
        self.assertEqual(
            "puretrack_provider_access_denied",
            main_module.ErrorCode.PURETRACK_PROVIDER_ACCESS_DENIED,
        )
        self.assertEqual(
            "puretrack_provider_session_unavailable",
            main_module.ErrorCode.PURETRACK_PROVIDER_SESSION_UNAVAILABLE,
        )
        self.assertEqual(
            "puretrack_traffic_rejected",
            main_module.ErrorCode.PURETRACK_TRAFFIC_REJECTED,
        )
        self.assertEqual(
            "feature_access_denied",
            main_module.ErrorCode.FEATURE_ACCESS_DENIED,
        )
        methods_by_path = {
            getattr(route, "path", None): getattr(route, "methods", set())
            for route in main_module.app.routes
        }
        self.assertIn("GET", methods_by_path["/api/v1/puretrack/status"])
        self.assertIn("POST", methods_by_path["/api/v1/puretrack/connect"])
        self.assertIn("POST", methods_by_path["/api/v1/puretrack/disconnect"])
        self.assertIn("POST", methods_by_path["/api/v1/puretrack/insert"])
        self.assertIn("POST", methods_by_path["/api/v1/puretrack/traffic"])

    def test_env_config_loader_uses_contract_defaults_and_values(self):
        default_config = main_module.load_puretrack_runtime_config({})
        self.assertIsNone(default_config.app_key)
        self.assertIsNone(default_config.provider_session_encryption_secret)
        self.assertEqual(main_module.PURETRACK_DEFAULT_API_BASE_URL, default_config.api_base_url)
        self.assertEqual(10.0, default_config.timeout_seconds)

        configured = main_module.load_puretrack_runtime_config({
            "XCPRO_PURETRACK_APP_KEY": " app-key-value ",
            "XCPRO_PURETRACK_INSERT_KEY": " insert-key-value ",
            "XCPRO_PURETRACK_API_BASE_URL": "https://puretrack.test/",
            "XCPRO_PURETRACK_TIMEOUT_SECONDS": "3.5",
            "XCPRO_PURETRACK_PROVIDER_SESSION_ENCRYPTION_SECRET": " provider-session-secret ",
        })
        self.assertEqual("app-key-value", configured.app_key)
        self.assertEqual("insert-key-value", configured.insert_key)
        self.assertEqual(b"provider-session-secret", configured.provider_session_encryption_secret)
        self.assertEqual("https://puretrack.test", configured.api_base_url)
        self.assertEqual(3.5, configured.timeout_seconds)
        self.assertIs(False, main_module.load_puretrack_traffic_evidence_enabled({}))
        self.assertIs(
            True,
            main_module.load_puretrack_traffic_evidence_enabled({
                "XCPRO_PURETRACK_TRAFFIC_EVIDENCE_ENABLED": "true",
            }),
        )

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

    def test_status_reports_insert_api_configured_from_server_key(self):
        self.enable_insert_key()

        response = self.client.get(
            "/api/v1/puretrack/status",
            headers=self.headers(),
        )

        self.assertEqual(200, response.status_code)
        self.assertIs(True, response.json()["insertApiConfigured"])

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
        self.assertIsNone(status["validUntilMs"])
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
            self.assertIsNotNone(row.provider_session_ciphertext)
            self.assertNotEqual("provider-session-secret", row.provider_session_ciphertext)
            self.assertEqual(
                "provider-session-secret",
                main_module.decrypt_puretrack_provider_session_secret(
                    row.provider_session_ciphertext
                ),
            )
            self.assertEqual("p***@example.com", row.account_label)
            self.assertIsNone(row.valid_until_ms)
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
                    provider_session_encryption_secret=b"puretrack-provider-session-test-secret",
                )
                self.upsert_entitlement_snapshot(token=token, tier=tier)
                self.upsert_provider_session(token=token, user_access=user_access)

                response = self.client.get(
                    "/api/v1/puretrack/status",
                    headers=self.headers(token=token),
                )

                self.assertEqual(200, response.status_code)
                self.assertIs(expected_allowed, response.json()["trafficApiAllowed"])

    def test_hash_only_provider_session_fails_closed_for_traffic_allowance(self):
        self.upsert_entitlement_snapshot(tier="PRO")
        self.upsert_provider_session(
            token=self.primary_bearer,
            user_access=main_module.PURETRACK_PROVIDER_ACCESS_PREMIUM,
            include_ciphertext=False,
        )

        response = self.client.get(
            "/api/v1/puretrack/status",
            headers=self.headers(),
        )

        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertIs(False, body["connected"])
        self.assertIs(False, body["trafficApiAllowed"])
        self.assertIsNone(body["accountLabel"])
        self.assertEqual(
            main_module.ErrorCode.PURETRACK_PROVIDER_SESSION_UNAVAILABLE,
            body["errorCode"],
        )

    def test_missing_puretrack_provider_session_encryption_secret_fails_closed(self):
        self.upsert_entitlement_snapshot(tier="PRO")
        main_module.PURETRACK_RUNTIME_CONFIG = main_module.PureTrackRuntimeConfig(
            app_key="server-side-app-key",
            api_base_url="https://puretrack.example",
            timeout_seconds=2.0,
            provider_session_encryption_secret=None,
        )
        password = "provider-password"

        response = self.client.post(
            "/api/v1/puretrack/connect",
            json={"email": "pilot@example.com", "password": password},
            headers=self.headers(),
        )

        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertEqual(main_module.PURETRACK_CONNECT_RESULT_ERROR, body["result"])
        self.assertIs(False, body["status"]["connected"])
        self.assertIs(False, body["status"]["trafficApiAllowed"])
        self.assertEqual(
            main_module.ErrorCode.PURETRACK_PROVIDER_SESSION_UNAVAILABLE,
            body["status"]["errorCode"],
        )
        self.assert_no_secret_leakage(body, password=password)
        db = self.session_local()
        try:
            self.assertEqual(0, db.query(main_module.PureTrackProviderSession).count())
        finally:
            db.close()

    def test_corrupt_puretrack_provider_session_ciphertext_fails_closed(self):
        self.upsert_entitlement_snapshot(tier="PRO")
        self.upsert_provider_session(
            token=self.primary_bearer,
            user_access=main_module.PURETRACK_PROVIDER_ACCESS_PREMIUM,
            provider_session_ciphertext="not-a-valid-fernet-token",
        )

        response = self.client.get(
            "/api/v1/puretrack/status",
            headers=self.headers(),
        )

        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertIs(False, body["connected"])
        self.assertIs(False, body["trafficApiAllowed"])
        self.assertIsNone(body["accountLabel"])
        self.assertEqual(
            main_module.ErrorCode.PURETRACK_PROVIDER_SESSION_UNAVAILABLE,
            body["errorCode"],
        )

    def test_historical_expired_provider_session_ciphertext_remains_connected(self):
        self.upsert_entitlement_snapshot(tier="PRO")
        self.upsert_provider_session(
            token=self.primary_bearer,
            user_access=main_module.PURETRACK_PROVIDER_ACCESS_PREMIUM,
            valid_until_ms=self.now_ms() - 1,
        )

        response = self.client.get(
            "/api/v1/puretrack/status",
            headers=self.headers(),
        )

        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertIs(True, body["connected"])
        self.assertIs(True, body["trafficApiAllowed"])
        self.assertEqual("p***@example.com", body["accountLabel"])
        self.assertIsNone(body["validUntilMs"])

    def test_provider_session_material_is_encrypted_and_redacted(self):
        self.upsert_entitlement_snapshot(tier="PRO")
        provider_session_secret = "provider-session-secret"

        response = self.client.post(
            "/api/v1/puretrack/connect",
            json={"email": "pilot@example.com", "password": "provider-password"},
            headers=self.headers(),
        )

        self.assertEqual(200, response.status_code)
        body = response.json()
        db = self.session_local()
        try:
            row = db.query(main_module.PureTrackProviderSession).one()
            self.assertEqual(main_module.hash_token(provider_session_secret), row.provider_session_hash)
            self.assertIsNotNone(row.provider_session_ciphertext)
            self.assertNotEqual(provider_session_secret, row.provider_session_ciphertext)
            self.assertNotEqual(row.provider_session_hash, row.provider_session_ciphertext)
            self.assertEqual(
                provider_session_secret,
                main_module.decrypt_puretrack_provider_session_secret(
                    row.provider_session_ciphertext
                ),
            )
            serialized_body = json.dumps(body, sort_keys=True)
            self.assertNotIn(provider_session_secret, serialized_body)
            self.assertNotIn(row.provider_session_ciphertext, serialized_body)
        finally:
            db.close()

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
        self.assertEqual(1, len(self.provider.calls))

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
        status_response = self.client.get(
            "/api/v1/puretrack/status",
            headers=self.headers(),
        )
        status = status_response.json()
        self.assertEqual(200, status_response.status_code)
        self.assertIs(False, status["connected"])
        self.assertIs(False, status["trafficApiAllowed"])
        self.assertEqual(main_module.PURETRACK_PROVIDER_ACCESS_UNKNOWN, status["userAccess"])
        self.assertIsNone(status["accountLabel"])
        self.assertIsNone(status["errorCode"])
        self.assertEqual(1, len(self.provider.calls))
        self.assertEqual([], self.traffic_client.calls)
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

    def test_insert_requires_valid_xcpro_bearer_and_package(self):
        self.enable_insert_key()
        self.upsert_entitlement_snapshot(tier="PRO")

        missing_auth = self.client.post(
            "/api/v1/puretrack/insert",
            json=self.insert_payload(),
            headers=self.headers(include_auth=False),
        )
        invalid_auth = self.client.post(
            "/api/v1/puretrack/insert",
            json=self.insert_payload(),
            headers=self.headers(token="unknown-bearer"),
        )
        invalid_package = self.client.post(
            "/api/v1/puretrack/insert",
            json=self.insert_payload(),
            headers=self.headers(package_name="com.example.other"),
        )

        self.assertEqual(401, missing_auth.status_code)
        self.assertEqual(main_module.ErrorCode.UNAUTHENTICATED, missing_auth.json()["code"])
        self.assertEqual(401, invalid_auth.status_code)
        self.assertEqual(main_module.ErrorCode.UNAUTHENTICATED, invalid_auth.json()["code"])
        self.assertEqual(400, invalid_package.status_code)
        self.assertEqual(main_module.ErrorCode.INVALID_PACKAGE, invalid_package.json()["code"])
        self.assertEqual([], self.insert_client.calls)

    def test_insert_requires_server_insert_key_before_provider_call(self):
        self.upsert_entitlement_snapshot(tier="PRO")

        response = self.client.post(
            "/api/v1/puretrack/insert",
            json=self.insert_payload(),
            headers=self.headers(),
        )

        self.assertEqual(503, response.status_code)
        self.assertEqual(
            main_module.ErrorCode.PURETRACK_INSERT_KEY_UNCONFIGURED,
            response.json()["code"],
        )
        self.assertEqual([], self.insert_client.calls)

    def test_insert_requires_verified_xcpro_pro_entitlement(self):
        self.enable_insert_key()
        self.upsert_entitlement_snapshot(tier="XC")

        response = self.client.post(
            "/api/v1/puretrack/insert",
            json=self.insert_payload(),
            headers=self.headers(),
        )

        self.assertEqual(403, response.status_code)
        self.assertEqual(main_module.ErrorCode.FEATURE_ACCESS_DENIED, response.json()["code"])
        self.assertEqual([], self.insert_client.calls)

    def test_insert_rejects_invalid_request_shapes_before_provider_call(self):
        self.enable_insert_key()
        self.upsert_entitlement_snapshot(tier="PRO")
        valid_tracker = self.insert_payload()["trackers"][0]
        valid_point = valid_tracker["points"][0]
        cases = (
            {},
            {"clientBatchId": "batch-1", "trackers": []},
            {"clientBatchId": "batch-1", "trackers": [dict(valid_tracker, deviceID="   ")]},
            {
                "clientBatchId": "batch-1",
                "trackers": [
                    dict(valid_tracker, points=[dict(valid_point, lat=91.0)])
                ],
            },
            {
                "clientBatchId": "batch-1",
                "trackers": [
                    dict(valid_tracker, points=[dict(valid_point, course=361.0)])
                ],
            },
            dict(self.insert_payload(), key="android-must-not-send-key"),
        )
        for payload in cases:
            with self.subTest(payload=payload):
                response = self.client.post(
                    "/api/v1/puretrack/insert",
                    json=payload,
                    headers=self.headers(),
                )

                self.assertEqual(422, response.status_code)
                self.assertEqual(
                    main_module.ErrorCode.VALIDATION_ERROR,
                    response.json()["code"],
                )
        self.assertEqual([], self.insert_client.calls)

    def test_insert_success_accepts_all_client_point_ids_and_redacts(self):
        self.enable_insert_key()
        self.upsert_entitlement_snapshot(tier="PRO")

        response = self.client.post(
            "/api/v1/puretrack/insert",
            json=self.insert_payload(),
            headers=self.headers(),
        )

        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertEqual(main_module.PURETRACK_INSERT_RESULT_ACCEPTED, body["result"])
        self.assertEqual(["point-1", "point-2"], body["acceptedClientPointIds"])
        self.assertEqual(2, body["serverReceivedPointCount"])
        self.assertEqual(2, body["providerInsertedPointCount"])
        self.assertIsNone(body["retryAfterMs"])
        self.assertTrue(body["auditId"].startswith("pt_"))
        self.assertEqual(1, len(self.insert_client.calls))
        call = self.insert_client.calls[0]
        self.assertIs(True, call["hasInsertKey"])
        self.assertNotIn("key", call["trackers"][0])
        self.assertEqual("d7ry390", call["trackers"][0]["deviceID"])
        self.assertEqual(1, call["trackers"][0]["type"])
        self.assertEqual("ZK-ABC", call["trackers"][0]["rego"])
        self.assertEqual("XCPro", call["trackers"][0]["label"])
        self.assertEqual([
            {
                "ts": 1713563621,
                "lat": -41.2334745,
                "lng": 174.348365,
                "alt": 345.1,
                "speed": 25.0,
                "vspeed": 5.3,
                "course": 270.0,
            },
            {
                "ts": 1713563624,
                "lat": -41.1634343,
                "lng": 174.36545,
            },
        ], call["trackers"][0]["points"])
        serialized = json.dumps(body, sort_keys=True)
        self.assertNotIn("server-side-insert-key", serialized)
        self.assertNotIn(self.primary_bearer, serialized)
        self.assertNotIn("pilot@example.com", serialized)

    def test_insert_partial_retry_keeps_all_client_points_queued(self):
        self.enable_insert_key()
        self.upsert_entitlement_snapshot(tier="PRO")
        self.insert_client.result = main_module.PureTrackInsertProviderResult(
            result=main_module.PURETRACK_INSERT_RESULT_PARTIAL_RETRY,
            provider_inserted_point_count=1,
        )

        response = self.client.post(
            "/api/v1/puretrack/insert",
            json=self.insert_payload(),
            headers=self.headers(),
        )

        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertEqual(main_module.PURETRACK_INSERT_RESULT_PARTIAL_RETRY, body["result"])
        self.assertEqual([], body["acceptedClientPointIds"])
        self.assertEqual(2, body["serverReceivedPointCount"])
        self.assertEqual(1, body["providerInsertedPointCount"])

    def test_insert_retryable_failure_returns_retry_after_without_secrets(self):
        self.enable_insert_key()
        self.upsert_entitlement_snapshot(tier="PRO")
        self.insert_client.result = main_module.PureTrackInsertProviderResult(
            result=main_module.PURETRACK_INSERT_RESULT_RETRYABLE_FAILURE,
            retry_after_ms=7000,
            error_code=main_module.ErrorCode.PURETRACK_RATE_LIMITED,
        )

        response = self.client.post(
            "/api/v1/puretrack/insert",
            json=self.insert_payload(),
            headers=self.headers(),
        )

        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertEqual(main_module.PURETRACK_INSERT_RESULT_RETRYABLE_FAILURE, body["result"])
        self.assertEqual([], body["acceptedClientPointIds"])
        self.assertEqual(7000, body["retryAfterMs"])
        serialized = json.dumps(body, sort_keys=True)
        self.assertNotIn("server-side-insert-key", serialized)
        self.assertNotIn(self.primary_bearer, serialized)
        self.assertNotIn("raw provider", serialized)

    def test_traffic_request_models_reject_unknown_fields(self):
        payload = self.traffic_payload()
        cases = (
            dict(payload, unexpected=True),
            dict(payload, bbox=dict(payload["bbox"], unexpected=True)),
            dict(payload, filters=dict(payload["filters"], s=["Y-ZK-MZE"])),
            dict(payload, filters=dict(payload["filters"], i=1)),
            dict(payload, providerUrl="https://puretrack.io/api/traffic"),
        )

        for invalid_payload in cases:
            with self.subTest(payload=invalid_payload):
                with self.assertRaises(ValidationError):
                    self.parse_model(
                        main_module.PureTrackTrafficRequest,
                        invalid_payload,
                    )

    def test_traffic_bbox_validation_rejects_invalid_and_too_large_bounds(self):
        valid_request = self.parse_model(
            main_module.PureTrackTrafficRequest,
            self.traffic_payload(),
        )
        bbox, filters = main_module.validate_puretrack_traffic_request(valid_request)

        self.assertEqual(-37.49503, bbox.north)
        self.assertEqual(main_module.PURETRACK_TRAFFIC_DEFAULT_CATEGORY, filters.category)
        valid_bbox = self.model_dump(valid_request.bbox)
        invalid_bboxes = (
            dict(valid_bbox, north=-38.0, south=-37.0),
            dict(valid_bbox, east=174.0, west=175.0),
            dict(valid_bbox, north=91.0),
            {"north": 1.0, "east": 4.0, "south": 0.9, "west": 0.0},
            {"north": 5.0, "east": 1.0, "south": 1.0, "west": 0.0},
        )
        for invalid_bbox in invalid_bboxes:
            with self.subTest(bbox=invalid_bbox):
                request = self.parse_model(
                    main_module.PureTrackTrafficRequest,
                    dict(self.traffic_payload(), bbox=invalid_bbox),
                )
                with self.assertRaises(main_module.ApiHTTPException) as raised:
                    main_module.validate_puretrack_traffic_request(request)
                self.assertEqual(main_module.ErrorCode.VALIDATION_ERROR, raised.exception.code)

        original_diagonal = main_module.PURETRACK_TRAFFIC_MAX_BBOX_DIAGONAL_METERS
        try:
            main_module.PURETRACK_TRAFFIC_MAX_BBOX_DIAGONAL_METERS = 1000.0
            request = self.parse_model(
                main_module.PureTrackTrafficRequest,
                dict(self.traffic_payload(), bbox={
                    "north": 0.02,
                    "east": 0.02,
                    "south": 0.0,
                    "west": 0.0,
                }),
            )
            with self.assertRaises(main_module.ApiHTTPException) as raised:
                main_module.validate_puretrack_traffic_request(request)
            self.assertEqual(main_module.ErrorCode.VALIDATION_ERROR, raised.exception.code)
        finally:
            main_module.PURETRACK_TRAFFIC_MAX_BBOX_DIAGONAL_METERS = original_diagonal

    def test_traffic_filter_validation_rejects_invalid_values_and_provider_key_filters(self):
        valid_filters = main_module.validate_puretrack_traffic_filters(
            self.parse_model(
                main_module.PureTrackTrafficFilters,
                {
                    "category": "air",
                    "objectTypeIds": [1, 2, 6, 7],
                    "sourceTypeIds": [0, 7, 12, 16],
                    "maxAgeSeconds": 300,
                },
            )
        )
        self.assertEqual("air", valid_filters.category)
        self.assertEqual([1, 2, 6, 7], valid_filters.objectTypeIds)

        invalid_filter_payloads = (
            {"category": "space"},
            {"objectTypeIds": [1, 1]},
            {"objectTypeIds": [-1]},
            {"sourceTypeIds": [1000]},
            {"maxAgeSeconds": 29},
            {"maxAgeSeconds": 901},
        )
        for payload in invalid_filter_payloads:
            with self.subTest(payload=payload):
                filters = self.parse_model(main_module.PureTrackTrafficFilters, payload)
                with self.assertRaises(main_module.ApiHTTPException) as raised:
                    main_module.validate_puretrack_traffic_filters(filters)
                self.assertEqual(main_module.ErrorCode.VALIDATION_ERROR, raised.exception.code)

        for payload in ({"objectTypeIds": [True]}, {"s": ["Y-ZK-MZE"]}, {"isolate": True}):
            with self.subTest(payload=payload):
                with self.assertRaises(ValidationError):
                    self.parse_model(main_module.PureTrackTrafficFilters, payload)

    def test_traffic_parser_maps_required_optional_fields_and_units(self):
        row = (
            "T1713592586,L-37.78174,G174.88159,A4685,P1006.7,C338,S144.05,"
            "V-13.31,O56,DC828EA,U12,EZK-MZE,g43,mANZ118M,t4739.4,KY-ZK-MZE"
        )
        fields = main_module.parse_puretrack_compact_traffic_row(row)
        self.assertEqual("1713592586", fields["T"])
        target, redacted_count = main_module.map_puretrack_compact_row_to_traffic_target(row)

        self.assertIsNotNone(target)
        self.assertTrue(target["targetId"].startswith("pt_"))
        self.assertNotIn("Y-ZK-MZE", target["targetId"])
        self.assertEqual(1713592586000, target["lastSeenAtMs"])
        self.assertEqual(-37.78174, target["latitudeDeg"])
        self.assertEqual(174.88159, target["longitudeDeg"])
        self.assertEqual(4685.0, target["altitudeGpsMeters"])
        self.assertEqual(4739.4, target["altitudePressureMeters"])
        self.assertEqual(338.0, target["courseDeg"])
        self.assertEqual(144.05, target["groundSpeedMps"])
        self.assertEqual(-13.31, target["verticalSpeedMps"])
        self.assertEqual(56, target["objectTypeId"])
        self.assertEqual("air", target["objectCategory"])
        self.assertEqual(12, target["sourceTypeId"])
        self.assertEqual("ADSBHub", target["sourceLabel"])
        self.assertEqual("ZK-MZE", target["displayLabel"])
        self.assertEqual("ZK-MZE", target["registration"])
        self.assertEqual("ANZ118M", target["callsign"])
        self.assertEqual(43.0, target["groundElevationMeters"])
        self.assertGreaterEqual(redacted_count, 2)

        minimal_target, _ = main_module.map_puretrack_compact_row_to_traffic_target(
            "T1713592586,L-37.7,G174.8,K0-ABC123"
        )
        self.assertEqual(1713592586000, minimal_target["lastSeenAtMs"])
        self.assertNotIn("altitudeGpsMeters", minimal_target)
        self.assertNotIn("displayLabel", minimal_target)

    def test_traffic_malformed_rows_drop_and_filters_apply(self):
        valid_row = (
            "T1713592586,L-37.78174,G174.88159,C338,S144.05,V-13.31,"
            "O56,U12,EZK-MZE,mANZ118M,KY-ZK-MZE"
        )
        malformed_rows = [
            valid_row,
            "T1713592586,L-37.78174,G174.88159,O56,U12",
            "T1713592586,Lnot-a-lat,G174.88159,O56,U12,KY-ZK-MZE",
            "T1713592586,L-37.78174,G174.88159,C361,O56,U12,KY-ZK-MZE",
            "",
        ]

        targets, dropped_count, redacted_count = main_module.map_puretrack_compact_rows_to_traffic_targets(
            malformed_rows,
            main_module.PureTrackTrafficFilters(category="air", sourceTypeIds=[12]),
        )

        self.assertEqual(1, len(targets))
        self.assertEqual(4, dropped_count)
        self.assertGreater(redacted_count, 0)

        targets, dropped_count, _ = main_module.map_puretrack_compact_rows_to_traffic_targets(
            [valid_row],
            main_module.PureTrackTrafficFilters(category="air", sourceTypeIds=[7]),
        )
        self.assertEqual([], targets)
        self.assertEqual(1, dropped_count)

    def test_traffic_privacy_redaction_omits_sensitive_provider_fields(self):
        row = (
            "T1713592586,L-37.78174,G174.88159,A4685,C338,S144.05,V-13.31,"
            "O56,U12,EZK-MZE,mANZ118M,MKing Air,cC828EA,KY-ZK-MZE,"
            "DC828EA,Jtarget-123,RReceiver One,NRaw Pilot,uusername,p+6421123456,"
            "jraw-target-key,kraw-inreach,lraw-spot,Fffvl-secret,^ognhex"
        )
        target, redacted_count = main_module.map_puretrack_compact_row_to_traffic_target(row)
        response = main_module.PureTrackTrafficResponse(
            result=main_module.PURETRACK_TRAFFIC_RESULT_OK,
            targets=[target],
            bbox=self.parse_model(main_module.PureTrackTrafficBbox, self.traffic_payload()["bbox"]),
            filtersApplied=main_module.validate_puretrack_traffic_filters(None),
            serverFetchedAtMs=self.now_ms(),
            freshUntilMs=self.now_ms() + 5000,
            providerRowCount=1,
            droppedRowCount=0,
            redactedFieldCount=redacted_count,
            cacheStatus=main_module.PURETRACK_TRAFFIC_CACHE_MISS,
            retryAfterMs=None,
            auditId="pt_test_audit",
        )
        serialized = json.dumps(self.model_dump(response), sort_keys=True)

        self.assertGreaterEqual(redacted_count, 10)
        for forbidden in (
            row,
            "Receiver One",
            "Raw Pilot",
            "username",
            "+6421123456",
            "target-123",
            "raw-target-key",
            "raw-inreach",
            "raw-spot",
            "ffvl-secret",
            "ognhex",
            "puretrack.io",
            "server-side-app-key",
            self.primary_bearer,
        ):
            self.assertNotIn(forbidden, serialized)
        self.assertIn("ZK-MZE", serialized)
        self.assertIn("ANZ118M", serialized)

        stealth_target, _ = main_module.map_puretrack_compact_row_to_traffic_target(
            "T1713592586,L-37.78174,G174.88159,O56,U12,EZK-MZE,mANZ118M,H1,KY-ZK-MZE"
        )
        self.assertTrue(stealth_target["stealth"])
        self.assertNotIn("displayLabel", stealth_target)
        self.assertNotIn("registration", stealth_target)
        self.assertNotIn("callsign", stealth_target)

    def test_traffic_response_models_reject_raw_rows_and_provider_secrets(self):
        target = {
            "targetId": "pt_077111111111111111111111",
            "lastSeenAtMs": 1713592586000,
            "latitudeDeg": -37.78174,
            "longitudeDeg": 174.88159,
        }
        main_module.PureTrackTrafficTarget(**target)
        with self.assertRaises(ValidationError):
            main_module.PureTrackTrafficTarget(
                **dict(target, rawCompactRow="T1713592586,L-37.7,G174.8,KY-ZK-MZE")
            )
        with self.assertRaises(ValidationError):
            main_module.PureTrackTrafficTarget(
                **dict(target, providerUrl="https://puretrack.io/api/traffic")
            )
        with self.assertRaises(ValidationError):
            main_module.PureTrackTrafficTarget(
                **dict(target, displayLabel="https://puretrack.io/api/traffic")
            )
        with self.assertRaises(ValidationError):
            main_module.PureTrackTrafficTarget(
                **dict(target, targetId="Y-ZK-MZE")
            )
        with self.assertRaises(ValidationError):
            main_module.PureTrackTrafficTarget(
                **dict(target, targetId="pt_opaque")
            )

        response = main_module.PureTrackTrafficResponse(
            result=main_module.PURETRACK_TRAFFIC_RESULT_OK,
            targets=[main_module.PureTrackTrafficTarget(**target)],
            bbox=self.parse_model(main_module.PureTrackTrafficBbox, self.traffic_payload()["bbox"]),
            filtersApplied=main_module.validate_puretrack_traffic_filters(None),
            serverFetchedAtMs=self.now_ms(),
            freshUntilMs=self.now_ms() + 5000,
            providerRowCount=1,
            droppedRowCount=0,
            redactedFieldCount=1,
            cacheStatus=main_module.PURETRACK_TRAFFIC_CACHE_MISS,
            retryAfterMs=None,
            auditId="pt_test_audit",
        )
        serialized = json.dumps(self.model_dump(response), sort_keys=True)
        self.assertNotIn("rawCompactRow", serialized)
        self.assertNotIn("providerUrl", serialized)
        self.assertNotIn("https://puretrack.io/api/traffic", serialized)

        with self.assertRaises(ValidationError):
            main_module.PureTrackTrafficResponse(
                result=main_module.PURETRACK_TRAFFIC_RESULT_OK,
                targets=[main_module.PureTrackTrafficTarget(**target)],
                bbox=self.parse_model(main_module.PureTrackTrafficBbox, self.traffic_payload()["bbox"]),
                filtersApplied=main_module.validate_puretrack_traffic_filters(None),
                serverFetchedAtMs=self.now_ms(),
                freshUntilMs=self.now_ms() + 5000,
                providerRowCount=1,
                droppedRowCount=0,
                redactedFieldCount=1,
                cacheStatus=main_module.PURETRACK_TRAFFIC_CACHE_MISS,
                retryAfterMs=None,
                auditId="pt_server-side-app-key",
            )

    def test_traffic_route_requires_valid_bearer_and_package(self):
        missing_auth = self.client.post(
            "/api/v1/puretrack/traffic",
            json=self.traffic_payload(),
            headers=self.headers(include_auth=False),
        )
        invalid_package = self.client.post(
            "/api/v1/puretrack/traffic",
            json=self.traffic_payload(),
            headers=self.headers(package_name="com.example.other"),
        )

        self.assertEqual(401, missing_auth.status_code)
        self.assertEqual(main_module.ErrorCode.UNAUTHENTICATED, missing_auth.json()["code"])
        self.assertEqual(400, invalid_package.status_code)
        self.assertEqual(main_module.ErrorCode.INVALID_PACKAGE, invalid_package.json()["code"])
        self.assertEqual([], self.traffic_client.calls)

    def test_traffic_route_fetches_normalizes_caches_and_redacts(self):
        self.upsert_entitlement_snapshot(tier="PRO")
        self.upsert_provider_session(
            token=None,
            user_access=main_module.PURETRACK_PROVIDER_ACCESS_PREMIUM,
        )
        raw_row = (
            "T1713592586,L-37.78174,G174.88159,A4685,C338,S144.05,V-13.31,"
            "O1,U12,EZK-MZE,mANZ118M,MKing Air,cC828EA,KY-ZK-MZE,"
            "DC828EA,Jtarget-123,RReceiver One,NRaw Pilot,uusername,p+6421123456"
        )
        self.traffic_client.result = main_module.PureTrackTrafficProviderResult(
            rows=[
                raw_row,
                "T1713592586,Lnot-a-lat,G174.88159,O56,U12,KY-ZK-BAD",
            ],
            provider_row_count=2,
        )

        response = self.client.post(
            "/api/v1/puretrack/traffic",
            json=self.traffic_payload(),
            headers=self.headers(),
        )

        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertEqual(main_module.PURETRACK_TRAFFIC_RESULT_OK, body["result"])
        self.assertEqual(main_module.PURETRACK_TRAFFIC_CACHE_MISS, body["cacheStatus"])
        self.assertEqual(2, body["providerRowCount"])
        self.assertEqual(1, body["droppedRowCount"])
        self.assertEqual(1, len(body["targets"]))
        target = body["targets"][0]
        self.assertTrue(target["targetId"].startswith("pt_"))
        self.assertEqual(-37.78174, target["latitudeDeg"])
        self.assertEqual(174.88159, target["longitudeDeg"])
        self.assertEqual("ZK-MZE", target["displayLabel"])
        self.assertEqual("ADSBHub", target["sourceLabel"])
        self.assertEqual("air", body["filtersApplied"]["category"])
        self.assertEqual([1, 2, 6, 7], body["filtersApplied"]["objectTypeIds"])
        self.assertEqual(1, len(self.traffic_client.calls))
        self.assertEqual("session-None", self.traffic_client.calls[0]["providerSessionSecret"])
        self.assertIs(True, self.traffic_client.calls[0]["hasAppKey"])
        self.assertEqual([], self.insert_client.calls)

        serialized = json.dumps(body, sort_keys=True)
        for forbidden in (
            raw_row,
            "server-side-app-key",
            "session-None",
            self.primary_bearer,
            "pilot@example.com",
            "+6421123456",
            "Raw Pilot",
            "username",
            "Receiver One",
            "target-123",
            "https://puretrack.io/api/traffic",
            "providerUrl",
            "rawCompactRow",
        ):
            self.assertNotIn(forbidden, serialized)

        cached_response = self.client.post(
            "/api/v1/puretrack/traffic",
            json=self.traffic_payload(),
            headers=self.headers(),
        )

        self.assertEqual(200, cached_response.status_code)
        cached_body = cached_response.json()
        self.assertEqual(main_module.PURETRACK_TRAFFIC_CACHE_HIT, cached_body["cacheStatus"])
        self.assertEqual(body["serverFetchedAtMs"], cached_body["serverFetchedAtMs"])
        self.assertEqual(body["freshUntilMs"], cached_body["freshUntilMs"])
        self.assertEqual(1, len(self.traffic_client.calls))

    def test_traffic_cadence_evidence_is_disabled_by_default(self):
        self.upsert_entitlement_snapshot(tier="PRO")
        self.upsert_provider_session(
            token=None,
            user_access=main_module.PURETRACK_PROVIDER_ACCESS_PREMIUM,
        )

        response = self.client.post(
            "/api/v1/puretrack/traffic",
            json=self.traffic_payload(),
            headers=self.headers(),
        )

        self.assertEqual(200, response.status_code)
        self.assertEqual([], self.traffic_evidence_events)

    def test_traffic_cadence_evidence_emits_sanitized_success_and_cache_events(self):
        main_module.PURETRACK_TRAFFIC_EVIDENCE_ENABLED = True
        self.upsert_entitlement_snapshot(tier="PRO")
        self.upsert_provider_session(
            token=None,
            user_access=main_module.PURETRACK_PROVIDER_ACCESS_PREMIUM,
        )
        user_id = self.user_id_for_token()

        first_response = self.client.post(
            "/api/v1/puretrack/traffic",
            json=self.traffic_payload(),
            headers=self.headers(),
        )
        second_response = self.client.post(
            "/api/v1/puretrack/traffic",
            json=self.traffic_payload(),
            headers=self.headers(),
        )

        self.assertEqual(200, first_response.status_code)
        self.assertEqual(200, second_response.status_code)
        self.assertEqual(2, len(self.traffic_evidence_events))
        first_event, second_event = self.traffic_evidence_events
        self.assertEqual(main_module.PURETRACK_TRAFFIC_EVIDENCE_EVENT_NAME, first_event["event"])
        self.assertEqual("/api/v1/puretrack/traffic", first_event["route"])
        self.assertEqual("POST", first_event["method"])
        self.assertEqual(200, first_event["statusCode"])
        self.assertEqual(first_response.json()["result"], first_event["outcome"])
        self.assertEqual(main_module.PURETRACK_TRAFFIC_CACHE_MISS, first_event["cacheStatus"])
        self.assertEqual(main_module.XCPRO_RELEASE_PACKAGE_NAME, first_event["packageName"])
        self.assertEqual(
            main_module.puretrack_traffic_cadence_user_hash(user_id),
            first_event["userHash"],
        )
        self.assertIn("serverReceivedAtMs", first_event)
        self.assertIn("serverCompletedAtMs", first_event)
        self.assertEqual(main_module.PURETRACK_TRAFFIC_CACHE_HIT, second_event["cacheStatus"])
        self.assertEqual(1, len(self.traffic_client.calls))

        serialized = json.dumps(self.traffic_evidence_events, sort_keys=True)
        for forbidden in (
            "server-side-app-key",
            "session-None",
            self.primary_bearer,
            "pilot@example.com",
            user_id,
            "map-refresh-20260619-0001",
            "-37.49503",
            "176.54678",
            "ZK-MZE",
            "ANZ118M",
            "KY-ZK-MZE",
            "providerSessionSecret",
            "https://puretrack.example",
        ):
            self.assertNotIn(forbidden, serialized)

    def test_traffic_cadence_evidence_emits_sanitized_error_without_provider_call(self):
        main_module.PURETRACK_TRAFFIC_EVIDENCE_ENABLED = True
        self.upsert_entitlement_snapshot(tier="PRO")

        response = self.client.post(
            "/api/v1/puretrack/traffic",
            json=self.traffic_payload(),
            headers=self.headers(),
        )

        self.assertEqual(409, response.status_code)
        self.assertEqual(
            main_module.ErrorCode.PURETRACK_PROVIDER_NOT_CONNECTED,
            response.json()["code"],
        )
        self.assertEqual([], self.traffic_client.calls)
        self.assertEqual(1, len(self.traffic_evidence_events))
        event = self.traffic_evidence_events[0]
        self.assertEqual(409, event["statusCode"])
        self.assertEqual(main_module.ErrorCode.PURETRACK_PROVIDER_NOT_CONNECTED, event["outcome"])
        self.assertNotIn("cacheStatus", event)
        self.assertEqual(main_module.XCPRO_RELEASE_PACKAGE_NAME, event["packageName"])

        serialized = json.dumps(event, sort_keys=True)
        for forbidden in (
            "server-side-app-key",
            self.primary_bearer,
            "pilot@example.com",
            "map-refresh-20260619-0001",
            "-37.49503",
            "176.54678",
            "providerSessionSecret",
        ):
            self.assertNotIn(forbidden, serialized)

    def test_traffic_route_denies_missing_preconditions_before_provider_call(self):
        original_config = main_module.PURETRACK_RUNTIME_CONFIG
        cases = [
            (
                "missing-app-key",
                "traffic-missing-app-key",
                None,
                main_module.PURETRACK_PROVIDER_ACCESS_PREMIUM,
                True,
                "PRO",
                main_module.ErrorCode.PURETRACK_APP_KEY_UNCONFIGURED,
                503,
            ),
            (
                "disconnected-provider",
                "traffic-disconnected-provider",
                "server-side-app-key",
                None,
                True,
                "PRO",
                main_module.ErrorCode.PURETRACK_PROVIDER_NOT_CONNECTED,
                409,
            ),
            (
                "non-premium-provider",
                "traffic-non-premium-provider",
                "server-side-app-key",
                main_module.PURETRACK_PROVIDER_ACCESS_FREE,
                True,
                "PRO",
                main_module.ErrorCode.PURETRACK_PROVIDER_ACCESS_DENIED,
                403,
            ),
            (
                "hash-only-session",
                "traffic-hash-only-session",
                "server-side-app-key",
                main_module.PURETRACK_PROVIDER_ACCESS_PREMIUM,
                False,
                "PRO",
                main_module.ErrorCode.PURETRACK_PROVIDER_SESSION_UNAVAILABLE,
                503,
            ),
            (
                "missing-xcpro-pro",
                "traffic-missing-xcpro-pro",
                "server-side-app-key",
                main_module.PURETRACK_PROVIDER_ACCESS_PREMIUM,
                True,
                None,
                main_module.ErrorCode.FEATURE_ACCESS_DENIED,
                403,
            ),
        ]
        try:
            for (
                label,
                token,
                app_key,
                provider_access,
                include_ciphertext,
                tier,
                expected_code,
                expected_status,
            ) in cases:
                with self.subTest(label=label):
                    self.add_static_bearer(token, token)
                    main_module.PURETRACK_RUNTIME_CONFIG = main_module.PureTrackRuntimeConfig(
                        app_key=app_key,
                        api_base_url="https://puretrack.example",
                        timeout_seconds=2.0,
                        provider_session_encryption_secret=(
                            b"puretrack-provider-session-test-secret"
                        ),
                    )
                    if tier is not None:
                        self.upsert_entitlement_snapshot(token=token, tier=tier)
                    if provider_access is not None:
                        self.upsert_provider_session(
                            token=token,
                            user_access=provider_access,
                            include_ciphertext=include_ciphertext,
                        )

                    response = self.client.post(
                        "/api/v1/puretrack/traffic",
                        json=self.traffic_payload(),
                        headers=self.headers(token=token),
                    )

                    self.assertEqual(expected_status, response.status_code)
                    self.assertEqual(expected_code, response.json()["code"])
                    self.assertEqual([], self.traffic_client.calls)
        finally:
            main_module.PURETRACK_RUNTIME_CONFIG = original_config

    def test_traffic_route_rejects_invalid_bbox_before_provider_call(self):
        self.upsert_entitlement_snapshot(tier="PRO")
        self.upsert_provider_session(
            token=None,
            user_access=main_module.PURETRACK_PROVIDER_ACCESS_PREMIUM,
        )
        response = self.client.post(
            "/api/v1/puretrack/traffic",
            json=dict(
                self.traffic_payload(),
                bbox={
                    "north": 1.0,
                    "east": 4.0,
                    "south": 0.9,
                    "west": 0.0,
                },
            ),
            headers=self.headers(),
        )

        self.assertEqual(422, response.status_code)
        self.assertEqual(main_module.ErrorCode.VALIDATION_ERROR, response.json()["code"])
        self.assertEqual([], self.traffic_client.calls)

    def test_traffic_provider_error_mapping_and_retry_after(self):
        cases = [
            (401, 409, main_module.ErrorCode.PURETRACK_PROVIDER_NOT_CONNECTED, None),
            (403, 403, main_module.ErrorCode.PURETRACK_PROVIDER_ACCESS_DENIED, None),
            (422, 502, main_module.ErrorCode.PURETRACK_TRAFFIC_REJECTED, None),
            (429, 429, main_module.ErrorCode.PURETRACK_RATE_LIMITED, 7000),
            (500, 503, main_module.ErrorCode.PURETRACK_PROVIDER_UNAVAILABLE, None),
        ]
        for provider_status, expected_status, expected_code, retry_after_ms in cases:
            with self.subTest(provider_status=provider_status):
                token = f"traffic-provider-error-{provider_status}"
                self.add_static_bearer(token, token)
                self.upsert_entitlement_snapshot(token=token, tier="PRO")
                self.upsert_provider_session(
                    token=token,
                    user_access=main_module.PURETRACK_PROVIDER_ACCESS_PREMIUM,
                )
                self.traffic_client.result = (
                    main_module.puretrack_traffic_provider_failure_for_http_status(
                        provider_status,
                        retry_after_ms,
                    )
                )

                response = self.client.post(
                    "/api/v1/puretrack/traffic",
                    json=self.traffic_payload(),
                    headers=self.headers(token=token),
                )

                self.assertEqual(expected_status, response.status_code)
                self.assertEqual(expected_code, response.json()["code"])
                if retry_after_ms is not None:
                    self.assertEqual("7", response.headers.get("Retry-After"))

    def test_traffic_provider_401_marks_reconnect_required_and_stops_retry(self):
        token = "traffic-provider-revoked"
        self.add_static_bearer(token, token)
        self.upsert_entitlement_snapshot(token=token, tier="PRO")
        self.upsert_provider_session(
            token=token,
            user_access=main_module.PURETRACK_PROVIDER_ACCESS_PREMIUM,
        )
        self.traffic_client.result = (
            main_module.puretrack_traffic_provider_failure_for_http_status(401)
        )

        response = self.client.post(
            "/api/v1/puretrack/traffic",
            json=self.traffic_payload(),
            headers=self.headers(token=token),
        )

        self.assertEqual(409, response.status_code)
        self.assertEqual(
            main_module.ErrorCode.PURETRACK_PROVIDER_NOT_CONNECTED,
            response.json()["code"],
        )
        self.assertEqual(1, len(self.traffic_client.calls))

        status_response = self.client.get(
            "/api/v1/puretrack/status",
            headers=self.headers(token=token),
        )
        status = status_response.json()
        self.assertEqual(200, status_response.status_code)
        self.assertIs(False, status["connected"])
        self.assertIs(False, status["trafficApiAllowed"])
        self.assertEqual(main_module.PURETRACK_PROVIDER_ACCESS_NONE, status["userAccess"])
        self.assertIsNone(status["accountLabel"])
        self.assertIsNone(status["validUntilMs"])
        self.assertEqual(
            main_module.ErrorCode.PURETRACK_PROVIDER_NOT_CONNECTED,
            status["errorCode"],
        )

        user_id = self.user_id_for_token(token)
        db = self.session_local()
        try:
            row = (
                db.query(main_module.PureTrackProviderSession)
                .filter(main_module.PureTrackProviderSession.user_id == user_id)
                .one()
            )
            self.assertIsNone(row.provider_session_hash)
            self.assertIsNone(row.provider_session_ciphertext)
            self.assertEqual(main_module.PURETRACK_PROVIDER_ACCESS_NONE, row.user_access)
            self.assertIsNone(row.account_label)
            self.assertEqual(
                main_module.ErrorCode.PURETRACK_PROVIDER_NOT_CONNECTED,
                row.error_code,
            )
        finally:
            db.close()

        retry_response = self.client.post(
            "/api/v1/puretrack/traffic",
            json=self.traffic_payload(),
            headers=self.headers(token=token),
        )

        self.assertEqual(409, retry_response.status_code)
        self.assertEqual(
            main_module.ErrorCode.PURETRACK_PROVIDER_NOT_CONNECTED,
            retry_response.json()["code"],
        )
        self.assertEqual(1, len(self.traffic_client.calls))

    def test_traffic_provider_403_marks_provider_access_denied_and_stops_retry(self):
        token = "traffic-provider-access-denied"
        self.add_static_bearer(token, token)
        self.upsert_entitlement_snapshot(token=token, tier="PRO")
        self.upsert_provider_session(
            token=token,
            user_access=main_module.PURETRACK_PROVIDER_ACCESS_PREMIUM,
        )
        self.traffic_client.result = (
            main_module.puretrack_traffic_provider_failure_for_http_status(403)
        )

        response = self.client.post(
            "/api/v1/puretrack/traffic",
            json=self.traffic_payload(),
            headers=self.headers(token=token),
        )

        self.assertEqual(403, response.status_code)
        self.assertEqual(
            main_module.ErrorCode.PURETRACK_PROVIDER_ACCESS_DENIED,
            response.json()["code"],
        )
        self.assertEqual(1, len(self.traffic_client.calls))

        status_response = self.client.get(
            "/api/v1/puretrack/status",
            headers=self.headers(token=token),
        )
        status = status_response.json()
        self.assertEqual(200, status_response.status_code)
        self.assertIs(True, status["connected"])
        self.assertIs(False, status["trafficApiAllowed"])
        self.assertEqual(main_module.PURETRACK_PROVIDER_ACCESS_FREE, status["userAccess"])
        self.assertEqual("p***@example.com", status["accountLabel"])
        self.assertIsNone(status["validUntilMs"])
        self.assertEqual(
            main_module.ErrorCode.PURETRACK_PROVIDER_ACCESS_DENIED,
            status["errorCode"],
        )

        user_id = self.user_id_for_token(token)
        db = self.session_local()
        try:
            row = (
                db.query(main_module.PureTrackProviderSession)
                .filter(main_module.PureTrackProviderSession.user_id == user_id)
                .one()
            )
            self.assertIsNotNone(row.provider_session_hash)
            self.assertIsNotNone(row.provider_session_ciphertext)
            self.assertEqual(main_module.PURETRACK_PROVIDER_ACCESS_FREE, row.user_access)
            self.assertEqual("p***@example.com", row.account_label)
            self.assertEqual(
                main_module.ErrorCode.PURETRACK_PROVIDER_ACCESS_DENIED,
                row.error_code,
            )
        finally:
            db.close()

        retry_response = self.client.post(
            "/api/v1/puretrack/traffic",
            json=self.traffic_payload(),
            headers=self.headers(token=token),
        )

        self.assertEqual(403, retry_response.status_code)
        self.assertEqual(
            main_module.ErrorCode.PURETRACK_PROVIDER_ACCESS_DENIED,
            retry_response.json()["code"],
        )
        self.assertEqual(1, len(self.traffic_client.calls))

    def test_traffic_provider_error_envelope_mapping_matches_status_mapping(self):
        cases = {
            401: main_module.ErrorCode.PURETRACK_PROVIDER_SESSION_UNAVAILABLE,
            403: main_module.ErrorCode.PURETRACK_PROVIDER_ACCESS_DENIED,
            422: main_module.ErrorCode.PURETRACK_TRAFFIC_REJECTED,
            429: main_module.ErrorCode.PURETRACK_RATE_LIMITED,
            503: main_module.ErrorCode.PURETRACK_PROVIDER_UNAVAILABLE,
        }
        for status_code, expected_code in cases.items():
            with self.subTest(status_code=status_code):
                result = main_module.puretrack_traffic_provider_failure_for_http_status(
                    status_code,
                    5000,
                )
                self.assertEqual(expected_code, result.error_code)

    def test_traffic_route_rate_limits_with_retry_after(self):
        main_module.PURETRACK_TRAFFIC_EVIDENCE_ENABLED = True
        self.upsert_entitlement_snapshot(tier="PRO")
        self.upsert_provider_session(
            token=None,
            user_access=main_module.PURETRACK_PROVIDER_ACCESS_PREMIUM,
        )

        for index in range(main_module.PURETRACK_TRAFFIC_RATE_LIMIT_BURST):
            response = self.client.post(
                "/api/v1/puretrack/traffic",
                json=self.traffic_payload(),
                headers=self.headers(),
            )
            self.assertEqual(200, response.status_code, index)

        limited_response = self.client.post(
            "/api/v1/puretrack/traffic",
            json=self.traffic_payload(),
            headers=self.headers(),
        )

        self.assertEqual(429, limited_response.status_code)
        self.assertEqual(
            main_module.ErrorCode.PURETRACK_RATE_LIMITED,
            limited_response.json()["code"],
        )
        self.assertEqual("5", limited_response.headers.get("Retry-After"))
        self.assertEqual(
            main_module.PURETRACK_TRAFFIC_RATE_LIMIT_BURST + 1,
            len(self.traffic_evidence_events),
        )
        limited_event = self.traffic_evidence_events[-1]
        self.assertEqual(429, limited_event["statusCode"])
        self.assertEqual(main_module.ErrorCode.PURETRACK_RATE_LIMITED, limited_event["outcome"])
        self.assertEqual(5000, limited_event["retryAfterMs"])

        serialized = json.dumps(limited_event, sort_keys=True)
        for forbidden in (
            "server-side-app-key",
            self.primary_bearer,
            "pilot@example.com",
            "map-refresh-20260619-0001",
            "-37.49503",
            "176.54678",
            "providerSessionSecret",
        ):
            self.assertNotIn(forbidden, serialized)

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
        token: str | None,
        user_access: str,
        include_ciphertext: bool = True,
        provider_session_ciphertext: str | None = None,
        valid_until_ms: int | None = None,
    ):
        user_id = self.user_id_for_token(token)
        now = self.clock.utcnow()
        now_ms = self.now_ms()
        provider_session_secret = f"session-{token}"
        if provider_session_ciphertext is None and include_ciphertext:
            provider_session_ciphertext = (
                main_module.encrypt_puretrack_provider_session_secret(
                    provider_session_secret
                )
            )
        db = self.session_local()
        try:
            db.merge(
                main_module.PureTrackProviderSession(
                    user_id=user_id,
                    provider_session_hash=main_module.hash_token(provider_session_secret),
                    provider_session_ciphertext=provider_session_ciphertext,
                    user_access=user_access,
                    account_label="p***@example.com",
                    verified_at_ms=now_ms,
                    valid_until_ms=valid_until_ms,
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

    def enable_insert_key(self):
        main_module.PURETRACK_RUNTIME_CONFIG = main_module.PureTrackRuntimeConfig(
            app_key="server-side-app-key",
            api_base_url="https://puretrack.example",
            timeout_seconds=2.0,
            insert_key="server-side-insert-key",
            provider_session_encryption_secret=b"puretrack-provider-session-test-secret",
        )

    @staticmethod
    def insert_payload() -> dict:
        return {
            "clientBatchId": "batch-20260618-0001",
            "trackers": [
                {
                    "deviceID": "d7ry390",
                    "type": 1,
                    "rego": "ZK-ABC",
                    "label": "XCPro",
                    "points": [
                        {
                            "clientPointId": "point-1",
                            "ts": 1713563621,
                            "lat": -41.2334745,
                            "lng": 174.348365,
                            "alt": 345.1,
                            "speed": 25.0,
                            "course": 270.0,
                            "vspeed": 5.3,
                        },
                        {
                            "clientPointId": "point-2",
                            "ts": 1713563624,
                            "lat": -41.1634343,
                            "lng": 174.36545,
                        },
                    ],
                }
            ],
        }

    @staticmethod
    def traffic_payload() -> dict:
        return {
            "bbox": {
                "north": -37.49503,
                "east": 176.54678,
                "south": -38.06575,
                "west": 174.82046,
            },
            "filters": {
                "category": "air",
                "objectTypeIds": [1, 2, 6, 7],
                "sourceTypeIds": [0, 7, 12, 16],
                "maxAgeSeconds": 300,
            },
            "clientRequestId": "map-refresh-20260619-0001",
        }

    @staticmethod
    def parse_model(model_cls, payload):
        if hasattr(model_cls, "model_validate"):
            return model_cls.model_validate(payload)
        return model_cls.parse_obj(payload)

    @staticmethod
    def model_dump(model):
        if hasattr(model, "model_dump"):
            return model.model_dump()
        return model.dict()

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
