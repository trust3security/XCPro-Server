import base64
import json
import unittest
from datetime import datetime, timedelta

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

    def advance(self, **kwargs):
        self.current += timedelta(**kwargs)


class FakeGooglePlayVerifier:
    def __init__(self):
        self.results_by_token = {}
        self.calls = []

    def set_result(self, purchase_token: str, result):
        self.results_by_token[purchase_token] = result

    def verify_subscription(
        self,
        package_name: str,
        product_id: str,
        purchase_token: str
    ):
        self.calls.append(
            {
                "packageName": package_name,
                "productId": product_id,
                "purchaseToken": purchase_token,
            }
        )
        result = self.results_by_token.get(purchase_token)
        if isinstance(result, Exception):
            raise result
        if result is None:
            raise main_module.GooglePlayVerificationTemporarilyUnavailable()
        return result


class FakeGooglePlayAcknowledger:
    def __init__(self, session_local=None):
        self.calls = []
        self.session_local = session_local
        self.snapshot_statuses_seen = []
        self.results_by_token = {}

    def set_result(self, purchase_token: str, result):
        self.results_by_token[purchase_token] = result

    def acknowledge_subscription(
        self,
        package_name: str,
        product_id: str,
        purchase_token: str
    ) -> bool:
        if self.session_local is not None:
            db = self.session_local()
            try:
                snapshot = db.query(main_module.AccountEntitlementSnapshot).one_or_none()
                self.snapshot_statuses_seen.append(snapshot.status if snapshot else None)
            finally:
                db.close()
        self.calls.append(
            {
                "packageName": package_name,
                "productId": product_id,
                "purchaseToken": purchase_token,
            }
        )
        result = self.results_by_token.get(purchase_token, True)
        if isinstance(result, Exception):
            raise result
        return result


class FakeAndroidPublisherApiClient:
    def __init__(self):
        self.subscription_responses_by_token = {}
        self.ack_results_by_token = {}
        self.verify_calls = []
        self.ack_calls = []

    def set_subscription_response(self, purchase_token: str, response):
        self.subscription_responses_by_token[purchase_token] = response

    def set_ack_result(self, purchase_token: str, result):
        self.ack_results_by_token[purchase_token] = result

    def get_subscription_v2(self, package_name: str, purchase_token: str):
        self.verify_calls.append(
            {
                "packageName": package_name,
                "purchaseToken": purchase_token,
            }
        )
        response = self.subscription_responses_by_token[purchase_token]
        if isinstance(response, Exception):
            raise response
        return response

    def acknowledge_subscription(
        self,
        package_name: str,
        product_id: str,
        purchase_token: str,
    ) -> bool:
        self.ack_calls.append(
            {
                "packageName": package_name,
                "productId": product_id,
                "purchaseToken": purchase_token,
            }
        )
        result = self.ack_results_by_token.get(purchase_token, True)
        if isinstance(result, Exception):
            raise result
        return result


class FakeRtdnOidcAuthenticator:
    def __init__(self):
        self.calls = []

    def verify_authorization(self, authorization: str | None):
        self.calls.append(authorization)
        if authorization != "Bearer valid-rtdn-oidc-token":
            raise main_module.ApiHTTPException(
                status_code=401,
                code=main_module.ErrorCode.INVALID_RTDN_AUTH,
                detail="invalid RTDN OIDC bearer token",
            )
        return {
            "aud": "https://api.xcpro.com.au/api/v1/subscriptions/googleplay/rtdn",
            "email": "xcpro-rtdn-push@xcpro-868cd.iam.gserviceaccount.com",
            "email_verified": True,
        }


class GooglePlaySubscriptionAuthorityTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        self.session_local = sessionmaker(bind=self.engine)
        main_module.Base.metadata.create_all(bind=self.engine)

        self.original_session_local = main_module.SessionLocal
        self.original_redis_client = main_module.redis_client
        self.original_utcnow = main_module.utcnow
        self.original_private_follow_runtime_config = main_module.PRIVATE_FOLLOW_RUNTIME_CONFIG
        self.original_static_bearer_tokens = main_module.STATIC_BEARER_TOKENS
        self.original_verifier = main_module.GOOGLE_PLAY_PURCHASE_VERIFIER
        self.original_acknowledger = main_module.GOOGLE_PLAY_PURCHASE_ACKNOWLEDGER
        self.original_google_play_runtime_config = main_module.GOOGLE_PLAY_RUNTIME_CONFIG
        self.original_rtdn_token = main_module.GOOGLE_PLAY_RTDN_INGEST_TOKEN
        self.original_rtdn_allow_test_header_auth = (
            main_module.GOOGLE_PLAY_RTDN_ALLOW_TEST_HEADER_AUTH
        )
        self.original_rtdn_oidc_authenticator = (
            main_module.GOOGLE_PLAY_RTDN_OIDC_AUTHENTICATOR
        )

        main_module.SessionLocal = self.session_local
        main_module.redis_client = FakeRedis()
        self.clock = MutableClock(datetime(2026, 5, 15, 10, 0, 0))
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
        self.primary_bearer_token = "test-bearer-token-1"
        self.secondary_bearer_token = "test-bearer-token-2"
        main_module.STATIC_BEARER_TOKENS = {
            self.primary_bearer_token: main_module.ResolvedBearerIdentity(
                provider="static",
                provider_subject="pilot-1",
                email="pilot1@example.com",
                display_name="Pilot One",
            ),
            self.secondary_bearer_token: main_module.ResolvedBearerIdentity(
                provider="static",
                provider_subject="pilot-2",
                email="pilot2@example.com",
                display_name="Pilot Two",
            ),
        }
        self.verifier = FakeGooglePlayVerifier()
        self.acknowledger = FakeGooglePlayAcknowledger(self.session_local)
        main_module.GOOGLE_PLAY_PURCHASE_VERIFIER = self.verifier
        main_module.GOOGLE_PLAY_PURCHASE_ACKNOWLEDGER = self.acknowledger
        main_module.GOOGLE_PLAY_RUNTIME_CONFIG = main_module.GooglePlayRuntimeConfig(
            package_name=main_module.XCPRO_RELEASE_PACKAGE_NAME,
            service_account_json_path="/missing/google-play-service-account.json",
            rtdn_oidc_audience="https://api.xcpro.com.au/api/v1/subscriptions/googleplay/rtdn",
            rtdn_expected_service_account_email=(
                "xcpro-rtdn-push@xcpro-868cd.iam.gserviceaccount.com"
            ),
            rtdn_test_ingest_token="rtdn-test-token",
            allow_test_rtdn_header_auth=True,
        )
        main_module.GOOGLE_PLAY_RTDN_INGEST_TOKEN = "rtdn-test-token"
        main_module.GOOGLE_PLAY_RTDN_ALLOW_TEST_HEADER_AUTH = True
        main_module.GOOGLE_PLAY_RTDN_OIDC_AUTHENTICATOR = FakeRtdnOidcAuthenticator()

        self.client = TestClient(main_module.app)

    def tearDown(self):
        self.client.close()
        main_module.SessionLocal = self.original_session_local
        main_module.redis_client = self.original_redis_client
        main_module.utcnow = self.original_utcnow
        main_module.PRIVATE_FOLLOW_RUNTIME_CONFIG = self.original_private_follow_runtime_config
        main_module.STATIC_BEARER_TOKENS = self.original_static_bearer_tokens
        main_module.GOOGLE_PLAY_PURCHASE_VERIFIER = self.original_verifier
        main_module.GOOGLE_PLAY_PURCHASE_ACKNOWLEDGER = self.original_acknowledger
        main_module.GOOGLE_PLAY_RUNTIME_CONFIG = self.original_google_play_runtime_config
        main_module.GOOGLE_PLAY_RTDN_INGEST_TOKEN = self.original_rtdn_token
        main_module.GOOGLE_PLAY_RTDN_ALLOW_TEST_HEADER_AUTH = (
            self.original_rtdn_allow_test_header_auth
        )
        main_module.GOOGLE_PLAY_RTDN_OIDC_AUTHENTICATOR = (
            self.original_rtdn_oidc_authenticator
        )
        main_module.Base.metadata.drop_all(bind=self.engine)
        self.engine.dispose()

    def test_sync_requires_auth_and_valid_package(self):
        missing_auth = self.client.post(
            "/api/v1/subscriptions/googleplay/sync",
            json=self.sync_payload(),
            headers=self.package_headers(include_auth=False),
        )
        invalid_package = self.client.post(
            "/api/v1/subscriptions/googleplay/sync",
            json=self.sync_payload(package_name="com.example.other"),
            headers=self.package_headers(package_name="com.example.other"),
        )
        mismatch_package = self.client.post(
            "/api/v1/subscriptions/googleplay/sync",
            json=self.sync_payload(package_name=main_module.XCPRO_DEBUG_PACKAGE_NAME),
            headers=self.package_headers(),
        )

        self.assertEqual(401, missing_auth.status_code)
        self.assertEqual(main_module.ErrorCode.UNAUTHENTICATED, missing_auth.json()["code"])
        self.assertEqual(400, invalid_package.status_code)
        self.assertEqual(main_module.ErrorCode.INVALID_PACKAGE, invalid_package.json()["code"])
        self.assertEqual(400, mismatch_package.status_code)
        self.assertEqual(main_module.ErrorCode.INVALID_PACKAGE, mismatch_package.json()["code"])

    def test_invalid_product_and_base_plan_reject_without_paid_grant(self):
        invalid_product = self.client.post(
            "/api/v1/subscriptions/googleplay/sync",
            json=self.sync_payload(product_id="other_product", purchase_token="bad-product-token"),
            headers=self.package_headers(),
        )
        invalid_base_plan = self.client.post(
            "/api/v1/subscriptions/googleplay/sync",
            json=self.sync_payload(base_plan_id="weekly", purchase_token="bad-plan-token"),
            headers=self.package_headers(),
        )

        self.assertEqual(200, invalid_product.status_code)
        self.assertEqual("INVALID_PRODUCT", invalid_product.json()["result"])
        self.assertEqual("FREE", invalid_product.json()["entitlement"]["tier"])
        self.assertEqual(200, invalid_base_plan.status_code)
        self.assertEqual("INVALID_BASE_PLAN", invalid_base_plan.json()["result"])
        self.assertEqual("FREE", invalid_base_plan.json()["entitlement"]["tier"])
        self.assertEqual(0, self.count_rows(main_module.AccountEntitlementSnapshot))
        self.assertEqual(0, self.count_rows(main_module.BillingGooglePurchase))

    def test_google_play_runtime_config_loads_operational_env(self):
        config = main_module.load_google_play_runtime_config(
            {
                "XCPRO_GOOGLE_PLAY_PACKAGE_NAME": "com.trust3.xcpro",
                "XCPRO_GOOGLE_PLAY_SERVICE_ACCOUNT_JSON_PATH": (
                    "/run/secrets/google-play-service-account.json"
                ),
                "XCPRO_GOOGLE_PLAY_RTDN_OIDC_AUDIENCE": (
                    "https://api.xcpro.com.au/api/v1/subscriptions/googleplay/rtdn"
                ),
                "XCPRO_GOOGLE_PLAY_RTDN_EXPECTED_SERVICE_ACCOUNT_EMAIL": (
                    "xcpro-rtdn-push@xcpro-868cd.iam.gserviceaccount.com"
                ),
                "XCPRO_RTDN_INGEST_TOKEN": "local-test-only",
                "XCPRO_ALLOW_TEST_RTDN_HEADER_AUTH": "1",
            }
        )

        self.assertEqual("com.trust3.xcpro", config.package_name)
        self.assertEqual(
            "/run/secrets/google-play-service-account.json",
            config.service_account_json_path,
        )
        self.assertEqual(
            "https://api.xcpro.com.au/api/v1/subscriptions/googleplay/rtdn",
            config.rtdn_oidc_audience,
        )
        self.assertEqual(
            "xcpro-rtdn-push@xcpro-868cd.iam.gserviceaccount.com",
            config.rtdn_expected_service_account_email,
        )
        self.assertEqual("local-test-only", config.rtdn_test_ingest_token)
        self.assertTrue(config.allow_test_rtdn_header_auth)

    def test_missing_google_play_config_returns_unavailable_without_paid_mutation(self):
        main_module.GOOGLE_PLAY_PURCHASE_VERIFIER = main_module.GooglePlayPurchaseVerifier(
            config=main_module.GooglePlayRuntimeConfig(
                package_name=None,
                service_account_json_path=None,
                rtdn_oidc_audience=None,
                rtdn_expected_service_account_email=None,
                rtdn_test_ingest_token=None,
                allow_test_rtdn_header_auth=False,
            )
        )

        response = self.client.post(
            "/api/v1/subscriptions/googleplay/sync",
            json=self.sync_payload(purchase_token="missing-config-token"),
            headers=self.package_headers(),
        )

        self.assertEqual(200, response.status_code)
        self.assertEqual("VERIFICATION_TEMPORARILY_UNAVAILABLE", response.json()["result"])
        self.assertEqual("FREE", response.json()["entitlement"]["tier"])
        self.assertEqual(0, self.count_rows(main_module.AccountEntitlementSnapshot))
        self.assertEqual(0, self.count_rows(main_module.BillingGooglePurchase))

    def test_real_verifier_maps_subscription_v2_states(self):
        api_client = FakeAndroidPublisherApiClient()
        verifier = main_module.GooglePlayPurchaseVerifier(api_client=api_client)
        cases = {
            "SUBSCRIPTION_STATE_PENDING": "PENDING",
            "SUBSCRIPTION_STATE_ACTIVE": "ACTIVE",
            "SUBSCRIPTION_STATE_IN_GRACE_PERIOD": "GRACE_PERIOD",
            "SUBSCRIPTION_STATE_CANCELED": "CANCELED_BUT_ACTIVE",
            "SUBSCRIPTION_STATE_ON_HOLD": "ON_HOLD",
            "SUBSCRIPTION_STATE_PAUSED": "PAUSED",
            "SUBSCRIPTION_STATE_EXPIRED": "EXPIRED",
            "SUBSCRIPTION_STATE_PENDING_PURCHASE_CANCELED": "REVOKED",
        }
        for google_state, expected_status in cases.items():
            with self.subTest(google_state=google_state):
                purchase_token = f"publisher-{expected_status.lower()}-token"
                api_client.set_subscription_response(
                    purchase_token,
                    self.publisher_subscription_response(
                        subscription_state=google_state,
                        acknowledgement_state="ACKNOWLEDGEMENT_STATE_PENDING",
                    ),
                )

                verified = verifier.verify_subscription(
                    package_name=main_module.XCPRO_RELEASE_PACKAGE_NAME,
                    product_id="xcpro_pro",
                    purchase_token=purchase_token,
                )

                self.assertEqual(expected_status, verified.subscription_status)
                self.assertEqual("xcpro_pro", verified.product_id)
                self.assertEqual("monthly", verified.base_plan_id)
                self.assertTrue(verified.acknowledgement_required)

        for cancellation_context, expected_status in (
            ({"developerInitiatedCancellation": {}}, "REVOKED"),
            ({"systemInitiatedCancellation": {}}, "SUSPENDED"),
        ):
            with self.subTest(cancellation_context=cancellation_context):
                purchase_token = f"publisher-cancel-context-{expected_status.lower()}-token"
                api_client.set_subscription_response(
                    purchase_token,
                    self.publisher_subscription_response(
                        subscription_state="SUBSCRIPTION_STATE_EXPIRED",
                        cancellation_context=cancellation_context,
                    ),
                )

                verified = verifier.verify_subscription(
                    package_name=main_module.XCPRO_RELEASE_PACKAGE_NAME,
                    product_id="xcpro_pro",
                    purchase_token=purchase_token,
                )

                self.assertEqual(expected_status, verified.subscription_status)

    def test_real_acknowledger_success_and_transient_failure(self):
        api_client = FakeAndroidPublisherApiClient()
        acknowledger = main_module.GooglePlayPurchaseAcknowledger(api_client=api_client)
        api_client.set_ack_result("ack-success-token", True)
        api_client.set_ack_result(
            "ack-transient-token",
            main_module.GooglePlayVerificationTemporarilyUnavailable(),
        )

        self.assertTrue(
            acknowledger.acknowledge_subscription(
                package_name=main_module.XCPRO_RELEASE_PACKAGE_NAME,
                product_id="xcpro_pro",
                purchase_token="ack-success-token",
            )
        )
        with self.assertRaises(main_module.GooglePlayVerificationTemporarilyUnavailable):
            acknowledger.acknowledge_subscription(
                package_name=main_module.XCPRO_RELEASE_PACKAGE_NAME,
                product_id="xcpro_pro",
                purchase_token="ack-transient-token",
            )

    def test_pending_purchase_grants_no_paid_access(self):
        purchase_token = "pending-purchase-token"
        self.verifier.set_result(
            purchase_token,
            self.verification_result(
                status="PENDING",
                acknowledgement_required=True,
            ),
        )

        response = self.client.post(
            "/api/v1/subscriptions/googleplay/sync",
            json=self.sync_payload(purchase_token=purchase_token),
            headers=self.package_headers(),
        )

        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertEqual("ACCEPTED_PENDING", body["result"])
        self.assertEqual("PENDING", body["entitlement"]["status"])
        self.assertIsNone(body["entitlement"]["validUntilMs"])
        self.assertEqual(0, len(self.acknowledger.calls))
        snapshot = self.single_row(main_module.AccountEntitlementSnapshot)
        self.assertEqual("PENDING", snapshot.status)
        self.assertIsNone(snapshot.valid_until_ms)

    def test_active_grace_and_canceled_active_update_entitlement_snapshot(self):
        for status in ("ACTIVE", "GRACE_PERIOD", "CANCELED_BUT_ACTIVE"):
            with self.subTest(status=status):
                purchase_token = f"{status.lower()}-purchase-token"
                expiry_time_ms = 1777777777000
                self.verifier.set_result(
                    purchase_token,
                    self.verification_result(
                        status=status,
                        product_id="xcpro_soaring",
                        base_plan_id="annual",
                        expiry_time_ms=expiry_time_ms,
                        auto_renewing=status != "CANCELED_BUT_ACTIVE",
                    ),
                )

                response = self.client.post(
                    "/api/v1/subscriptions/googleplay/sync",
                    json=self.sync_payload(
                        product_id="xcpro_soaring",
                        base_plan_id="annual",
                        purchase_token=purchase_token,
                    ),
                    headers=self.package_headers(),
                )

                self.assertEqual(200, response.status_code)
                entitlement = response.json()["entitlement"]
                self.assertEqual("ACCEPTED_VERIFIED", response.json()["result"])
                self.assertEqual("SOARING", entitlement["tier"])
                self.assertEqual("ANNUAL", entitlement["billingPeriod"])
                self.assertEqual(status, entitlement["status"])
                self.assertEqual(expiry_time_ms, entitlement["validUntilMs"])

    def test_revoked_and_expired_remove_paid_access(self):
        for status in ("EXPIRED", "REVOKED"):
            with self.subTest(status=status):
                purchase_token = f"{status.lower()}-purchase-token"
                self.verifier.set_result(
                    purchase_token,
                    self.verification_result(status="ACTIVE"),
                )
                active = self.client.post(
                    "/api/v1/subscriptions/googleplay/sync",
                    json=self.sync_payload(purchase_token=purchase_token),
                    headers=self.package_headers(),
                )
                self.assertEqual(200, active.status_code)
                self.assertEqual("ACTIVE", active.json()["entitlement"]["status"])

                self.verifier.set_result(
                    purchase_token,
                    self.verification_result(status=status, expiry_time_ms=1777000000000),
                )
                denied = self.client.post(
                    "/api/v1/subscriptions/googleplay/sync",
                    json=self.sync_payload(purchase_token=purchase_token),
                    headers=self.package_headers(),
                )

                self.assertEqual(200, denied.status_code)
                self.assertEqual("REVOKED_OR_EXPIRED", denied.json()["result"])
                self.assertEqual(status, denied.json()["entitlement"]["status"])
                self.assertIsNone(denied.json()["entitlement"]["validUntilMs"])

    def test_account_mismatch_token_already_owned_returns_recovery_and_no_paid_grant(self):
        purchase_token = "owned-by-primary-token"
        self.verifier.set_result(purchase_token, self.verification_result(status="ACTIVE"))
        owner_response = self.client.post(
            "/api/v1/subscriptions/googleplay/sync",
            json=self.sync_payload(purchase_token=purchase_token),
            headers=self.package_headers(),
        )
        self.assertEqual(200, owner_response.status_code)
        owner_purchase = self.single_row(main_module.BillingGooglePurchase)

        mismatch = self.client.post(
            "/api/v1/subscriptions/googleplay/sync",
            json=self.sync_payload(purchase_token=purchase_token),
            headers=self.package_headers(token=self.secondary_bearer_token),
        )

        self.assertEqual(200, mismatch.status_code)
        self.assertEqual("TOKEN_ALREADY_OWNED", mismatch.json()["result"])
        self.assertEqual("CHOOSE_CORRECT_ACCOUNT", mismatch.json()["recoveryAction"])
        self.assertEqual("FREE", mismatch.json()["entitlement"]["tier"])
        self.assertEqual(owner_purchase.user_id, self.single_row(main_module.BillingGooglePurchase).user_id)

    def test_linked_purchase_token_same_account_replaces_old_authority(self):
        old_token = "linked-old-token"
        new_token = "linked-new-token"
        self.verifier.set_result(old_token, self.verification_result(status="ACTIVE"))
        old_response = self.client.post(
            "/api/v1/subscriptions/googleplay/sync",
            json=self.sync_payload(purchase_token=old_token),
            headers=self.package_headers(),
        )
        self.assertEqual(200, old_response.status_code)
        self.assertEqual("ACTIVE", old_response.json()["entitlement"]["status"])
        self.verifier.set_result(
            new_token,
            self.verification_result(
                status="ACTIVE",
                product_id="xcpro_soaring",
                base_plan_id="annual",
                linked_purchase_token=old_token,
            ),
        )

        new_response = self.client.post(
            "/api/v1/subscriptions/googleplay/sync",
            json=self.sync_payload(
                product_id="xcpro_soaring",
                base_plan_id="annual",
                purchase_token=new_token,
            ),
            headers=self.package_headers(),
        )

        self.assertEqual(200, new_response.status_code)
        self.assertEqual("ACCEPTED_VERIFIED", new_response.json()["result"])
        self.assertEqual("SOARING", new_response.json()["entitlement"]["tier"])
        db = self.session_local()
        try:
            old_purchase = (
                db.query(main_module.BillingGooglePurchase)
                .filter(
                    main_module.BillingGooglePurchase.purchase_token_hash
                    == main_module.hash_purchase_token(old_token)
                )
                .one()
            )
            new_purchase = (
                db.query(main_module.BillingGooglePurchase)
                .filter(
                    main_module.BillingGooglePurchase.purchase_token_hash
                    == main_module.hash_purchase_token(new_token)
                )
                .one()
            )
            self.assertEqual("SUPERSEDED_BY_LINKED_PURCHASE", old_purchase.google_subscription_state)
            self.assertEqual("REVOKED", old_purchase.xcpro_subscription_status)
            self.assertEqual(main_module.hash_purchase_token(old_token), new_purchase.linked_purchase_token_hash)
            self.assertEqual("ACTIVE", new_purchase.xcpro_subscription_status)
        finally:
            db.close()

    def test_late_rtdn_for_superseded_linked_purchase_does_not_overwrite_active_entitlement(self):
        old_token = "linked-late-rtdn-old-basic-token"
        new_token = "linked-late-rtdn-new-basic-token"
        self.verifier.set_result(
            old_token,
            self.verification_result(
                status="ACTIVE",
                product_id="xcpro_basic",
                base_plan_id="monthly",
            ),
        )
        old_response = self.client.post(
            "/api/v1/subscriptions/googleplay/sync",
            json=self.sync_payload(
                product_id="xcpro_basic",
                base_plan_id="monthly",
                purchase_token=old_token,
            ),
            headers=self.package_headers(),
        )
        self.assertEqual(200, old_response.status_code)
        self.assertEqual("ACTIVE", old_response.json()["entitlement"]["status"])

        self.verifier.set_result(
            new_token,
            self.verification_result(
                status="ACTIVE",
                product_id="xcpro_basic",
                base_plan_id="monthly",
                linked_purchase_token=old_token,
            ),
        )
        new_response = self.client.post(
            "/api/v1/subscriptions/googleplay/sync",
            json=self.sync_payload(
                product_id="xcpro_basic",
                base_plan_id="monthly",
                purchase_token=new_token,
            ),
            headers=self.package_headers(),
        )
        self.assertEqual(200, new_response.status_code)
        self.assertEqual("ACCEPTED_VERIFIED", new_response.json()["result"])
        self.assertEqual("BASIC", new_response.json()["entitlement"]["tier"])
        self.assertEqual("ACTIVE", new_response.json()["entitlement"]["status"])
        verifier_calls_after_replacement = len(self.verifier.calls)

        self.verifier.set_result(
            old_token,
            self.verification_result(
                status="SUSPENDED",
                product_id="xcpro_basic",
                base_plan_id="monthly",
                expiry_time_ms=1777000000000,
                auto_renewing=False,
            ),
        )
        late_old_rtdn = self.client.post(
            "/api/v1/subscriptions/googleplay/rtdn",
            json=self.rtdn_envelope(
                message_id="late-old-basic-linked-token",
                purchase_token=old_token,
                product_id="xcpro_basic",
            ),
            headers=self.rtdn_headers(),
        )

        self.assertEqual(200, late_old_rtdn.status_code)
        self.assertEqual("SUPERSEDED_PURCHASE_IGNORED", late_old_rtdn.json()["result"])
        self.assertEqual(verifier_calls_after_replacement, len(self.verifier.calls))
        entitlement_response = self.client.get(
            "/api/v1/subscriptions/entitlements",
            headers=self.package_headers(),
        )
        self.assertEqual(200, entitlement_response.status_code)
        entitlement = entitlement_response.json()["entitlement"]
        self.assertEqual("BASIC", entitlement["tier"])
        self.assertEqual("ACTIVE", entitlement["status"])
        self.assertEqual("monthly", entitlement["basePlanId"])

        db = self.session_local()
        try:
            old_purchase = (
                db.query(main_module.BillingGooglePurchase)
                .filter(
                    main_module.BillingGooglePurchase.purchase_token_hash
                    == main_module.hash_purchase_token(old_token)
                )
                .one()
            )
            event = self.single_row(main_module.BillingGoogleEvent)
            self.assertEqual(
                "SUPERSEDED_BY_LINKED_PURCHASE",
                old_purchase.google_subscription_state,
            )
            self.assertEqual("SUPERSEDED_PURCHASE_IGNORED", event.processing_result)
        finally:
            db.close()

    def test_linked_purchase_token_different_account_denies_paid_grant(self):
        old_token = "linked-different-account-old-token"
        new_token = "linked-different-account-new-token"
        self.verifier.set_result(old_token, self.verification_result(status="ACTIVE"))
        owner_response = self.client.post(
            "/api/v1/subscriptions/googleplay/sync",
            json=self.sync_payload(purchase_token=old_token),
            headers=self.package_headers(),
        )
        self.assertEqual(200, owner_response.status_code)
        self.verifier.set_result(
            new_token,
            self.verification_result(
                status="ACTIVE",
                linked_purchase_token=old_token,
            ),
        )

        mismatch = self.client.post(
            "/api/v1/subscriptions/googleplay/sync",
            json=self.sync_payload(purchase_token=new_token),
            headers=self.package_headers(token=self.secondary_bearer_token),
        )

        self.assertEqual(200, mismatch.status_code)
        self.assertEqual("ACCOUNT_MISMATCH", mismatch.json()["result"])
        self.assertEqual("CHOOSE_CORRECT_ACCOUNT", mismatch.json()["recoveryAction"])
        self.assertEqual("FREE", mismatch.json()["entitlement"]["tier"])
        self.assertEqual(1, self.count_rows(main_module.BillingGooglePurchase))

    def test_linked_purchase_token_unknown_old_token_stores_hash_and_continues(self):
        new_token = "linked-unknown-old-new-token"
        unknown_old_token = "linked-unknown-old-token"
        self.verifier.set_result(
            new_token,
            self.verification_result(
                status="ACTIVE",
                linked_purchase_token=unknown_old_token,
            ),
        )

        response = self.client.post(
            "/api/v1/subscriptions/googleplay/sync",
            json=self.sync_payload(purchase_token=new_token),
            headers=self.package_headers(),
        )

        self.assertEqual(200, response.status_code)
        self.assertEqual("ACCEPTED_VERIFIED", response.json()["result"])
        purchase = self.single_row(main_module.BillingGooglePurchase)
        self.assertEqual(
            main_module.hash_purchase_token(unknown_old_token),
            purchase.linked_purchase_token_hash,
        )

    def test_transient_verifier_failure_returns_unavailable_and_preserves_canonical_entitlement(self):
        purchase_token = "transient-token"
        self.verifier.set_result(purchase_token, self.verification_result(status="ACTIVE"))
        active = self.client.post(
            "/api/v1/subscriptions/googleplay/sync",
            json=self.sync_payload(purchase_token=purchase_token),
            headers=self.package_headers(),
        )
        self.assertEqual("ACTIVE", active.json()["entitlement"]["status"])

        self.verifier.set_result(
            purchase_token,
            main_module.GooglePlayVerificationTemporarilyUnavailable(),
        )
        unavailable = self.client.post(
            "/api/v1/subscriptions/googleplay/sync",
            json=self.sync_payload(purchase_token=purchase_token),
            headers=self.package_headers(),
        )

        self.assertEqual(200, unavailable.status_code)
        self.assertEqual("VERIFICATION_TEMPORARILY_UNAVAILABLE", unavailable.json()["result"])
        self.assertEqual("RETRY_LATER", unavailable.json()["recoveryAction"])
        self.assertEqual("ACTIVE", unavailable.json()["entitlement"]["status"])
        self.assertEqual("ACTIVE", self.single_row(main_module.AccountEntitlementSnapshot).status)

    def test_acknowledgement_runs_only_after_verified_entitlement_readiness(self):
        active_token = "ack-active-token"
        pending_token = "ack-pending-token"
        self.verifier.set_result(
            active_token,
            self.verification_result(status="ACTIVE", acknowledgement_required=True),
        )
        self.verifier.set_result(
            pending_token,
            self.verification_result(status="PENDING", acknowledgement_required=True),
        )

        active = self.client.post(
            "/api/v1/subscriptions/googleplay/sync",
            json=self.sync_payload(purchase_token=active_token),
            headers=self.package_headers(),
        )
        pending = self.client.post(
            "/api/v1/subscriptions/googleplay/sync",
            json=self.sync_payload(purchase_token=pending_token),
            headers=self.package_headers(),
        )

        self.assertEqual(200, active.status_code)
        self.assertTrue(active.json()["acknowledgementRequired"])
        self.assertTrue(active.json()["acknowledgementCompleted"])
        self.assertEqual(200, pending.status_code)
        self.assertTrue(pending.json()["acknowledgementRequired"])
        self.assertFalse(pending.json()["acknowledgementCompleted"])
        self.assertEqual([active_token], [call["purchaseToken"] for call in self.acknowledger.calls])
        self.assertEqual(["ACTIVE"], self.acknowledger.snapshot_statuses_seen)

    def test_acknowledgement_transient_failure_is_retryable_and_preserves_entitlement(self):
        purchase_token = "ack-transient-process-token"
        self.verifier.set_result(
            purchase_token,
            self.verification_result(status="ACTIVE", acknowledgement_required=True),
        )
        self.acknowledger.set_result(
            purchase_token,
            main_module.GooglePlayVerificationTemporarilyUnavailable(),
        )

        response = self.client.post(
            "/api/v1/subscriptions/googleplay/sync",
            json=self.sync_payload(purchase_token=purchase_token),
            headers=self.package_headers(),
        )

        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertEqual("ACCEPTED_VERIFIED", body["result"])
        self.assertTrue(body["acknowledgementRequired"])
        self.assertFalse(body["acknowledgementCompleted"])
        self.assertEqual(main_module.DENIED_ENTITLEMENT_STALE_AFTER_MS, body["acknowledgementRetryAfterMs"])
        self.assertEqual("ACTIVE", body["entitlement"]["status"])
        purchase = self.single_row(main_module.BillingGooglePurchase)
        self.assertEqual("ACK_RETRYABLE", purchase.acknowledgement_state)
        self.assertEqual("ACTIVE", self.single_row(main_module.AccountEntitlementSnapshot).status)

    def test_rtdn_test_notification_records_evidence_without_purchase_mutation(self):
        purchase_token = "rtdn-test-notification-existing-token"
        self.verifier.set_result(purchase_token, self.verification_result(status="ACTIVE"))
        sync = self.client.post(
            "/api/v1/subscriptions/googleplay/sync",
            json=self.sync_payload(purchase_token=purchase_token),
            headers=self.package_headers(),
        )
        self.assertEqual(200, sync.status_code)
        purchase_before = self.single_row(main_module.BillingGooglePurchase)
        snapshot_before = self.single_row(main_module.AccountEntitlementSnapshot)
        audit_count_before = self.count_rows(main_module.BillingAuditRecord)
        self.clock.advance(minutes=5)

        response = self.client.post(
            "/api/v1/subscriptions/googleplay/rtdn",
            json=self.rtdn_test_notification_envelope(
                message_id="rtdn-test-notification-message-1"
            ),
            headers=self.rtdn_headers(),
        )

        self.assertEqual(200, response.status_code)
        self.assertEqual("TEST_NOTIFICATION", response.json()["result"])
        self.assertFalse(response.json()["deduped"])
        self.assertEqual(1, self.count_rows(main_module.BillingGooglePurchase))
        self.assertEqual(1, self.count_rows(main_module.AccountEntitlementSnapshot))
        self.assertEqual(audit_count_before, self.count_rows(main_module.BillingAuditRecord))
        self.assertEqual(1, len(self.verifier.calls))
        purchase_after = self.single_row(main_module.BillingGooglePurchase)
        snapshot_after = self.single_row(main_module.AccountEntitlementSnapshot)
        self.assertEqual(purchase_before.updated_at, purchase_after.updated_at)
        self.assertEqual(purchase_before.last_verified_at_ms, purchase_after.last_verified_at_ms)
        self.assertEqual(snapshot_before.status, snapshot_after.status)
        self.assertEqual(snapshot_before.updated_at, snapshot_after.updated_at)
        event = self.single_row(main_module.BillingGoogleEvent)
        self.assertEqual("TEST_NOTIFICATION", event.event_type)
        self.assertEqual("TEST_NOTIFICATION", event.processing_result)
        self.assertIsNone(event.product_id)
        self.assertIsNone(event.purchase_token_hash)
        self.assertIsNone(event.audit_id)
        self.assertIsNotNone(event.processed_at)

    def test_duplicate_rtdn_test_notification_is_idempotent(self):
        envelope = self.rtdn_test_notification_envelope(
            message_id="rtdn-test-notification-duplicate"
        )

        first = self.client.post(
            "/api/v1/subscriptions/googleplay/rtdn",
            json=envelope,
            headers=self.rtdn_headers(),
        )
        duplicate = self.client.post(
            "/api/v1/subscriptions/googleplay/rtdn",
            json=envelope,
            headers=self.rtdn_headers(),
        )

        self.assertEqual(200, first.status_code)
        self.assertEqual("TEST_NOTIFICATION", first.json()["result"])
        self.assertFalse(first.json()["deduped"])
        self.assertEqual(200, duplicate.status_code)
        self.assertEqual("TEST_NOTIFICATION", duplicate.json()["result"])
        self.assertTrue(duplicate.json()["deduped"])
        self.assertEqual(1, self.count_rows(main_module.BillingGoogleEvent))
        self.assertEqual(0, self.count_rows(main_module.BillingGooglePurchase))
        self.assertEqual(0, self.count_rows(main_module.AccountEntitlementSnapshot))
        self.assertEqual(0, self.count_rows(main_module.BillingAuditRecord))
        self.assertEqual([], self.verifier.calls)

    def test_rtdn_without_subscription_id_uses_stored_owned_purchase_metadata(self):
        purchase_token = "rtdn-owned-token-without-subscription-id"
        self.verifier.set_result(
            purchase_token,
            self.verification_result(
                status="ACTIVE",
                product_id="xcpro_xc",
                base_plan_id="annual",
            ),
        )
        sync = self.client.post(
            "/api/v1/subscriptions/googleplay/sync",
            json=self.sync_payload(
                product_id="xcpro_xc",
                base_plan_id="annual",
                purchase_token=purchase_token,
            ),
            headers=self.package_headers(),
        )
        self.assertEqual(200, sync.status_code)
        self.verifier.set_result(
            purchase_token,
            self.verification_result(
                status="EXPIRED",
                product_id="xcpro_xc",
                base_plan_id="annual",
                expiry_time_ms=1777000000000,
            ),
        )

        response = self.client.post(
            "/api/v1/subscriptions/googleplay/rtdn",
            json=self.rtdn_envelope(
                message_id="rtdn-no-subscription-id-owned",
                purchase_token=purchase_token,
                product_id=None,
            ),
            headers=self.rtdn_headers(),
        )

        self.assertEqual(200, response.status_code)
        self.assertEqual("REVOKED_OR_EXPIRED", response.json()["result"])
        self.assertEqual(
            ["xcpro_xc", "xcpro_xc"],
            [call["productId"] for call in self.verifier.calls],
        )
        purchase = self.single_row(main_module.BillingGooglePurchase)
        self.assertEqual("xcpro_xc", purchase.product_id)
        self.assertEqual("annual", purchase.base_plan_id)
        self.assertEqual("EXPIRED", self.single_row(main_module.AccountEntitlementSnapshot).status)

    def test_unknown_rtdn_without_subscription_id_records_token_not_owned(self):
        purchase_token = "rtdn-unknown-no-subscription-id-token"

        response = self.client.post(
            "/api/v1/subscriptions/googleplay/rtdn",
            json=self.rtdn_envelope(
                message_id="rtdn-no-subscription-id-unknown",
                purchase_token=purchase_token,
                product_id=None,
            ),
            headers=self.rtdn_headers(),
        )

        self.assertEqual(200, response.status_code)
        self.assertEqual("TOKEN_NOT_OWNED", response.json()["result"])
        self.assertEqual([], self.verifier.calls)
        self.assertEqual(0, self.count_rows(main_module.BillingGooglePurchase))
        self.assertEqual(0, self.count_rows(main_module.AccountEntitlementSnapshot))
        event = self.single_row(main_module.BillingGoogleEvent)
        self.assertEqual(
            main_module.hash_purchase_token(purchase_token),
            event.purchase_token_hash,
        )
        self.assertIsNone(event.product_id)
        audit = self.single_row(main_module.BillingAuditRecord)
        self.assertEqual("TOKEN_NOT_OWNED", audit.result)
        self.assertEqual(
            main_module.hash_purchase_token(purchase_token),
            audit.purchase_token_hash,
        )

    def test_rtdn_missing_message_id_and_invalid_base64_still_return_422(self):
        valid_payload = {
            "version": "1.0",
            "packageName": main_module.XCPRO_RELEASE_PACKAGE_NAME,
            "eventTimeMillis": "1777000000000",
            "testNotification": {"version": "1.0"},
        }
        missing_message_id = {
            "message": {
                "publishTime": "2026-05-15T10:00:00Z",
                "data": base64.b64encode(json.dumps(valid_payload).encode("utf-8")).decode("ascii"),
            },
            "subscription": "projects/test/subscriptions/google-play-rtdn",
        }
        invalid_base64 = {
            "message": {
                "messageId": "rtdn-invalid-base64",
                "publishTime": "2026-05-15T10:00:00Z",
                "data": "not valid base64",
            },
            "subscription": "projects/test/subscriptions/google-play-rtdn",
        }

        missing = self.client.post(
            "/api/v1/subscriptions/googleplay/rtdn",
            json=missing_message_id,
            headers=self.rtdn_headers(),
        )
        invalid = self.client.post(
            "/api/v1/subscriptions/googleplay/rtdn",
            json=invalid_base64,
            headers=self.rtdn_headers(),
        )

        self.assertEqual(422, missing.status_code)
        self.assertEqual(main_module.ErrorCode.INVALID_RTDN_ENVELOPE, missing.json()["code"])
        self.assertEqual(422, invalid.status_code)
        self.assertEqual(main_module.ErrorCode.INVALID_RTDN_ENVELOPE, invalid.json()["code"])
        self.assertEqual(0, self.count_rows(main_module.BillingGoogleEvent))

    def test_duplicate_rtdn_message_id_is_idempotent(self):
        purchase_token = "rtdn-owned-token"
        self.verifier.set_result(purchase_token, self.verification_result(status="ACTIVE"))
        sync = self.client.post(
            "/api/v1/subscriptions/googleplay/sync",
            json=self.sync_payload(purchase_token=purchase_token),
            headers=self.package_headers(),
        )
        self.assertEqual(200, sync.status_code)
        self.verifier.set_result(
            purchase_token,
            self.verification_result(status="EXPIRED", expiry_time_ms=1777000000000),
        )
        envelope = self.rtdn_envelope(
            message_id="rtdn-message-1",
            purchase_token=purchase_token,
        )

        first = self.client.post(
            "/api/v1/subscriptions/googleplay/rtdn",
            json=envelope,
            headers=self.rtdn_headers(),
        )
        duplicate = self.client.post(
            "/api/v1/subscriptions/googleplay/rtdn",
            json=envelope,
            headers=self.rtdn_headers(),
        )

        self.assertEqual(200, first.status_code)
        self.assertFalse(first.json()["deduped"])
        self.assertEqual("REVOKED_OR_EXPIRED", first.json()["result"])
        self.assertEqual(200, duplicate.status_code)
        self.assertTrue(duplicate.json()["deduped"])
        self.assertEqual(1, self.count_rows(main_module.BillingGoogleEvent))
        self.assertEqual(2, len(self.verifier.calls))

    def test_rtdn_transient_verifier_failure_returns_503_and_records_message_id(self):
        purchase_token = "rtdn-transient-token"
        self.verifier.set_result(purchase_token, self.verification_result(status="ACTIVE"))
        sync = self.client.post(
            "/api/v1/subscriptions/googleplay/sync",
            json=self.sync_payload(purchase_token=purchase_token),
            headers=self.package_headers(),
        )
        self.assertEqual(200, sync.status_code)
        self.verifier.set_result(
            purchase_token,
            main_module.GooglePlayVerificationTemporarilyUnavailable(),
        )
        envelope = self.rtdn_envelope(
            message_id="rtdn-transient-message-1",
            purchase_token=purchase_token,
        )

        response = self.client.post(
            "/api/v1/subscriptions/googleplay/rtdn",
            json=envelope,
            headers=self.rtdn_headers(),
        )

        self.assertEqual(503, response.status_code)
        self.assertEqual(
            main_module.ErrorCode.GOOGLE_PLAY_VERIFICATION_UNAVAILABLE,
            response.json()["code"],
        )
        event = self.single_row(main_module.BillingGoogleEvent)
        self.assertEqual("rtdn-transient-message-1", event.pubsub_message_id)
        self.assertEqual("VERIFICATION_TEMPORARILY_UNAVAILABLE", event.processing_result)
        self.assertIsNone(event.processed_at)
        self.assertEqual("ACTIVE", self.single_row(main_module.AccountEntitlementSnapshot).status)

    def test_retrying_same_rtdn_after_transient_reprocesses_and_terminal_duplicate_dedupes(self):
        purchase_token = "rtdn-retry-token"
        self.verifier.set_result(purchase_token, self.verification_result(status="ACTIVE"))
        sync = self.client.post(
            "/api/v1/subscriptions/googleplay/sync",
            json=self.sync_payload(purchase_token=purchase_token),
            headers=self.package_headers(),
        )
        self.assertEqual(200, sync.status_code)
        self.verifier.set_result(
            purchase_token,
            main_module.GooglePlayVerificationTemporarilyUnavailable(),
        )
        envelope = self.rtdn_envelope(
            message_id="rtdn-retry-message-1",
            purchase_token=purchase_token,
        )
        first = self.client.post(
            "/api/v1/subscriptions/googleplay/rtdn",
            json=envelope,
            headers=self.rtdn_headers(),
        )
        self.assertEqual(503, first.status_code)

        self.verifier.set_result(
            purchase_token,
            self.verification_result(status="EXPIRED", expiry_time_ms=1777000000000),
        )
        retry = self.client.post(
            "/api/v1/subscriptions/googleplay/rtdn",
            json=envelope,
            headers=self.rtdn_headers(),
        )
        terminal_duplicate = self.client.post(
            "/api/v1/subscriptions/googleplay/rtdn",
            json=envelope,
            headers=self.rtdn_headers(),
        )

        self.assertEqual(200, retry.status_code)
        self.assertFalse(retry.json()["deduped"])
        self.assertEqual("REVOKED_OR_EXPIRED", retry.json()["result"])
        snapshot = self.single_row(main_module.AccountEntitlementSnapshot)
        self.assertEqual("EXPIRED", snapshot.status)
        self.assertIsNone(snapshot.valid_until_ms)
        event = self.single_row(main_module.BillingGoogleEvent)
        self.assertEqual("REVOKED_OR_EXPIRED", event.processing_result)
        self.assertIsNotNone(event.processed_at)
        self.assertEqual(200, terminal_duplicate.status_code)
        self.assertTrue(terminal_duplicate.json()["deduped"])
        self.assertEqual("REVOKED_OR_EXPIRED", terminal_duplicate.json()["result"])
        self.assertEqual(3, len(self.verifier.calls))

    def test_rtdn_requires_infrastructure_auth_gate(self):
        envelope = self.rtdn_envelope(
            message_id="rtdn-auth-message",
            purchase_token="rtdn-auth-token",
        )

        missing = self.client.post(
            "/api/v1/subscriptions/googleplay/rtdn",
            json=envelope,
        )
        main_module.GOOGLE_PLAY_RTDN_INGEST_TOKEN = None
        unavailable = self.client.post(
            "/api/v1/subscriptions/googleplay/rtdn",
            json=envelope,
            headers=self.rtdn_headers(),
        )

        self.assertEqual(401, missing.status_code)
        self.assertEqual(main_module.ErrorCode.INVALID_RTDN_AUTH, missing.json()["code"])
        self.assertEqual(503, unavailable.status_code)
        self.assertEqual(main_module.ErrorCode.RTDN_AUTH_UNAVAILABLE, unavailable.json()["code"])

    def test_rtdn_oidc_auth_required_in_production_even_with_test_token_configured(self):
        main_module.PRIVATE_FOLLOW_RUNTIME_CONFIG = main_module.PrivateFollowRuntimeConfig(
            runtime_env=main_module.RUNTIME_ENV_PROD,
            allow_static_dev_bearer_auth=False,
            allow_debug_entitlement_package=False,
            has_static_bearer_tokens_env=False,
            static_bearer_tokens={},
            google_server_client_ids=frozenset(),
            private_follow_bearer_secret=None,
            push_token_encryption_secret=None,
            private_follow_bearer_ttl_seconds=main_module.DEFAULT_PRIVATE_FOLLOW_BEARER_TTL_SECONDS,
        )
        main_module.GOOGLE_PLAY_RTDN_ALLOW_TEST_HEADER_AUTH = True
        oidc_authenticator = FakeRtdnOidcAuthenticator()
        main_module.GOOGLE_PLAY_RTDN_OIDC_AUTHENTICATOR = oidc_authenticator
        envelope = self.rtdn_envelope(
            message_id="rtdn-oidc-auth-message",
            purchase_token="rtdn-oidc-auth-token",
        )

        token_only = self.client.post(
            "/api/v1/subscriptions/googleplay/rtdn",
            json=envelope,
            headers=self.rtdn_headers(),
        )
        oidc = self.client.post(
            "/api/v1/subscriptions/googleplay/rtdn",
            json=envelope,
            headers={
                **self.rtdn_headers(),
                "Authorization": "Bearer valid-rtdn-oidc-token",
            },
        )

        self.assertEqual(401, token_only.status_code)
        self.assertEqual(main_module.ErrorCode.INVALID_RTDN_AUTH, token_only.json()["code"])
        self.assertEqual(200, oidc.status_code)
        self.assertEqual("TOKEN_NOT_OWNED", oidc.json()["result"])
        self.assertEqual(
            [None, "Bearer valid-rtdn-oidc-token"],
            oidc_authenticator.calls,
        )

    def test_rtdn_oidc_missing_config_returns_auth_unavailable(self):
        main_module.GOOGLE_PLAY_RTDN_ALLOW_TEST_HEADER_AUTH = False
        main_module.GOOGLE_PLAY_RTDN_OIDC_AUTHENTICATOR = (
            main_module.GooglePlayRtdnOidcAuthenticator(
                config=main_module.GooglePlayRuntimeConfig(
                    package_name=main_module.XCPRO_RELEASE_PACKAGE_NAME,
                    service_account_json_path="/missing/google-play-service-account.json",
                    rtdn_oidc_audience=None,
                    rtdn_expected_service_account_email=None,
                    rtdn_test_ingest_token=None,
                    allow_test_rtdn_header_auth=False,
                )
            )
        )
        envelope = self.rtdn_envelope(
            message_id="rtdn-oidc-missing-config-message",
            purchase_token="rtdn-oidc-missing-config-token",
        )

        response = self.client.post(
            "/api/v1/subscriptions/googleplay/rtdn",
            json=envelope,
            headers={"Authorization": "Bearer any-token"},
        )

        self.assertEqual(503, response.status_code)
        self.assertEqual(main_module.ErrorCode.RTDN_AUTH_UNAVAILABLE, response.json()["code"])

    def test_audit_purchase_and_event_rows_do_not_contain_plaintext_purchase_token(self):
        purchase_token = "plain-secret-purchase-token"
        self.verifier.set_result(purchase_token, self.verification_result(status="ACTIVE"))
        sync = self.client.post(
            "/api/v1/subscriptions/googleplay/sync",
            json=self.sync_payload(purchase_token=purchase_token),
            headers=self.package_headers(),
        )
        self.assertEqual(200, sync.status_code)
        envelope = self.rtdn_envelope(
            message_id="redaction-rtdn-message",
            purchase_token=purchase_token,
        )
        self.client.post(
            "/api/v1/subscriptions/googleplay/rtdn",
            json=envelope,
            headers=self.rtdn_headers(),
        )

        db = self.session_local()
        try:
            for model in (
                main_module.BillingGooglePurchase,
                main_module.BillingGoogleEvent,
                main_module.BillingAuditRecord,
            ):
                for row in db.query(model).all():
                    values = [
                        getattr(row, column.name)
                        for column in model.__table__.columns
                    ]
                    self.assertNotIn(
                        purchase_token,
                        json.dumps(values, default=str),
                    )
            purchase = db.query(main_module.BillingGooglePurchase).one()
            self.assertEqual(
                main_module.hash_purchase_token(purchase_token),
                purchase.purchase_token_hash,
            )
        finally:
            db.close()

    def package_headers(
        self,
        token: str | None = None,
        package_name: str = main_module.XCPRO_RELEASE_PACKAGE_NAME,
        include_auth: bool = True,
    ):
        headers = {"X-XCPro-Package-Name": package_name}
        if include_auth:
            headers["Authorization"] = f"Bearer {token or self.primary_bearer_token}"
        return headers

    def rtdn_headers(self):
        return {"X-XCPro-RTDN-Token": "rtdn-test-token"}

    def sync_payload(
        self,
        package_name: str = main_module.XCPRO_RELEASE_PACKAGE_NAME,
        product_id: str = "xcpro_pro",
        base_plan_id: str = "monthly",
        purchase_token: str = "purchase-token-1",
    ):
        return {
            "packageName": package_name,
            "productId": product_id,
            "basePlanId": base_plan_id,
            "purchaseToken": purchase_token,
            "clientPurchaseState": "PURCHASED",
            "clientAcknowledged": False,
            "obfuscatedAccountId": "obfuscated-account",
            "obfuscatedProfileId": "obfuscated-profile",
            "clientSeenAtMs": 1777000000000,
            "appVersionCode": 1,
        }

    def verification_result(
        self,
        status: str,
        package_name: str = main_module.XCPRO_RELEASE_PACKAGE_NAME,
        product_id: str = "xcpro_pro",
        base_plan_id: str = "monthly",
        expiry_time_ms: int | None = 1777777777000,
        auto_renewing: bool | None = True,
        acknowledgement_required: bool = False,
        linked_purchase_token: str | None = None,
    ):
        return main_module.GooglePlayVerificationResult(
            package_name=package_name,
            product_id=product_id,
            base_plan_id=base_plan_id,
            subscription_status=status,
            expiry_time_ms=expiry_time_ms,
            auto_renewing=auto_renewing,
            acknowledgement_required=acknowledgement_required,
            linked_purchase_token=linked_purchase_token,
        )

    def publisher_subscription_response(
        self,
        subscription_state: str,
        product_id: str = "xcpro_pro",
        base_plan_id: str = "monthly",
        expiry_time: str = "2026-06-15T10:00:00Z",
        auto_renew_enabled: bool = True,
        acknowledgement_state: str = "ACKNOWLEDGEMENT_STATE_ACKNOWLEDGED",
        linked_purchase_token: str | None = None,
        cancellation_context: dict | None = None,
    ):
        response = {
            "kind": "androidpublisher#subscriptionPurchaseV2",
            "subscriptionState": subscription_state,
            "acknowledgementState": acknowledgement_state,
            "lineItems": [
                {
                    "productId": product_id,
                    "expiryTime": expiry_time,
                    "autoRenewingPlan": {
                        "autoRenewEnabled": auto_renew_enabled,
                    },
                    "offerDetails": {
                        "basePlanId": base_plan_id,
                    },
                }
            ],
        }
        if linked_purchase_token is not None:
            response["linkedPurchaseToken"] = linked_purchase_token
        if cancellation_context is not None:
            response["canceledStateContext"] = cancellation_context
        return response

    def rtdn_envelope(
        self,
        message_id: str | None,
        purchase_token: str,
        package_name: str = main_module.XCPRO_RELEASE_PACKAGE_NAME,
        product_id: str | None = "xcpro_pro",
    ):
        subscription_notification = {
            "version": "1.0",
            "notificationType": 13,
            "purchaseToken": purchase_token,
        }
        if product_id is not None:
            subscription_notification["subscriptionId"] = product_id
        payload = {
            "version": "1.0",
            "packageName": package_name,
            "eventTimeMillis": "1777000000000",
            "subscriptionNotification": subscription_notification,
        }
        message = {
            "publishTime": "2026-05-15T10:00:00Z",
            "data": base64.b64encode(json.dumps(payload).encode("utf-8")).decode("ascii"),
        }
        if message_id is not None:
            message["messageId"] = message_id
        return {
            "message": message,
            "subscription": "projects/test/subscriptions/google-play-rtdn",
        }

    def rtdn_test_notification_envelope(
        self,
        message_id: str | None,
        package_name: str = main_module.XCPRO_RELEASE_PACKAGE_NAME,
    ):
        payload = {
            "version": "1.0",
            "packageName": package_name,
            "eventTimeMillis": "1777000000000",
            "testNotification": {"version": "1.0"},
        }
        message = {
            "publishTime": "2026-05-15T10:00:00Z",
            "data": base64.b64encode(json.dumps(payload).encode("utf-8")).decode("ascii"),
        }
        if message_id is not None:
            message["messageId"] = message_id
        return {
            "message": message,
            "subscription": "projects/test/subscriptions/google-play-rtdn",
        }

    def count_rows(self, model) -> int:
        db = self.session_local()
        try:
            return db.query(model).count()
        finally:
            db.close()

    def single_row(self, model):
        db = self.session_local()
        try:
            return db.query(model).one()
        finally:
            db.close()


if __name__ == "__main__":
    unittest.main()
