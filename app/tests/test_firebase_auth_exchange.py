import inspect
import json
import unittest
from dataclasses import replace
from datetime import datetime, timedelta

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app import main as main_module
from app.scripts import support_billing_snapshot as support_billing_snapshot_script


class FakeFirebaseApp:
    def __init__(self, credential, options, name):
        self.credential = credential
        self.options = options
        self.name = name


class FakeFirebaseAdmin:
    def __init__(self):
        self.initialized_apps = []
        self.deleted_apps = []

    def initialize_app(self, credential, options, name=None):
        app = FakeFirebaseApp(credential, options, name)
        self.initialized_apps.append(app)
        return app

    def delete_app(self, app):
        self.deleted_apps.append(app)

    def get_app(self, _name):
        raise ValueError("app not found")


class FakeFirebaseCredentials:
    def __init__(self):
        self.application_default_calls = 0
        self.certificate_paths = []

    def ApplicationDefault(self):
        self.application_default_calls += 1
        return {"kind": "application_default"}

    def Certificate(self, path):
        self.certificate_paths.append(path)
        return {"kind": "certificate", "path": path}


class FakeFirebaseAuth:
    class CertificateFetchError(Exception):
        pass

    class RevokedIdTokenError(Exception):
        pass

    class UserDisabledError(Exception):
        pass

    def __init__(self, decoded_token=None, exception_to_raise=None):
        self.decoded_token = decoded_token
        self.exception_to_raise = exception_to_raise
        self.calls = []

    def verify_id_token(self, token, app=None, check_revoked=False):
        self.calls.append({
            "token": token,
            "app": app,
            "check_revoked": check_revoked,
        })
        if self.exception_to_raise is not None:
            raise self.exception_to_raise
        return self.decoded_token


class FakeFirebaseExceptions:
    class DeadlineExceededError(Exception):
        pass

    class InternalError(Exception):
        pass

    class UnavailableError(Exception):
        pass


class MutableClock:
    def __init__(self, current: datetime):
        self.current = current

    def utcnow(self) -> datetime:
        return self.current

    def advance(self, **kwargs):
        self.current += timedelta(**kwargs)


class FirebaseAuthVerifierSeamTest(unittest.TestCase):
    MISSING = object()

    def setUp(self):
        self.project_id = "xcpro-firebase-auth"
        self.original_private_follow_runtime_config = (
            main_module.PRIVATE_FOLLOW_RUNTIME_CONFIG
        )
        self.original_firebase_admin = main_module.firebase_admin
        self.original_firebase_auth = main_module.firebase_auth
        self.original_firebase_credentials = main_module.firebase_credentials
        self.original_firebase_exceptions = main_module.firebase_exceptions
        self.original_firebase_id_token_verifier = (
            main_module.FIREBASE_ID_TOKEN_VERIFIER
        )
        self.original_firebase_app = main_module._FIREBASE_AUTH_APP
        self.original_firebase_app_config_key = main_module._FIREBASE_AUTH_APP_CONFIG_KEY
        self.reset_firebase_app_cache()

    def tearDown(self):
        main_module.PRIVATE_FOLLOW_RUNTIME_CONFIG = (
            self.original_private_follow_runtime_config
        )
        main_module.firebase_admin = self.original_firebase_admin
        main_module.firebase_auth = self.original_firebase_auth
        main_module.firebase_credentials = self.original_firebase_credentials
        main_module.firebase_exceptions = self.original_firebase_exceptions
        main_module.FIREBASE_ID_TOKEN_VERIFIER = (
            self.original_firebase_id_token_verifier
        )
        main_module._FIREBASE_AUTH_APP = self.original_firebase_app
        main_module._FIREBASE_AUTH_APP_CONFIG_KEY = (
            self.original_firebase_app_config_key
        )

    def test_required_verifier_symbols_are_available(self):
        self.assertTrue(hasattr(main_module, "ResolvedFirebaseIdentity"))
        self.assertIs(
            main_module.verify_firebase_id_token_for_exchange,
            main_module.FIREBASE_ID_TOKEN_VERIFIER,
        )

    def test_valid_token_resolves_firebase_identity(self):
        fake_auth, fake_credentials, fake_admin = self.install_firebase_fakes(
            decoded_token=self.decoded_token()
        )

        identity = main_module.FIREBASE_ID_TOKEN_VERIFIER(" valid-token ")

        self.assertEqual("firebase-uid-1", identity.firebase_uid)
        self.assertEqual("pilot@example.com", identity.email)
        self.assertTrue(identity.email_verified)
        self.assertEqual("Pilot One", identity.display_name)
        self.assertEqual("google.com", identity.sign_in_provider)
        self.assertEqual(
            {
                "email": ["pilot@example.com"],
                "google.com": ["google-provider-subject-1"],
            },
            identity.provider_identities,
        )
        self.assertEqual(1, fake_credentials.application_default_calls)
        self.assertEqual([], fake_credentials.certificate_paths)
        self.assertEqual(
            {
                "token": "valid-token",
                "app": fake_admin.initialized_apps[0],
                "check_revoked": True,
            },
            fake_auth.calls[0],
        )
        self.assertEqual(
            {"projectId": self.project_id},
            fake_admin.initialized_apps[0].options,
        )

    def test_service_account_path_uses_certificate_credentials(self):
        service_account_path = "/run/secrets/firebase-auth-service-account.json"
        _fake_auth, fake_credentials, fake_admin = self.install_firebase_fakes(
            decoded_token=self.decoded_token(),
            service_account_json_path=service_account_path,
        )

        identity = main_module.verify_firebase_id_token_for_exchange("valid-token")

        self.assertEqual("firebase-uid-1", identity.firebase_uid)
        self.assertEqual([service_account_path], fake_credentials.certificate_paths)
        self.assertEqual(0, fake_credentials.application_default_calls)
        self.assertEqual(
            {"kind": "certificate", "path": service_account_path},
            fake_admin.initialized_apps[0].credential,
        )

    def test_blank_token_is_rejected_as_invalid(self):
        with self.assertRaises(main_module.ApiHTTPException) as raised:
            main_module.verify_firebase_id_token_for_exchange("   ")

        self.assertEqual(422, raised.exception.status_code)
        self.assertEqual(
            main_module.ErrorCode.INVALID_FIREBASE_ID_TOKEN,
            raised.exception.code,
        )

    def test_invalid_token_is_rejected_as_invalid_without_raw_details(self):
        raw_token = "raw-invalid-token"
        self.install_firebase_fakes(
            exception_to_raise=Exception(f"sdk failure includes {raw_token}")
        )

        with self.assertRaises(main_module.ApiHTTPException) as raised:
            main_module.verify_firebase_id_token_for_exchange(raw_token)

        self.assertEqual(401, raised.exception.status_code)
        self.assertEqual(
            main_module.ErrorCode.INVALID_FIREBASE_ID_TOKEN,
            raised.exception.code,
        )
        self.assertNotIn(raw_token, str(raised.exception.detail))
        self.assertNotIn("sdk failure", str(raised.exception.detail))

    def test_wrong_project_audience_or_issuer_is_rejected_as_invalid(self):
        cases = [
            ("audience", {"aud": "other-project"}),
            ("issuer", {"iss": "https://securetoken.google.com/other-project"}),
        ]

        for case_name, overrides in cases:
            with self.subTest(case_name=case_name):
                self.reset_firebase_app_cache()
                self.install_firebase_fakes(
                    decoded_token=self.decoded_token(**overrides)
                )

                with self.assertRaises(main_module.ApiHTTPException) as raised:
                    main_module.verify_firebase_id_token_for_exchange("valid-token")

                self.assertEqual(401, raised.exception.status_code)
                self.assertEqual(
                    main_module.ErrorCode.INVALID_FIREBASE_ID_TOKEN,
                    raised.exception.code,
                )

    def test_revoked_token_is_rejected_as_invalid(self):
        self.install_firebase_fakes(
            exception_to_raise=FakeFirebaseAuth.RevokedIdTokenError("revoked")
        )

        with self.assertRaises(main_module.ApiHTTPException) as raised:
            main_module.verify_firebase_id_token_for_exchange("revoked-token")

        self.assertEqual(401, raised.exception.status_code)
        self.assertEqual(
            main_module.ErrorCode.INVALID_FIREBASE_ID_TOKEN,
            raised.exception.code,
        )

    def test_disabled_user_is_rejected_as_invalid(self):
        self.install_firebase_fakes(
            exception_to_raise=FakeFirebaseAuth.UserDisabledError("disabled")
        )

        with self.assertRaises(main_module.ApiHTTPException) as raised:
            main_module.verify_firebase_id_token_for_exchange("disabled-user-token")

        self.assertEqual(401, raised.exception.status_code)
        self.assertEqual(
            main_module.ErrorCode.INVALID_FIREBASE_ID_TOKEN,
            raised.exception.code,
        )

    def test_missing_project_config_is_auth_unavailable(self):
        self.install_firebase_fakes(
            decoded_token=self.decoded_token(),
            project_id=None,
        )

        with self.assertRaises(main_module.ApiHTTPException) as raised:
            main_module.verify_firebase_id_token_for_exchange("valid-token")

        self.assertEqual(503, raised.exception.status_code)
        self.assertEqual(main_module.ErrorCode.AUTH_UNAVAILABLE, raised.exception.code)

    def test_verifier_unavailable_does_not_expose_raw_details(self):
        raw_token = "valid-token-with-cert-fetch-failure"
        self.install_firebase_fakes(
            exception_to_raise=FakeFirebaseAuth.CertificateFetchError(
                f"cert fetch failed for {raw_token}"
            )
        )

        with self.assertRaises(main_module.ApiHTTPException) as raised:
            main_module.verify_firebase_id_token_for_exchange(raw_token)

        self.assertEqual(503, raised.exception.status_code)
        self.assertEqual(main_module.ErrorCode.AUTH_UNAVAILABLE, raised.exception.code)
        self.assertNotIn(raw_token, str(raised.exception.detail))
        self.assertNotIn("cert fetch failed", str(raised.exception.detail))

    def test_custom_claims_and_non_identity_authority_claims_are_not_exposed(self):
        decoded_token = self.decoded_token()
        decoded_token.update(
            {
                "tier": "SOARING",
                "paid": True,
                "subscription_state": "ACTIVE",
                "entitlement": {"tier": "SOARING"},
                "subscription": {"status": "ACTIVE"},
                "billing": {"provider": "firebase"},
                "device": {"limit": 99},
                "liveFollow": {"enabled": True},
                "custom_claims": {
                    "tier": "SOARING",
                    "paid": True,
                    "subscription": "ACTIVE",
                },
                "customClaims": {
                    "entitlement": "SOARING",
                    "LiveFollow": True,
                },
            }
        )
        self.install_firebase_fakes(decoded_token=decoded_token)

        identity = main_module.verify_firebase_id_token_for_exchange("valid-token")

        self.assertEqual(
            {
                "firebase_uid",
                "email",
                "email_verified",
                "display_name",
                "sign_in_provider",
                "provider_identities",
            },
            set(identity.__dict__.keys()),
        )
        for authority_attribute in (
            "tier",
            "paid",
            "subscription_state",
            "entitlement",
            "subscription",
            "billing",
            "device",
            "liveFollow",
            "custom_claims",
            "customClaims",
        ):
            with self.subTest(authority_attribute=authority_attribute):
                self.assertFalse(hasattr(identity, authority_attribute))

    def test_exchange_endpoint_sanitizes_firebase_admin_sdk_exception_details(self):
        cases = [
            (
                "raw-firebase-token-invalid-sdk-detail",
                Exception(
                    "Firebase Admin SDK invalid token detail includes "
                    "raw-firebase-token-invalid-sdk-detail"
                ),
                401,
                main_module.ErrorCode.INVALID_FIREBASE_ID_TOKEN,
                "Firebase Admin SDK invalid token detail",
            ),
            (
                "raw-firebase-token-unavailable-sdk-detail",
                FakeFirebaseAuth.CertificateFetchError(
                    "certificate fetch failed for "
                    "raw-firebase-token-unavailable-sdk-detail"
                ),
                503,
                main_module.ErrorCode.AUTH_UNAVAILABLE,
                "certificate fetch failed",
            ),
        ]

        for raw_token, exception, status_code, code, raw_detail in cases:
            with self.subTest(raw_token=raw_token):
                self.reset_firebase_app_cache()
                self.install_firebase_fakes(exception_to_raise=exception)
                main_module.FIREBASE_ID_TOKEN_VERIFIER = (
                    main_module.verify_firebase_id_token_for_exchange
                )
                client = TestClient(main_module.app)
                try:
                    response = client.post(
                        "/api/v2/auth/firebase/exchange",
                        json={"firebase_id_token": raw_token},
                    )
                finally:
                    client.close()

                self.assertEqual(status_code, response.status_code)
                self.assertEqual(code, response.json()["code"])
                serialized = str(response.json())
                self.assertNotIn(raw_token, serialized)
                self.assertNotIn(raw_detail, serialized)

    def test_firebase_auth_exchange_path_does_not_log_or_print_raw_tokens(self):
        source = "\n".join(
            inspect.getsource(target)
            for target in (
                main_module.exchange_firebase_auth_token,
                main_module.verify_firebase_id_token_for_exchange,
                main_module.initialize_firebase_auth_app_for_exchange,
                main_module.get_firebase_auth_app_for_exchange,
            )
        )

        for forbidden_logging_marker in ("print(", "logger.", "logging."):
            with self.subTest(forbidden_logging_marker=forbidden_logging_marker):
                self.assertNotIn(forbidden_logging_marker, source)

    def install_firebase_fakes(
        self,
        decoded_token=None,
        exception_to_raise=None,
        project_id=MISSING,
        service_account_json_path=None,
    ):
        if project_id is self.MISSING:
            project_id = self.project_id

        fake_auth = FakeFirebaseAuth(
            decoded_token=decoded_token,
            exception_to_raise=exception_to_raise,
        )
        fake_credentials = FakeFirebaseCredentials()
        fake_admin = FakeFirebaseAdmin()

        main_module.firebase_auth = fake_auth
        main_module.firebase_credentials = fake_credentials
        main_module.firebase_admin = fake_admin
        main_module.firebase_exceptions = FakeFirebaseExceptions
        main_module.PRIVATE_FOLLOW_RUNTIME_CONFIG = replace(
            self.original_private_follow_runtime_config,
            firebase_auth_project_id=project_id,
            firebase_auth_service_account_json_path=service_account_json_path,
        )

        return fake_auth, fake_credentials, fake_admin

    def reset_firebase_app_cache(self):
        main_module._FIREBASE_AUTH_APP = None
        main_module._FIREBASE_AUTH_APP_CONFIG_KEY = None

    def decoded_token(self, **overrides):
        decoded_token = {
            "uid": "firebase-uid-1",
            "aud": self.project_id,
            "iss": f"https://securetoken.google.com/{self.project_id}",
            "email": "pilot@example.com",
            "email_verified": True,
            "name": "Pilot One",
            "firebase": {
                "sign_in_provider": "google.com",
                "identities": {
                    "email": ["pilot@example.com"],
                    "google.com": ["google-provider-subject-1"],
                },
            },
        }
        decoded_token.update(overrides)
        return decoded_token


class FirebaseAuthExchangeEndpointTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        self.session_local = sessionmaker(bind=self.engine)
        main_module.Base.metadata.create_all(bind=self.engine)

        self.original_session_local = main_module.SessionLocal
        self.original_firebase_id_token_verifier = (
            main_module.FIREBASE_ID_TOKEN_VERIFIER
        )
        self.original_private_follow_bearer_secret = (
            main_module.PRIVATE_FOLLOW_BEARER_SECRET
        )
        self.original_utcnow = main_module.utcnow
        self.original_private_follow_runtime_config = (
            main_module.PRIVATE_FOLLOW_RUNTIME_CONFIG
        )

        self.clock = MutableClock(datetime(2026, 3, 20, 12, 0, 0))
        self.firebase_id_token = "firebase-id-token-1"
        self.verifier_calls = []
        self.verifier_results = {}

        main_module.SessionLocal = self.session_local
        main_module.FIREBASE_ID_TOKEN_VERIFIER = self.fake_firebase_id_token_verifier
        main_module.PRIVATE_FOLLOW_BEARER_SECRET = b"test-private-follow-secret"
        main_module.utcnow = self.clock.utcnow

        self.client = TestClient(main_module.app)

    def tearDown(self):
        self.client.close()
        main_module.SessionLocal = self.original_session_local
        main_module.FIREBASE_ID_TOKEN_VERIFIER = (
            self.original_firebase_id_token_verifier
        )
        main_module.PRIVATE_FOLLOW_BEARER_SECRET = (
            self.original_private_follow_bearer_secret
        )
        main_module.utcnow = self.original_utcnow
        main_module.PRIVATE_FOLLOW_RUNTIME_CONFIG = (
            self.original_private_follow_runtime_config
        )
        main_module.Base.metadata.drop_all(bind=self.engine)
        self.engine.dispose()

    def test_required_endpoint_model_and_error_symbols_are_available(self):
        self.assertTrue(hasattr(main_module, "FirebaseAuthExchangeRequest"))
        self.assertTrue(hasattr(main_module, "build_auth_recovery_required_detail"))
        self.assertEqual(
            "email_verification_required",
            main_module.ErrorCode.EMAIL_VERIFICATION_REQUIRED,
        )
        self.assertEqual(
            "auth_recovery_required",
            main_module.ErrorCode.AUTH_RECOVERY_REQUIRED,
        )
        self.assertEqual(
            "unsupported_firebase_auth_provider",
            main_module.ErrorCode.UNSUPPORTED_FIREBASE_AUTH_PROVIDER,
        )
        route_paths = {getattr(route, "path", None) for route in main_module.app.routes}
        self.assertIn("/api/v2/auth/firebase/exchange", route_paths)

    def test_blank_token_returns_invalid_firebase_id_token(self):
        response = self.exchange("   ")

        self.assertEqual(422, response.status_code)
        self.assertEqual(
            main_module.ErrorCode.INVALID_FIREBASE_ID_TOKEN,
            response.json()["code"],
        )
        self.assertEqual([], self.verifier_calls)

    def test_invalid_wrong_project_revoked_and_disabled_tokens_return_401(self):
        for token in (
            "invalid-token",
            "wrong-project-token",
            "revoked-token",
            "disabled-user-token",
        ):
            with self.subTest(token=token):
                self.verifier_results[token] = main_module.invalid_firebase_id_token_exception()

                response = self.exchange(token)

                self.assertEqual(401, response.status_code)
                self.assertEqual(
                    main_module.ErrorCode.INVALID_FIREBASE_ID_TOKEN,
                    response.json()["code"],
                )
                self.assertNotIn(token, str(response.json()))

    def test_missing_config_or_unavailable_verifier_returns_auth_unavailable(self):
        self.verifier_results[self.firebase_id_token] = (
            main_module.firebase_auth_unavailable_exception(
                "Firebase Auth project ID is not configured"
            )
        )

        response = self.exchange(self.firebase_id_token)

        self.assertEqual(503, response.status_code)
        self.assertEqual(main_module.ErrorCode.AUTH_UNAVAILABLE, response.json()["code"])

        main_module.FIREBASE_ID_TOKEN_VERIFIER = None
        response = self.exchange("another-token")

        self.assertEqual(503, response.status_code)
        self.assertEqual(main_module.ErrorCode.AUTH_UNAVAILABLE, response.json()["code"])

    def test_unverified_email_password_requires_email_verification(self):
        for sign_in_provider in ("password", "email-password"):
            with self.subTest(sign_in_provider=sign_in_provider):
                token = f"{sign_in_provider}-token"
                self.verifier_results[token] = self.firebase_identity(
                    firebase_uid=f"firebase-{sign_in_provider}",
                    email_verified=False,
                    sign_in_provider=sign_in_provider,
                )

                response = self.exchange(token)

                self.assertEqual(403, response.status_code)
                self.assertEqual(
                    main_module.ErrorCode.EMAIL_VERIFICATION_REQUIRED,
                    response.json()["code"],
                )

    def test_verified_email_password_creates_account_defaults_and_usable_bearer(self):
        self.verifier_results[self.firebase_id_token] = self.firebase_identity(
            sign_in_provider="password",
            email_verified=True,
        )

        response = self.exchange(self.firebase_id_token)

        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assert_firebase_exchange_response_has_bootstrap(body)
        self.assertEqual("Bearer", body["token_type"])
        self.assertEqual("email_password", body["auth_method"])
        self.assertNotIn(self.firebase_id_token, str(body))
        self.assertEqual(body["user_id"], body["profile"]["user_id"])
        self.assertEqual("Firebase Pilot", body["profile"]["display_name"])
        self.assertEqual("searchable", body["profile"]["privacy"]["discoverability"])
        self.assertEqual(
            {
                "following_count": 0,
                "max_following": 1,
                "status": "under_limit",
            },
            body["profile"]["relationship_limits"],
        )
        self.assertEqual(body["user_id"], body["entitlement"]["accountSubject"])
        self.assertEqual("FREE", body["entitlement"]["tier"])
        self.assertEqual("FREE_CANONICAL", body["entitlement"]["verificationState"])
        self.assertIsNone(body["entitlement"]["validUntilMs"])

        bearer_identity = main_module.verify_private_follow_bearer(body["access_token"])
        self.assertEqual("firebase", bearer_identity.provider)
        self.assertEqual("firebase-uid-1", bearer_identity.provider_subject)

        me_response = self.client.get(
            "/api/v2/me",
            headers=self.bearer_headers(body["access_token"]),
        )
        self.assertEqual(200, me_response.status_code)
        me_body = me_response.json()
        self.assertEqual(body["user_id"], me_body["user_id"])
        self.assertEqual("Firebase Pilot", me_body["display_name"])
        self.assertEqual("searchable", me_body["privacy"]["discoverability"])

        db = self.session_local()
        try:
            self.assertEqual(1, db.query(main_module.User).count())
            self.assertEqual(1, db.query(main_module.AuthIdentity).count())
            self.assertEqual(1, db.query(main_module.PilotProfile).count())
            self.assertEqual(1, db.query(main_module.PrivacySetting).count())
            auth_identity = db.query(main_module.AuthIdentity).one()
            self.assertIs(auth_identity.provider_email_verified, True)
            support_snapshot = main_module.build_billing_support_snapshot(
                db,
                body["user_id"],
            )
            self.assertIs(
                support_snapshot["accountOwner"]["authProviderEmailVerified"],
                True,
            )
        finally:
            db.close()

    def test_verified_email_password_alias_returns_email_password_auth_method(self):
        self.verifier_results[self.firebase_id_token] = self.firebase_identity(
            sign_in_provider="email-password",
            email_verified=True,
        )

        response = self.exchange(self.firebase_id_token)

        self.assertEqual(200, response.status_code)
        self.assertEqual("email_password", response.json()["auth_method"])

    def test_phone_provider_creates_account_defaults_and_usable_bearer(self):
        phone_number = "+15555550100"
        self.verifier_results[self.firebase_id_token] = self.firebase_identity(
            sign_in_provider="phone",
            email=None,
            email_verified=False,
            display_name="Phone Pilot",
            provider_identities={"phone": [phone_number]},
        )

        response = self.exchange(self.firebase_id_token)

        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assert_firebase_exchange_response_has_bootstrap(body)
        self.assertEqual("Bearer", body["token_type"])
        self.assertEqual("phone", body["auth_method"])
        self.assertNotIn(self.firebase_id_token, str(body))
        self.assertNotIn(phone_number, str(body))
        self.assertEqual(body["user_id"], body["profile"]["user_id"])
        self.assertEqual("Phone Pilot", body["profile"]["display_name"])
        self.assertEqual("searchable", body["profile"]["privacy"]["discoverability"])
        self.assertEqual(body["user_id"], body["entitlement"]["accountSubject"])
        self.assertEqual("FREE", body["entitlement"]["tier"])
        self.assertEqual("FREE_CANONICAL", body["entitlement"]["verificationState"])

        bearer_identity = main_module.verify_private_follow_bearer(body["access_token"])
        self.assertEqual("firebase", bearer_identity.provider)
        self.assertEqual("firebase-uid-1", bearer_identity.provider_subject)

        me_response = self.client.get(
            "/api/v2/me",
            headers=self.bearer_headers(body["access_token"]),
        )
        self.assertEqual(200, me_response.status_code)
        self.assertEqual(body["user_id"], me_response.json()["user_id"])

        db = self.session_local()
        try:
            self.assertEqual(1, db.query(main_module.User).count())
            self.assertEqual(1, db.query(main_module.AuthIdentity).count())
            self.assertEqual(1, db.query(main_module.PilotProfile).count())
            self.assertEqual(1, db.query(main_module.PrivacySetting).count())
            auth_identity = db.query(main_module.AuthIdentity).one()
            self.assertEqual("firebase", auth_identity.provider)
            self.assertEqual("firebase-uid-1", auth_identity.provider_subject)
            self.assertIsNone(auth_identity.provider_email)
            self.assertIs(auth_identity.provider_email_verified, False)
            support_snapshot = main_module.build_billing_support_snapshot(
                db,
                body["user_id"],
            )
            self.assertNotIn(phone_number, json.dumps(support_snapshot, default=str))
        finally:
            db.close()

    def test_existing_phone_identity_reuses_user_and_updates_last_seen(self):
        self.verifier_results[self.firebase_id_token] = self.firebase_identity(
            sign_in_provider="phone",
            email=None,
            email_verified=False,
            display_name=None,
            provider_identities={"phone": ["+15555550101"]},
        )
        first_response = self.exchange(self.firebase_id_token)
        self.assertEqual(200, first_response.status_code)
        user_id = first_response.json()["user_id"]

        db = self.session_local()
        try:
            auth_identity = db.query(main_module.AuthIdentity).one()
            first_seen_at = auth_identity.last_seen_at
            self.assertEqual(self.clock.utcnow(), first_seen_at)
        finally:
            db.close()

        self.clock.advance(minutes=5)
        self.verifier_results[self.firebase_id_token] = self.firebase_identity(
            sign_in_provider="phone",
            email=None,
            email_verified=False,
            display_name="Ignored Updated Phone Name",
            provider_identities={"phone": ["+15555550102"]},
        )

        second_response = self.exchange(self.firebase_id_token)

        self.assertEqual(200, second_response.status_code)
        self.assertEqual("phone", second_response.json()["auth_method"])
        self.assertEqual(user_id, second_response.json()["user_id"])
        db = self.session_local()
        try:
            auth_identity = db.query(main_module.AuthIdentity).one()
            self.assertEqual(self.clock.utcnow(), auth_identity.last_seen_at)
            self.assertGreater(auth_identity.last_seen_at, first_seen_at)
            self.assertIsNone(auth_identity.provider_email)
            self.assertIs(auth_identity.provider_email_verified, False)
            self.assertEqual(1, db.query(main_module.User).count())
            self.assertEqual(1, db.query(main_module.AuthIdentity).count())
        finally:
            db.close()

    def test_phone_provider_without_email_or_display_name_leaves_profile_incomplete(self):
        self.verifier_results[self.firebase_id_token] = self.firebase_identity(
            sign_in_provider="phone",
            email=None,
            email_verified=False,
            display_name=None,
            provider_identities={"phone": ["+15555550103"]},
        )

        response = self.exchange(self.firebase_id_token)

        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertEqual("phone", body["auth_method"])
        self.assertIsNone(body["profile"]["handle"])
        self.assertIsNone(body["profile"]["display_name"])
        self.assertIsNone(body["profile"]["comp_number"])
        db = self.session_local()
        try:
            profile = db.query(main_module.PilotProfile).one()
            self.assertIsNone(profile.handle)
            self.assertIsNone(profile.display_name)
            self.assertIsNone(profile.comp_number)
        finally:
            db.close()

    def test_unsupported_and_missing_firebase_providers_are_rejected_without_mutation(self):
        cases = (
            ("github.com", "github.com"),
            (None, "unknown"),
            (" ", "unknown"),
        )
        for raw_provider, expected_provider in cases:
            with self.subTest(raw_provider=raw_provider):
                token = f"unsupported-{expected_provider}-token"
                self.verifier_results[token] = self.firebase_identity(
                    firebase_uid=f"firebase-{expected_provider}",
                    sign_in_provider=raw_provider,
                    provider_identities={},
                )

                response = self.exchange(token)

                self.assertEqual(403, response.status_code)
                body = response.json()
                self.assertEqual(
                    main_module.ErrorCode.UNSUPPORTED_FIREBASE_AUTH_PROVIDER,
                    body["code"],
                )
                self.assertEqual(
                    f"Unsupported Firebase auth method: {expected_provider}",
                    body["detail"],
                )
                self.assertNotIn(token, str(body))
                db = self.session_local()
                try:
                    self.assertEqual(0, db.query(main_module.User).count())
                    self.assertEqual(0, db.query(main_module.AuthIdentity).count())
                    self.assertEqual(0, db.query(main_module.PilotProfile).count())
                    self.assertEqual(0, db.query(main_module.PrivacySetting).count())
                finally:
                    db.close()

    def test_unverified_firebase_google_persists_unverified_email_state(self):
        self.verifier_results[self.firebase_id_token] = self.firebase_identity(
            email_verified=False,
            sign_in_provider="google.com",
        )

        response = self.exchange(self.firebase_id_token)

        self.assertEqual(200, response.status_code)
        self.assertEqual("firebase_google", response.json()["auth_method"])
        db = self.session_local()
        try:
            auth_identity = (
                db.query(main_module.AuthIdentity)
                .filter(main_module.AuthIdentity.provider == "firebase")
                .one()
            )
            self.assertIs(auth_identity.provider_email_verified, False)
        finally:
            db.close()

    def test_existing_firebase_identity_reuses_user_and_updates_identity(self):
        self.verifier_results[self.firebase_id_token] = self.firebase_identity(
            email="first@example.com",
            email_verified=False,
        )
        first_response = self.exchange(self.firebase_id_token)
        self.assertEqual(200, first_response.status_code)
        user_id = first_response.json()["user_id"]
        db = self.session_local()
        try:
            auth_identity = (
                db.query(main_module.AuthIdentity)
                .filter(main_module.AuthIdentity.provider == "firebase")
                .one()
            )
            self.assertIs(auth_identity.provider_email_verified, False)
        finally:
            db.close()

        self.clock.advance(minutes=5)
        self.verifier_results[self.firebase_id_token] = self.firebase_identity(
            email="updated@example.com",
            email_verified=True,
        )
        second_response = self.exchange(self.firebase_id_token)

        self.assertEqual(200, second_response.status_code)
        self.assertEqual(user_id, second_response.json()["user_id"])
        db = self.session_local()
        try:
            self.assertEqual(1, db.query(main_module.User).count())
            auth_identity = (
                db.query(main_module.AuthIdentity)
                .filter(main_module.AuthIdentity.provider == "firebase")
                .one()
            )
            self.assertEqual("updated@example.com", auth_identity.provider_email)
            self.assertIs(auth_identity.provider_email_verified, True)
            self.assertEqual(self.clock.utcnow(), auth_identity.last_seen_at)
            self.assertEqual(self.clock.utcnow(), auth_identity.updated_at)
        finally:
            db.close()

    def test_firebase_google_does_not_link_without_exact_google_subject(self):
        legacy_user_id = self.seed_legacy_google_user(
            provider_subject="google-provider-subject-1",
            email="legacy@example.com",
        )
        self.verifier_results[self.firebase_id_token] = self.firebase_identity(
            provider_identities={"google.com": ["different-google-subject"]},
            email="firebase@example.com",
        )

        response = self.exchange(self.firebase_id_token)

        self.assertEqual(200, response.status_code)
        self.assertNotEqual(legacy_user_id, response.json()["user_id"])

    def test_firebase_google_link_persists_identity_and_issues_firebase_bearer(self):
        google_provider_subject = "google-provider-subject-1"
        legacy_user_id = self.seed_legacy_google_user(
            provider_subject=google_provider_subject,
            email="legacy@example.com",
        )
        self.verifier_results[self.firebase_id_token] = self.firebase_identity(
            provider_identities={"google.com": [google_provider_subject]},
            email="legacy@example.com",
        )

        response = self.exchange(self.firebase_id_token)

        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertEqual(legacy_user_id, body["user_id"])
        bearer_identity = main_module.verify_private_follow_bearer(body["access_token"])
        self.assertEqual("firebase", bearer_identity.provider)
        self.assertEqual("firebase-uid-1", bearer_identity.provider_subject)

        me_response = self.client.get(
            "/api/v2/me",
            headers=self.bearer_headers(body["access_token"]),
        )
        self.assertEqual(200, me_response.status_code)
        me_body = me_response.json()
        self.assertEqual(legacy_user_id, me_body["user_id"])
        self.assertEqual("Legacy Google Pilot", me_body["display_name"])

        db = self.session_local()
        try:
            firebase_identity = (
                db.query(main_module.AuthIdentity)
                .filter(
                    main_module.AuthIdentity.provider == "firebase",
                    main_module.AuthIdentity.provider_subject == "firebase-uid-1",
                )
                .one()
            )
            self.assertEqual(legacy_user_id, firebase_identity.user_id)
            self.assertEqual(2, db.query(main_module.AuthIdentity).count())
            self.assertEqual(1, db.query(main_module.User).count())
        finally:
            db.close()

    def test_multiple_firebase_google_subjects_do_not_link_legacy_google(self):
        legacy_user_id = self.seed_legacy_google_user(
            provider_subject="google-provider-subject-1",
            email="legacy@example.com",
        )
        self.verifier_results[self.firebase_id_token] = self.firebase_identity(
            provider_identities={
                "google.com": ["google-provider-subject-1", "other-google-subject"],
            },
            email="firebase@example.com",
        )

        response = self.exchange(self.firebase_id_token)

        self.assertEqual(200, response.status_code)
        self.assertNotEqual(legacy_user_id, response.json()["user_id"])

    def test_verified_email_with_non_matching_google_subject_requires_recovery(self):
        legacy_user_id = self.seed_legacy_google_user(
            provider_subject="google-provider-subject-1",
            email="pilot@example.com",
        )
        self.verifier_results[self.firebase_id_token] = self.firebase_identity(
            email=" pilot@example.com ",
            email_verified=True,
            provider_identities={
                "email": ["pilot@example.com"],
                "google.com": ["different-google-subject"],
            },
        )

        response = self.exchange(self.firebase_id_token)

        self.assertEqual(409, response.status_code)
        body = response.json()
        self.assertEqual(
            main_module.ErrorCode.AUTH_RECOVERY_REQUIRED,
            body["code"],
        )
        detail = body["detail"]
        self.assertEqual(
            {
                "message",
                "reason",
                "provider",
                "providerEmail",
                "conflictProvider",
                "conflictProviderEmail",
                "conflictProviderEmailVerified",
            },
            set(detail.keys()),
        )
        self.assertEqual("account recovery is required", detail["message"])
        self.assertEqual("verified_email_conflict", detail["reason"])
        self.assertEqual("firebase", detail["provider"])
        self.assertEqual("pilot@example.com", detail["providerEmail"])
        self.assertEqual("google", detail["conflictProvider"])
        self.assertEqual("pilot@example.com", detail["conflictProviderEmail"])
        self.assertIsNone(detail["conflictProviderEmailVerified"])
        body_text = str(body)
        for forbidden_value in (
            self.firebase_id_token,
            "Bearer",
            legacy_user_id,
            "google-provider-subject-1",
            "Legacy Google Pilot",
            "billing",
            "entitlement",
            "LiveFollow",
            "purchase",
        ):
            with self.subTest(forbidden_value=forbidden_value):
                self.assertNotIn(forbidden_value, body_text)
        db = self.session_local()
        try:
            self.assertEqual(1, db.query(main_module.User).count())
            self.assertEqual(
                0,
                db.query(main_module.AuthIdentity)
                .filter(main_module.AuthIdentity.provider == "firebase")
                .count(),
            )
        finally:
            db.close()

    def test_recovery_required_detail_support_lookup_context_is_safe_and_read_only(self):
        firebase_id_token = "firebase-id-token-conflict-repair"
        google_id_token = "raw-google-id-token-conflict-repair"
        bearer_token = "Bearer support-conflict-bearer-token"
        raw_purchase_token = "support-conflict-raw-purchase-token"
        password_material = "support-conflict-password-material"
        secret_material = "support-conflict-secret-material"
        conflict_provider_subject = "google-conflict-provider-subject"
        conflict_email = "pilot@example.com"
        legacy_user_id = self.seed_legacy_google_user(
            provider_subject=conflict_provider_subject,
            email=conflict_email,
            email_verified=True,
        )
        self.upsert_entitlement_snapshot(
            user_id=legacy_user_id,
            tier="SOARING",
            billing_period="MONTHLY",
            status="ACTIVE",
            product_id="xcpro_soaring",
            base_plan_id="monthly",
        )
        purchase_token_hash = main_module.hash_purchase_token(raw_purchase_token)
        now = self.clock.utcnow()
        db = self.session_local()
        try:
            db.add(
                main_module.BillingGooglePurchase(
                    id="support-conflict-purchase",
                    user_id=legacy_user_id,
                    package_name=main_module.XCPRO_RELEASE_PACKAGE_NAME,
                    product_id="xcpro_soaring",
                    base_plan_id="monthly",
                    purchase_token_hash=purchase_token_hash,
                    linked_purchase_token_hash=None,
                    google_subscription_state="ACTIVE",
                    xcpro_subscription_status="ACTIVE",
                    acknowledgement_state="ACKNOWLEDGED",
                    expiry_time_ms=1777777777000,
                    auto_renewing=True,
                    last_verified_at_ms=1777000000000,
                    created_at=now,
                    updated_at=now,
                )
            )
            db.add(
                main_module.BillingGoogleEvent(
                    id="support-conflict-event",
                    pubsub_message_id="support-conflict-message",
                    event_type="SUBSCRIPTION_NOTIFICATION_2",
                    package_name=main_module.XCPRO_RELEASE_PACKAGE_NAME,
                    product_id="xcpro_soaring",
                    purchase_token_hash=purchase_token_hash,
                    published_at=now,
                    processed_at=now,
                    processing_result="VERIFIED",
                    audit_id="support-conflict-audit",
                    created_at=now,
                    updated_at=now,
                )
            )
            db.add(
                main_module.BillingAuditRecord(
                    audit_id="support-conflict-audit",
                    user_id=legacy_user_id,
                    event_type="GOOGLE_PLAY_SYNC",
                    redacted_subject=f"purchase_token_sha256:{purchase_token_hash[:12]}",
                    purchase_token_hash=purchase_token_hash,
                    result="VERIFIED",
                    detail_json=json.dumps(
                        {
                            "packageName": main_module.XCPRO_RELEASE_PACKAGE_NAME,
                            "productId": "xcpro_soaring",
                            "basePlanId": "monthly",
                            "firebase_id_token": firebase_id_token,
                            "google_id_token": google_id_token,
                            "Authorization": bearer_token,
                            "purchaseToken": raw_purchase_token,
                            "password": password_material,
                            "secret": secret_material,
                        }
                    ),
                    created_at=now,
                )
            )
            db.commit()
        finally:
            db.close()
        self.verifier_results[firebase_id_token] = self.firebase_identity(
            email=f" {conflict_email} ",
            email_verified=True,
            provider_identities={
                "email": [conflict_email],
                "google.com": ["different-google-subject"],
            },
        )

        response = self.exchange(firebase_id_token)

        self.assertEqual(409, response.status_code)
        body = response.json()
        self.assertEqual(main_module.ErrorCode.AUTH_RECOVERY_REQUIRED, body["code"])
        detail = body["detail"]
        self.assertEqual("firebase", detail["provider"])
        self.assertEqual(conflict_email, detail["providerEmail"])
        self.assertEqual("google", detail["conflictProvider"])
        self.assertEqual(conflict_email, detail["conflictProviderEmail"])
        self.assertIs(True, detail["conflictProviderEmailVerified"])
        for forbidden_key in (
            "providerSubject",
            "userId",
            "conflictProviderSubject",
            "conflictUserId",
        ):
            with self.subTest(forbidden_key=forbidden_key):
                self.assertNotIn(forbidden_key, detail)
        body_text = json.dumps(body, default=str)
        for forbidden_value in (
            firebase_id_token,
            google_id_token,
            bearer_token,
            raw_purchase_token,
            password_material,
            secret_material,
            legacy_user_id,
            conflict_provider_subject,
        ):
            with self.subTest(forbidden_response_value=forbidden_value):
                self.assertNotIn(forbidden_value, body_text)

        before_counts = self.support_lookup_row_counts()
        email_result = support_billing_snapshot_script.run(
            support_billing_snapshot_script.parse_args(
                ["--email", detail["providerEmail"]]
            ),
            session_factory=self.session_local,
        )
        self.assertEqual(before_counts, self.support_lookup_row_counts())

        email_owner = email_result["snapshot"]["accountOwner"]
        self.assertTrue(email_result["ok"])
        self.assertEqual("email", email_result["lookup"]["kind"])
        self.assertEqual(conflict_email, email_result["lookup"]["email"])
        self.assertEqual(legacy_user_id, email_owner["userId"])
        self.assertEqual("google", email_owner["authProvider"])
        self.assertEqual(
            conflict_provider_subject,
            email_owner["authProviderSubject"],
        )
        self.assertEqual(conflict_email, email_owner["authProviderEmail"])
        self.assertIs(True, email_owner["authProviderEmailVerified"])
        self.assertEqual("SOARING", email_result["snapshot"]["entitlement"]["tier"])
        self.assertEqual(
            purchase_token_hash,
            email_result["snapshot"]["currentPurchase"]["purchaseTokenHash"],
        )
        self.assertEqual(
            purchase_token_hash,
            email_result["snapshot"]["latestGoogleEvent"]["purchaseTokenHash"],
        )
        self.assertEqual(
            purchase_token_hash,
            email_result["snapshot"]["latestAudit"]["purchaseTokenHash"],
        )

        provider_subject_result = support_billing_snapshot_script.run(
            support_billing_snapshot_script.parse_args(
                [
                    "--provider",
                    detail["conflictProvider"],
                    "--provider-subject",
                    email_owner["authProviderSubject"],
                ]
            ),
            session_factory=self.session_local,
        )

        self.assertEqual(before_counts, self.support_lookup_row_counts())
        self.assertTrue(provider_subject_result["ok"])
        self.assertEqual(
            "providerSubject",
            provider_subject_result["lookup"]["kind"],
        )
        self.assertEqual(
            detail["conflictProvider"],
            provider_subject_result["lookup"]["provider"],
        )
        self.assertEqual(
            conflict_provider_subject,
            provider_subject_result["lookup"]["providerSubject"],
        )
        self.assertEqual(
            email_result["snapshot"]["accountOwner"],
            provider_subject_result["snapshot"]["accountOwner"],
        )
        self.assertEqual(
            email_result["snapshot"]["entitlement"],
            provider_subject_result["snapshot"]["entitlement"],
        )
        self.assertEqual(
            email_result["snapshot"]["currentPurchase"],
            provider_subject_result["snapshot"]["currentPurchase"],
        )

        support_text = json.dumps(
            [email_result, provider_subject_result],
            default=str,
        )
        for forbidden_value in (
            firebase_id_token,
            google_id_token,
            bearer_token,
            raw_purchase_token,
            password_material,
            secret_material,
        ):
            with self.subTest(forbidden_support_value=forbidden_value):
                self.assertNotIn(forbidden_value, support_text)

    def test_recovery_required_detail_matches_conflicting_email_verified_state(self):
        cases = (
            ("nullable", None),
            ("verified", True),
            ("unverified", False),
        )
        for case_name, expected_verified in cases:
            with self.subTest(case_name=case_name):
                token = f"{case_name}-conflict-token"
                email = f"{case_name}@example.com"
                self.seed_legacy_google_user(
                    provider_subject=f"google-provider-subject-{case_name}",
                    email=email,
                    email_verified=expected_verified,
                )
                self.verifier_results[token] = self.firebase_identity(
                    firebase_uid=f"firebase-uid-{case_name}",
                    email=email,
                    email_verified=True,
                    provider_identities={
                        "email": [email],
                        "google.com": [f"different-google-subject-{case_name}"],
                    },
                )

                response = self.exchange(token)

                self.assertEqual(409, response.status_code)
                body = response.json()
                self.assertEqual(
                    main_module.ErrorCode.AUTH_RECOVERY_REQUIRED,
                    body["code"],
                )
                detail = body["detail"]
                self.assertEqual(email, detail["providerEmail"])
                self.assertEqual("google", detail["conflictProvider"])
                self.assertEqual(email, detail["conflictProviderEmail"])
                self.assertIs(
                    detail["conflictProviderEmailVerified"],
                    expected_verified,
                )

    def test_non_identity_state_cannot_authorize_email_conflict_linking(self):
        google_provider_subject = "google-provider-subject-1"
        legacy_user_id = self.seed_legacy_google_user(
            provider_subject=google_provider_subject,
            email="pilot@example.com",
        )
        self.upsert_entitlement_snapshot(
            user_id=legacy_user_id,
            tier="SOARING",
            billing_period="ANNUAL",
            status="ACTIVE",
            product_id="xcpro_soaring",
            base_plan_id="annual",
        )
        firebase_identity = self.firebase_identity(
            email="pilot@example.com",
            email_verified=True,
            provider_identities={
                "email": ["pilot@example.com"],
                "google.com": ["different-google-subject"],
            },
        )
        object.__setattr__(
            firebase_identity,
            "customClaims",
            {"entitlement": "SOARING", "paid": True, "LiveFollow": True},
        )
        object.__setattr__(
            firebase_identity,
            "custom_claims",
            {"tier": "SOARING", "subscription_state": "ACTIVE"},
        )
        object.__setattr__(
            firebase_identity,
            "local_state",
            {"last_user_id": legacy_user_id, "account_link": "google"},
        )
        object.__setattr__(
            firebase_identity,
            "android_local_state",
            {"last_user_id": legacy_user_id, "linked_provider": "google"},
        )
        object.__setattr__(
            firebase_identity,
            "billing_state",
            {"accountSubject": legacy_user_id, "tier": "SOARING"},
        )
        object.__setattr__(
            firebase_identity,
            "entitlement_state",
            {"accountSubject": legacy_user_id, "tier": "SOARING"},
        )
        object.__setattr__(
            firebase_identity,
            "live_follow_state",
            {"enabled": True, "viewer": legacy_user_id},
        )
        object.__setattr__(
            firebase_identity,
            "purchase_state",
            {"purchaseToken": "fixture-purchase-token-never-authority"},
        )
        self.verifier_results[self.firebase_id_token] = firebase_identity

        response = self.exchange(self.firebase_id_token)

        self.assertEqual(409, response.status_code)
        body = response.json()
        self.assertEqual(
            main_module.ErrorCode.AUTH_RECOVERY_REQUIRED,
            body["code"],
        )
        body_text = str(body)
        for forbidden_value in (
            self.firebase_id_token,
            "fixture-purchase-token-never-authority",
            legacy_user_id,
            google_provider_subject,
            "SOARING",
            "ANNUAL",
            "xcpro_soaring",
            "billing_state",
            "entitlement_state",
            "purchase_state",
            "LiveFollow",
            "live_follow_state",
            "android_local_state",
        ):
            with self.subTest(forbidden_value=forbidden_value):
                self.assertNotIn(forbidden_value, body_text)
        db = self.session_local()
        try:
            self.assertEqual(1, db.query(main_module.User).count())
            self.assertEqual(
                0,
                db.query(main_module.AuthIdentity)
                .filter(main_module.AuthIdentity.provider == "firebase")
                .count(),
            )
        finally:
            db.close()

    def test_non_identity_authority_claims_do_not_grant_access_or_appear_in_response(self):
        firebase_identity = self.firebase_identity()
        object.__setattr__(
            firebase_identity,
            "customClaims",
            {"entitlement": "SOARING", "paid": True, "LiveFollow": True},
        )
        object.__setattr__(
            firebase_identity,
            "custom_claims",
            {"tier": "SOARING", "subscription_state": "ACTIVE"},
        )
        self.verifier_results[self.firebase_id_token] = firebase_identity

        exchange_response = self.exchange(self.firebase_id_token)

        self.assertEqual(200, exchange_response.status_code)
        exchange_body = exchange_response.json()
        self.assert_firebase_exchange_response_has_bootstrap(exchange_body)
        self.assertEqual("FREE", exchange_body["entitlement"]["tier"])
        self.assertEqual(
            "FREE_CANONICAL",
            exchange_body["entitlement"]["verificationState"],
        )
        self.assertNotIn("SOARING", str(exchange_body))
        self.assertNotIn("customClaims", str(exchange_body))
        self.assertNotIn("custom_claims", str(exchange_body))

        entitlement_response = self.client.get(
            "/api/v1/subscriptions/entitlements",
            headers={
                **self.bearer_headers(exchange_body["access_token"]),
                "X-XCPro-Package-Name": main_module.XCPRO_RELEASE_PACKAGE_NAME,
            },
        )

        self.assertEqual(200, entitlement_response.status_code)
        entitlement = entitlement_response.json()["entitlement"]
        self.assertEqual("FREE", entitlement["tier"])
        self.assertEqual("FREE_CANONICAL", entitlement["verificationState"])

    def test_firebase_exchange_returns_current_server_stored_paid_entitlement_snapshot(self):
        self.verifier_results[self.firebase_id_token] = self.firebase_identity()
        first_response = self.exchange(self.firebase_id_token)
        self.assertEqual(200, first_response.status_code)
        user_id = first_response.json()["user_id"]
        valid_until_ms = 1780000000000
        self.upsert_entitlement_snapshot(
            user_id=user_id,
            tier="SOARING",
            billing_period="ANNUAL",
            status="ACTIVE",
            product_id="xcpro_soaring",
            base_plan_id="annual",
            expiry_time_ms=valid_until_ms,
            valid_until_ms=valid_until_ms,
        )

        second_response = self.exchange(self.firebase_id_token)

        self.assertEqual(200, second_response.status_code)
        body = second_response.json()
        self.assertEqual(user_id, body["user_id"])
        entitlement = body["entitlement"]
        self.assertEqual(user_id, entitlement["accountSubject"])
        self.assertEqual("SOARING", entitlement["tier"])
        self.assertEqual("ANNUAL", entitlement["billingPeriod"])
        self.assertEqual("ACTIVE", entitlement["status"])
        self.assertEqual("GOOGLE_PLAY", entitlement["source"])
        self.assertEqual("VERIFIED", entitlement["verificationState"])
        self.assertEqual("xcpro_soaring", entitlement["productId"])
        self.assertEqual("annual", entitlement["basePlanId"])
        self.assertEqual(valid_until_ms, entitlement["validUntilMs"])
        self.assertEqual(
            {
                "following_count": 0,
                "max_following": 15,
                "status": "under_limit",
            },
            body["profile"]["relationship_limits"],
        )

    def test_firebase_exchange_returns_denied_and_recovery_entitlements_without_valid_until(self):
        self.verifier_results[self.firebase_id_token] = self.firebase_identity()
        first_response = self.exchange(self.firebase_id_token)
        self.assertEqual(200, first_response.status_code)
        user_id = first_response.json()["user_id"]
        cases = [
            ("ON_HOLD", "VERIFIED", "OPEN_PLAY_SUBSCRIPTIONS"),
            ("RECOVERY_REQUIRED", "ACCOUNT_MISMATCH", "CHOOSE_CORRECT_ACCOUNT"),
        ]

        for status, verification_state, recovery_action in cases:
            with self.subTest(status=status):
                self.upsert_entitlement_snapshot(
                    user_id=user_id,
                    tier="SOARING",
                    billing_period="MONTHLY",
                    status=status,
                    verification_state=verification_state,
                    product_id="xcpro_soaring",
                    base_plan_id="monthly",
                    expiry_time_ms=1777777777000,
                    valid_until_ms=1777777777000,
                    recovery_action=recovery_action,
                )

                response = self.exchange(self.firebase_id_token)

                self.assertEqual(200, response.status_code)
                entitlement = response.json()["entitlement"]
                self.assertEqual("SOARING", entitlement["tier"])
                self.assertEqual(status, entitlement["status"])
                self.assertEqual(verification_state, entitlement["verificationState"])
                self.assertEqual(recovery_action, entitlement["recoveryAction"])
                self.assertIsNone(entitlement["validUntilMs"])

    def test_firebase_exchange_malformed_stored_entitlement_fails_closed(self):
        self.verifier_results[self.firebase_id_token] = self.firebase_identity()
        first_response = self.exchange(self.firebase_id_token)
        self.assertEqual(200, first_response.status_code)
        user_id = first_response.json()["user_id"]
        self.upsert_entitlement_snapshot(
            user_id=user_id,
            status="ALIEN_ACTIVE",
        )

        response = self.exchange(self.firebase_id_token)

        self.assertEqual(500, response.status_code)
        self.assertEqual(
            main_module.ErrorCode.ENTITLEMENT_STATE_INVALID,
            response.json()["code"],
        )

    def test_firebase_exchange_missing_or_invalid_package_header_fails_invalid_package(self):
        cases = [
            ("missing", None),
            ("invalid", "com.example.other"),
        ]

        for case_name, package_name in cases:
            with self.subTest(case_name=case_name):
                token = f"{case_name}-package-token"
                self.verifier_results[token] = self.firebase_identity(
                    firebase_uid=f"firebase-{case_name}-package"
                )

                response = self.exchange(token, package_name=package_name)

                self.assertEqual(400, response.status_code)
                self.assertEqual(
                    main_module.ErrorCode.INVALID_PACKAGE,
                    response.json()["code"],
                )

    def test_firebase_exchange_debug_package_requires_runtime_opt_in(self):
        self.override_private_follow_runtime_config(
            runtime_env=main_module.RUNTIME_ENV_PROD,
            allow_debug_entitlement_package=False,
        )
        denied_token = "debug-package-denied-token"
        self.verifier_results[denied_token] = self.firebase_identity(
            firebase_uid="firebase-debug-denied"
        )

        denied_response = self.exchange(
            denied_token,
            package_name=main_module.XCPRO_DEBUG_PACKAGE_NAME,
        )

        self.assertEqual(400, denied_response.status_code)
        self.assertEqual(
            main_module.ErrorCode.INVALID_PACKAGE,
            denied_response.json()["code"],
        )

        self.override_private_follow_runtime_config(
            runtime_env=main_module.RUNTIME_ENV_PROD,
            allow_debug_entitlement_package=True,
        )
        allowed_token = "debug-package-allowed-token"
        self.verifier_results[allowed_token] = self.firebase_identity(
            firebase_uid="firebase-debug-allowed"
        )

        allowed_response = self.exchange(
            allowed_token,
            package_name=main_module.XCPRO_DEBUG_PACKAGE_NAME,
        )

        self.assertEqual(200, allowed_response.status_code)
        self.assertEqual("FREE", allowed_response.json()["entitlement"]["tier"])

    def test_response_never_contains_raw_firebase_id_token(self):
        raw_token = "raw-firebase-token-never-returned"
        self.verifier_results[raw_token] = self.firebase_identity()

        response = self.exchange(raw_token)

        self.assertEqual(200, response.status_code)
        self.assertNotIn(raw_token, str(response.json()))

    def test_error_responses_never_contain_raw_firebase_id_token(self):
        cases = [
            (
                "raw-firebase-token-invalid-response",
                main_module.invalid_firebase_id_token_exception(),
                401,
                main_module.ErrorCode.INVALID_FIREBASE_ID_TOKEN,
            ),
            (
                "raw-firebase-token-unavailable-response",
                main_module.firebase_auth_unavailable_exception(
                    "Firebase Auth verifier is unavailable"
                ),
                503,
                main_module.ErrorCode.AUTH_UNAVAILABLE,
            ),
            (
                "raw-firebase-token-unverified-email-response",
                self.firebase_identity(
                    sign_in_provider="password",
                    email_verified=False,
                ),
                403,
                main_module.ErrorCode.EMAIL_VERIFICATION_REQUIRED,
            ),
        ]

        for raw_token, verifier_result, status_code, code in cases:
            with self.subTest(raw_token=raw_token):
                self.verifier_results[raw_token] = verifier_result

                response = self.exchange(raw_token)

                self.assertEqual(status_code, response.status_code)
                self.assertEqual(code, response.json()["code"])
                self.assertNotIn(raw_token, str(response.json()))

    def fake_firebase_id_token_verifier(self, token: str):
        self.verifier_calls.append(token)
        result = self.verifier_results.get(token)
        if isinstance(result, Exception):
            raise result
        if result is not None:
            return result
        return self.firebase_identity()

    def exchange(
        self,
        token: str,
        package_name: str | None = main_module.XCPRO_RELEASE_PACKAGE_NAME,
    ):
        headers = {}
        if package_name is not None:
            headers["X-XCPro-Package-Name"] = package_name
        return self.client.post(
            "/api/v2/auth/firebase/exchange",
            json={"firebase_id_token": token},
            headers=headers,
        )

    def bearer_headers(self, token: str):
        return {"Authorization": f"Bearer {token}"}

    def count_rows(self, model) -> int:
        db = self.session_local()
        try:
            return db.query(model).count()
        finally:
            db.close()

    def support_lookup_row_counts(self) -> dict[str, int]:
        return {
            "users": self.count_rows(main_module.User),
            "authIdentities": self.count_rows(main_module.AuthIdentity),
            "entitlements": self.count_rows(main_module.AccountEntitlementSnapshot),
            "purchases": self.count_rows(main_module.BillingGooglePurchase),
            "events": self.count_rows(main_module.BillingGoogleEvent),
            "audits": self.count_rows(main_module.BillingAuditRecord),
            "followRequests": self.count_rows(main_module.FollowRequest),
            "followEdges": self.count_rows(main_module.FollowEdge),
            "favoriteFollows": self.count_rows(main_module.FavoriteFollow),
            "relationshipCounters": self.count_rows(main_module.UserRelationshipCounter),
        }

    def upsert_entitlement_snapshot(
        self,
        user_id: str,
        tier: str = "SOARING",
        billing_period: str = "MONTHLY",
        status: str = "ACTIVE",
        source: str = "GOOGLE_PLAY",
        verification_state: str = "VERIFIED",
        product_id: str | None = "xcpro_soaring",
        base_plan_id: str | None = "monthly",
        expiry_time_ms: int | None = 1777777777000,
        auto_renewing: bool | None = True,
        will_lose_access_at_ms: int | None = None,
        verified_at_ms: int | None = 1777000000000,
        fetched_at_ms: int = 1777000000000,
        valid_until_ms: int | None = 1777777777000,
        stale_after_ms: int | None = None,
        hard_refresh_after_ms: int | None = None,
        recovery_action: str = "NONE",
    ):
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

    def override_private_follow_runtime_config(self, **overrides):
        main_module.PRIVATE_FOLLOW_RUNTIME_CONFIG = replace(
            main_module.PRIVATE_FOLLOW_RUNTIME_CONFIG,
            **overrides,
        )

    def assert_firebase_exchange_response_has_bootstrap(self, body):
        self.assertEqual(
            {
                "access_token",
                "token_type",
                "auth_method",
                "user_id",
                "expires_at",
                "profile",
                "entitlement",
            },
            set(body.keys()),
        )
        for authority_field in (
            "entitlements",
            "tier",
            "subscription",
            "subscriptions",
            "billing",
            "device",
            "devices",
            "liveFollow",
            "LiveFollow",
            "live_follow",
        ):
            with self.subTest(authority_field=authority_field):
                self.assertNotIn(authority_field, body)

    def firebase_identity(
        self,
        firebase_uid: str = "firebase-uid-1",
        email: str = "pilot@example.com",
        email_verified: bool = True,
        display_name: str = "Firebase Pilot",
        sign_in_provider: str = "google.com",
        provider_identities: dict[str, list[str]] | None = None,
    ) -> main_module.ResolvedFirebaseIdentity:
        if provider_identities is None:
            provider_identities = {
                "email": [email],
                "google.com": ["google-provider-subject-1"],
            }
        return main_module.ResolvedFirebaseIdentity(
            firebase_uid=firebase_uid,
            email=email,
            email_verified=email_verified,
            display_name=display_name,
            sign_in_provider=sign_in_provider,
            provider_identities=provider_identities,
        )

    def seed_legacy_google_user(
        self,
        provider_subject: str,
        email: str,
        email_verified: bool | None = None,
    ) -> str:
        identity = main_module.ResolvedBearerIdentity(
            provider="google",
            provider_subject=provider_subject,
            email=email,
            email_verified=email_verified,
            display_name="Legacy Google Pilot",
        )
        db = self.session_local()
        try:
            current_user = main_module.ensure_current_user_record_for_identity(
                db,
                identity,
            )
            return current_user.user.id
        finally:
            db.close()


if __name__ == "__main__":
    unittest.main()
