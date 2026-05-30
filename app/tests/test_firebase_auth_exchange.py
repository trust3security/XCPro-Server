import unittest
from dataclasses import replace
from datetime import datetime, timedelta

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app import main as main_module


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

    def test_non_identity_authority_claims_are_not_exposed(self):
        decoded_token = self.decoded_token()
        decoded_token["tier"] = "PRO"
        decoded_token["subscription_state"] = "ACTIVE"
        decoded_token["device_limit"] = 99
        decoded_token["live_follow_enabled"] = True
        self.install_firebase_fakes(decoded_token=decoded_token)

        identity = main_module.verify_firebase_id_token_for_exchange("valid-token")

        self.assertEqual("firebase-uid-1", identity.firebase_uid)
        self.assertFalse(hasattr(identity, "tier"))
        self.assertFalse(hasattr(identity, "subscription_state"))
        self.assertFalse(hasattr(identity, "device_limit"))
        self.assertFalse(hasattr(identity, "live_follow_enabled"))

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
        main_module.Base.metadata.drop_all(bind=self.engine)
        self.engine.dispose()

    def test_required_endpoint_model_and_error_symbols_are_available(self):
        self.assertTrue(hasattr(main_module, "FirebaseAuthExchangeRequest"))
        self.assertEqual(
            "email_verification_required",
            main_module.ErrorCode.EMAIL_VERIFICATION_REQUIRED,
        )
        self.assertEqual(
            "auth_recovery_required",
            main_module.ErrorCode.AUTH_RECOVERY_REQUIRED,
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
        self.assertEqual(
            {
                "access_token",
                "token_type",
                "auth_method",
                "user_id",
                "expires_at",
            },
            set(body.keys()),
        )
        self.assertEqual("Bearer", body["token_type"])
        self.assertEqual("firebase", body["auth_method"])
        self.assertNotIn(self.firebase_id_token, str(body))

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
        finally:
            db.close()

    def test_existing_firebase_identity_reuses_user_and_updates_identity(self):
        self.verifier_results[self.firebase_id_token] = self.firebase_identity(
            email="first@example.com",
        )
        first_response = self.exchange(self.firebase_id_token)
        self.assertEqual(200, first_response.status_code)
        user_id = first_response.json()["user_id"]

        self.clock.advance(minutes=5)
        self.verifier_results[self.firebase_id_token] = self.firebase_identity(
            email="updated@example.com",
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

    def test_verified_email_only_conflict_requires_recovery_without_duplicate_user(self):
        self.seed_legacy_google_user(
            provider_subject="google-provider-subject-1",
            email="pilot@example.com",
        )
        self.verifier_results[self.firebase_id_token] = self.firebase_identity(
            email="pilot@example.com",
            email_verified=True,
            provider_identities={"email": ["pilot@example.com"]},
        )

        response = self.exchange(self.firebase_id_token)

        self.assertEqual(409, response.status_code)
        self.assertEqual(
            main_module.ErrorCode.AUTH_RECOVERY_REQUIRED,
            response.json()["code"],
        )
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
        self.verifier_results[self.firebase_id_token] = self.firebase_identity()

        exchange_response = self.exchange(self.firebase_id_token)

        self.assertEqual(200, exchange_response.status_code)
        exchange_body = exchange_response.json()
        self.assertNotIn("entitlement", exchange_body)
        self.assertNotIn("tier", exchange_body)

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

    def test_response_never_contains_raw_firebase_id_token(self):
        raw_token = "raw-firebase-token-never-returned"
        self.verifier_results[raw_token] = self.firebase_identity()

        response = self.exchange(raw_token)

        self.assertEqual(200, response.status_code)
        self.assertNotIn(raw_token, str(response.json()))

    def fake_firebase_id_token_verifier(self, token: str):
        self.verifier_calls.append(token)
        result = self.verifier_results.get(token)
        if isinstance(result, Exception):
            raise result
        if result is not None:
            return result
        return self.firebase_identity()

    def exchange(self, token: str):
        return self.client.post(
            "/api/v2/auth/firebase/exchange",
            json={"firebase_id_token": token},
        )

    def bearer_headers(self, token: str):
        return {"Authorization": f"Bearer {token}"}

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

    def seed_legacy_google_user(self, provider_subject: str, email: str) -> str:
        identity = main_module.ResolvedBearerIdentity(
            provider="google",
            provider_subject=provider_subject,
            email=email,
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
