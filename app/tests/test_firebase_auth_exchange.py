import unittest
from dataclasses import replace

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


if __name__ == "__main__":
    unittest.main()
