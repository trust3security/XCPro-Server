import unittest
import json
from dataclasses import replace
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app import main as main_module
from app.scripts import seed_test_entitlement


class SeedTestEntitlementScriptTest(unittest.TestCase):
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
        self.original_private_follow_runtime_config = (
            main_module.PRIVATE_FOLLOW_RUNTIME_CONFIG
        )
        main_module.SessionLocal = self.session_local
        main_module.utcnow = lambda: datetime(2026, 5, 4, 12, 0, 0)
        self.set_runtime_env(main_module.RUNTIME_ENV_DEV)

    def tearDown(self):
        main_module.SessionLocal = self.original_session_local
        main_module.utcnow = self.original_utcnow
        main_module.PRIVATE_FOLLOW_RUNTIME_CONFIG = (
            self.original_private_follow_runtime_config
        )
        main_module.Base.metadata.drop_all(bind=self.engine)
        self.engine.dispose()

    def test_seed_by_email_creates_paid_soaring_snapshot(self):
        self.create_user(email="pilot@example.com")

        result = seed_test_entitlement.run(
            seed_test_entitlement.parse_args(
                [
                    "--email",
                    "pilot@example.com",
                    "--tier",
                    "SOARING",
                    "--period",
                    "MONTHLY",
                    "--confirm-manual-test",
                ]
            )
        )

        self.assertTrue(result["ok"])
        self.assertEqual("SOARING", result["tier"])
        db = self.session_local()
        try:
            snapshot = db.query(main_module.AccountEntitlementSnapshot).one()
            self.assertEqual("SOARING", snapshot.tier)
            self.assertEqual("MONTHLY", snapshot.billing_period)
            self.assertEqual("ACTIVE", snapshot.status)
            self.assertEqual("GOOGLE_PLAY", snapshot.source)
            self.assertEqual("VERIFIED", snapshot.verification_state)
            self.assertEqual("xcpro_soaring", snapshot.product_id)
            self.assertEqual("monthly-auto2", snapshot.base_plan_id)
            self.assertIsNotNone(snapshot.valid_until_ms)
        finally:
            db.close()

    def test_clear_removes_existing_snapshot(self):
        self.create_user(email="pilot@example.com")
        seed_test_entitlement.run(
            seed_test_entitlement.parse_args(
                [
                    "--email",
                    "pilot@example.com",
                    "--confirm-manual-test",
                ]
            )
        )

        result = seed_test_entitlement.run(
            seed_test_entitlement.parse_args(
                [
                    "--email",
                    "pilot@example.com",
                    "--clear",
                    "--confirm-manual-test",
                ]
            )
        )

        self.assertTrue(result["ok"])
        self.assertTrue(result["snapshotExisted"])
        db = self.session_local()
        try:
            self.assertEqual(0, db.query(main_module.AccountEntitlementSnapshot).count())
        finally:
            db.close()

    def test_seed_requires_explicit_confirmation(self):
        self.create_user(email="pilot@example.com")

        with self.assertRaises(seed_test_entitlement.ManualSeedError):
            seed_test_entitlement.run(
                seed_test_entitlement.parse_args(["--email", "pilot@example.com"])
            )

    def test_production_seed_requires_explicit_operator_repair_context(self):
        self.create_user(email="pilot@example.com")
        self.set_runtime_env(main_module.RUNTIME_ENV_PROD)

        with self.assertRaises(seed_test_entitlement.ManualSeedError):
            seed_test_entitlement.run(
                seed_test_entitlement.parse_args(
                    [
                        "--email",
                        "pilot@example.com",
                        "--confirm-manual-test",
                    ]
                )
            )

        db = self.session_local()
        try:
            self.assertEqual(0, db.query(main_module.AccountEntitlementSnapshot).count())
            self.assertEqual(0, db.query(main_module.BillingAuditRecord).count())
        finally:
            db.close()

    def test_production_dry_run_is_read_only_and_does_not_audit(self):
        user_id = self.create_user(email="pilot@example.com")
        self.set_runtime_env(main_module.RUNTIME_ENV_PROD)

        result = seed_test_entitlement.run(
            seed_test_entitlement.parse_args(
                [
                    "--email",
                    "pilot@example.com",
                    "--dry-run",
                    "--confirm-manual-test",
                ]
            )
        )

        self.assertTrue(result["ok"])
        self.assertTrue(result["dryRun"])
        self.assertNotIn("userId", result)
        self.assertEqual(f"user:{user_id[:8]}", result["userRef"])
        db = self.session_local()
        try:
            self.assertEqual(0, db.query(main_module.AccountEntitlementSnapshot).count())
            self.assertEqual(0, db.query(main_module.BillingAuditRecord).count())
        finally:
            db.close()

    def test_production_clear_dry_run_redacts_user_and_does_not_audit(self):
        user_id = self.create_user(email="pilot@example.com")
        seed_test_entitlement.run(
            seed_test_entitlement.parse_args(
                [
                    "--email",
                    "pilot@example.com",
                    "--confirm-manual-test",
                ]
            )
        )
        self.set_runtime_env(main_module.RUNTIME_ENV_PROD)

        result = seed_test_entitlement.run(
            seed_test_entitlement.parse_args(
                [
                    "--email",
                    "pilot@example.com",
                    "--clear",
                    "--dry-run",
                    "--confirm-manual-test",
                ]
            )
        )

        self.assertTrue(result["ok"])
        self.assertTrue(result["dryRun"])
        self.assertTrue(result["snapshotExisted"])
        self.assertNotIn("userId", result)
        self.assertEqual(f"user:{user_id[:8]}", result["userRef"])
        db = self.session_local()
        try:
            self.assertEqual(1, db.query(main_module.AccountEntitlementSnapshot).count())
            self.assertEqual(0, db.query(main_module.BillingAuditRecord).count())
        finally:
            db.close()

    def test_production_seed_writes_redacted_operator_audit(self):
        user_id = self.create_user(email="pilot@example.com")
        self.set_runtime_env(main_module.RUNTIME_ENV_PROD)

        result = seed_test_entitlement.run(
            seed_test_entitlement.parse_args(
                [
                    "--email",
                    "pilot@example.com",
                    "--tier",
                    "PRO",
                    "--period",
                    "MONTHLY",
                    "--confirm-manual-test",
                    "--confirm-production-repair",
                    "--operator-id",
                    "support.ops",
                    "--support-ticket",
                    "SUP-12345",
                ]
            )
        )

        self.assertTrue(result["ok"])
        self.assertEqual("PRO", result["tier"])
        self.assertNotIn("userId", result)
        self.assertEqual(f"user:{user_id[:8]}", result["userRef"])
        self.assertIn("auditId", result)
        db = self.session_local()
        try:
            snapshot = db.query(main_module.AccountEntitlementSnapshot).one()
            self.assertEqual("PRO", snapshot.tier)
            self.assertEqual("ACTIVE", snapshot.status)
            audit = db.query(main_module.BillingAuditRecord).one()
            self.assertEqual(result["auditId"], audit.audit_id)
            self.assertEqual(user_id, audit.user_id)
            self.assertEqual(
                seed_test_entitlement.PRODUCTION_REPAIR_EVENT_TYPE,
                audit.event_type,
            )
            self.assertEqual(
                seed_test_entitlement.PRODUCTION_SEED_RESULT,
                audit.result,
            )
            self.assertIsNone(audit.purchase_token_hash)
            detail = json.loads(audit.detail_json)
            self.assertEqual("OPERATOR_ENTITLEMENT_REPAIR", detail["source"])
            self.assertEqual("seed", detail["action"])
            self.assertEqual("support.ops", detail["operatorId"])
            self.assertEqual("SUP-12345", detail["supportTicket"])
            self.assertEqual(
                main_module.XCPRO_RELEASE_PACKAGE_NAME,
                detail["packageName"],
            )
            self.assertEqual("xcpro_pro", detail["productId"])
            self.assertEqual("monthly-auto2", detail["basePlanId"])
            self.assertEqual("ACTIVE", detail["subscriptionStatus"])
            self.assertNotIn("pilot@example.com", audit.detail_json)
        finally:
            db.close()

    def test_production_clear_writes_operator_audit(self):
        self.create_user(email="pilot@example.com")
        seed_test_entitlement.run(
            seed_test_entitlement.parse_args(
                [
                    "--email",
                    "pilot@example.com",
                    "--confirm-manual-test",
                ]
            )
        )
        self.set_runtime_env(main_module.RUNTIME_ENV_PROD)

        result = seed_test_entitlement.run(
            seed_test_entitlement.parse_args(
                [
                    "--email",
                    "pilot@example.com",
                    "--clear",
                    "--confirm-manual-test",
                    "--confirm-production-repair",
                    "--operator-id",
                    "support.ops",
                    "--support-ticket",
                    "SUP-12345",
                ]
            )
        )

        self.assertTrue(result["ok"])
        self.assertTrue(result["snapshotExisted"])
        self.assertIn("auditId", result)
        db = self.session_local()
        try:
            self.assertEqual(0, db.query(main_module.AccountEntitlementSnapshot).count())
            audit = db.query(main_module.BillingAuditRecord).one()
            self.assertEqual(
                seed_test_entitlement.PRODUCTION_CLEAR_RESULT,
                audit.result,
            )
            detail = json.loads(audit.detail_json)
            self.assertEqual("clear", detail["action"])
            self.assertNotIn("productId", detail)
            self.assertNotIn("pilot@example.com", audit.detail_json)
        finally:
            db.close()

    def set_runtime_env(self, runtime_env: str):
        main_module.PRIVATE_FOLLOW_RUNTIME_CONFIG = replace(
            main_module.PRIVATE_FOLLOW_RUNTIME_CONFIG,
            runtime_env=runtime_env,
        )

    def create_user(self, email: str):
        db = self.session_local()
        try:
            current_user = main_module.ensure_current_user_record_for_identity(
                db,
                main_module.ResolvedBearerIdentity(
                    provider="google",
                    provider_subject=f"subject-{email}",
                    email=email,
                    display_name="Seed Test Pilot",
                ),
            )
            db.commit()
            return current_user.user.id
        finally:
            db.close()


if __name__ == "__main__":
    unittest.main()
