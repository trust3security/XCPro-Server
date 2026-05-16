import unittest
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
        main_module.SessionLocal = self.session_local
        main_module.utcnow = lambda: datetime(2026, 5, 4, 12, 0, 0)

    def tearDown(self):
        main_module.SessionLocal = self.original_session_local
        main_module.utcnow = self.original_utcnow
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
            self.assertEqual("monthly", snapshot.base_plan_id)
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
