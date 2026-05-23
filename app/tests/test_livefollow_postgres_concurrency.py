"""
PostgreSQL-only LiveFollow following-limit concurrency proof.

Run manually with a disposable local PostgreSQL database:

    DATABASE_URL=postgresql://user:password@localhost:5432/xcpro_test \
        python -m unittest app.tests.test_livefollow_postgres_concurrency

The test creates a temporary schema, points the app SessionLocal at that schema,
and drops the schema at teardown. It skips when DATABASE_URL is absent or is not
a PostgreSQL URL. Do not point DATABASE_URL at production.
"""

import os
import threading
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from app import main as main_module


class FakeRedis:
    def __init__(self):
        self.values = {}

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value):
        self.values[key] = value


class LiveFollowPostgresConcurrencyTest(unittest.TestCase):
    def setUp(self):
        database_url = os.getenv("DATABASE_URL", "").strip()
        if not database_url:
            self.skipTest(
                "DATABASE_URL is required for PostgreSQL LiveFollow concurrency proof"
            )

        parsed_url = make_url(database_url)
        if parsed_url.get_backend_name() != "postgresql":
            self.skipTest(
                "DATABASE_URL must use PostgreSQL for LiveFollow concurrency proof"
            )

        self.schema_name = f"lf_concurrency_{uuid.uuid4().hex}"
        self.admin_engine = create_engine(database_url, isolation_level="AUTOCOMMIT")
        try:
            with self.admin_engine.connect() as connection:
                connection.execute(text(f'CREATE SCHEMA "{self.schema_name}"'))
        except OperationalError as exc:
            self.admin_engine.dispose()
            self.skipTest(f"PostgreSQL DATABASE_URL is not reachable: {exc}")

        try:
            self.engine = create_engine(
                database_url,
                connect_args={"options": f"-csearch_path={self.schema_name}"},
                pool_size=6,
                max_overflow=0,
                pool_pre_ping=True,
            )
            self.session_local = sessionmaker(bind=self.engine)
            main_module.Base.metadata.create_all(bind=self.engine)
        except Exception:
            if hasattr(self, "engine"):
                self.engine.dispose()
            with self.admin_engine.connect() as connection:
                connection.execute(
                    text(f'DROP SCHEMA IF EXISTS "{self.schema_name}" CASCADE')
                )
            self.admin_engine.dispose()
            raise

        self.original_session_local = main_module.SessionLocal
        self.original_redis_client = main_module.redis_client
        self.original_utcnow = main_module.utcnow
        self.original_static_bearer_tokens = main_module.STATIC_BEARER_TOKENS

        self.now = datetime(2026, 5, 21, 10, 0, 0)
        main_module.SessionLocal = self.session_local
        main_module.redis_client = FakeRedis()
        main_module.utcnow = lambda: self.now
        self.requester_token = "postgres-concurrency-requester"
        main_module.STATIC_BEARER_TOKENS = {
            self.requester_token: main_module.ResolvedBearerIdentity(
                provider="static",
                provider_subject="postgres-concurrency-requester",
                email="postgres-concurrency-requester@example.com",
                display_name="Postgres Requester",
            )
        }

    def tearDown(self):
        if hasattr(self, "original_session_local"):
            main_module.SessionLocal = self.original_session_local
            main_module.redis_client = self.original_redis_client
            main_module.utcnow = self.original_utcnow
            main_module.STATIC_BEARER_TOKENS = self.original_static_bearer_tokens

        if hasattr(self, "engine"):
            self.engine.dispose()
        if hasattr(self, "admin_engine") and hasattr(self, "schema_name"):
            try:
                with self.admin_engine.connect() as connection:
                    connection.execute(
                        text(f'DROP SCHEMA IF EXISTS "{self.schema_name}" CASCADE')
                    )
            finally:
                self.admin_engine.dispose()

    def test_two_concurrent_auto_approve_requests_at_cap_boundary_serialize_on_lock(self):
        with TestClient(main_module.app) as client:
            requester = self.complete_profile(
                client=client,
                token=self.requester_token,
                handle="pg.requester",
                display_name="Postgres Requester",
            )

        requester_user_id = requester["user_id"]
        self.seed_basic_entitlement(requester_user_id)
        for index in range(3):
            followed = self.seed_profile_user(
                handle=f"pg.seed.{index}",
                display_name=f"Seed Followed {index}",
            )
            self.seed_follow_edge(requester_user_id, followed["user_id"])

        first_target = self.seed_profile_user(
            handle="pg.target.one",
            display_name="Postgres Target One",
            follow_policy="auto_approve",
        )
        second_target = self.seed_profile_user(
            handle="pg.target.two",
            display_name="Postgres Target Two",
            follow_policy="auto_approve",
        )
        self.install_follow_edge_insert_sleep_trigger()

        start_barrier = threading.Barrier(3)
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(
                    self.create_follow_request,
                    start_barrier,
                    first_target["user_id"],
                ),
                executor.submit(
                    self.create_follow_request,
                    start_barrier,
                    second_target["user_id"],
                ),
            ]
            start_barrier.wait(timeout=10)
            results = [future.result(timeout=30) for future in futures]

        success_results = [result for result in results if result["status_code"] == 200]
        limit_results = [result for result in results if result["status_code"] == 409]

        self.assertEqual(1, len(success_results), results)
        self.assertEqual(1, len(limit_results), results)
        self.assertEqual(
            main_module.ErrorCode.LIVEFOLLOW_FOLLOWING_LIMIT_EXCEEDED,
            limit_results[0]["body"]["code"],
        )

        final_state = self.load_final_relationship_state(
            requester_user_id,
            [first_target["user_id"], second_target["user_id"]],
        )
        self.assertEqual(4, final_state["following_count"])
        self.assertEqual(1, final_state["accepted_request_count"])
        self.assertEqual(0, final_state["pending_request_count"])
        self.assertEqual(1, final_state["new_target_edge_count"])
        self.assertEqual(
            success_results[0]["body"]["request_id"],
            final_state["accepted_request_ids"][0],
        )
        print(
            "PostgreSQL LiveFollow concurrency proof final state: "
            f"following_count={final_state['following_count']}, "
            f"accepted_request_count={final_state['accepted_request_count']}, "
            f"pending_request_count={final_state['pending_request_count']}, "
            f"new_target_edge_count={final_state['new_target_edge_count']}"
        )

    def bearer_headers(self, token: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {token}"}

    def complete_profile(
        self,
        client: TestClient,
        token: str,
        handle: str,
        display_name: str,
    ) -> dict[str, str | None]:
        client.get("/api/v2/me", headers=self.bearer_headers(token))
        response = client.patch(
            "/api/v2/me/profile",
            json={
                "handle": handle,
                "display_name": display_name,
                "comp_number": None,
            },
            headers=self.bearer_headers(token),
        )
        self.assertEqual(200, response.status_code)
        return response.json()

    def seed_basic_entitlement(self, user_id: str) -> None:
        db = self.session_local()
        try:
            db.merge(
                main_module.AccountEntitlementSnapshot(
                    user_id=user_id,
                    tier="BASIC",
                    billing_period="MONTHLY",
                    status="ACTIVE",
                    source="GOOGLE_PLAY",
                    verification_state="VERIFIED",
                    product_id="xcpro_basic",
                    base_plan_id="monthly",
                    expiry_time_ms=1777777777000,
                    auto_renewing=True,
                    will_lose_access_at_ms=None,
                    verified_at_ms=1777000000000,
                    fetched_at_ms=1777000000000,
                    valid_until_ms=1777777777000,
                    stale_after_ms=None,
                    hard_refresh_after_ms=None,
                    recovery_action="NONE",
                    created_at=self.now,
                    updated_at=self.now,
                )
            )
            db.commit()
        finally:
            db.close()

    def seed_profile_user(
        self,
        handle: str,
        display_name: str,
        follow_policy: str = "approval_required",
    ) -> dict[str, str | None]:
        user_id = str(uuid.uuid4())
        db = self.session_local()
        try:
            db.add(
                main_module.User(
                    id=user_id,
                    created_at=self.now,
                    updated_at=self.now,
                )
            )
            db.add(
                main_module.PilotProfile(
                    user_id=user_id,
                    handle=handle,
                    handle_normalized=handle.lower(),
                    display_name=display_name,
                    comp_number=None,
                    created_at=self.now,
                    updated_at=self.now,
                )
            )
            db.add(
                main_module.PrivacySetting(
                    user_id=user_id,
                    discoverability="searchable",
                    follow_policy=follow_policy,
                    default_live_visibility="followers",
                    connection_list_visibility="owner_only",
                    created_at=self.now,
                    updated_at=self.now,
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
        db = self.session_local()
        try:
            db.add(
                main_module.FollowEdge(
                    follower_user_id=follower_user_id,
                    followed_user_id=followed_user_id,
                    created_at=self.now,
                    updated_at=self.now,
                )
            )
            db.commit()
        finally:
            db.close()

    def install_follow_edge_insert_sleep_trigger(self) -> None:
        try:
            with self.engine.begin() as connection:
                connection.execute(
                    text(
                        """
                        CREATE FUNCTION test_sleep_before_follow_edge_insert()
                        RETURNS trigger AS $$
                        BEGIN
                            PERFORM pg_sleep(0.25);
                            RETURN NEW;
                        END;
                        $$ LANGUAGE plpgsql
                        """
                    )
                )
                connection.execute(
                    text(
                        """
                        CREATE TRIGGER test_sleep_before_follow_edge_insert
                        BEFORE INSERT ON follow_edges
                        FOR EACH ROW
                        EXECUTE FUNCTION test_sleep_before_follow_edge_insert()
                        """
                    )
                )
        except SQLAlchemyError as exc:
            self.fail(f"failed to install PostgreSQL sleep trigger: {exc}")

    def create_follow_request(
        self,
        start_barrier: threading.Barrier,
        target_user_id: str,
    ) -> dict[str, object]:
        start_barrier.wait(timeout=10)
        with TestClient(main_module.app) as client:
            response = client.post(
                "/api/v2/follow-requests",
                json={"target_user_id": target_user_id},
                headers=self.bearer_headers(self.requester_token),
            )
            return {
                "status_code": response.status_code,
                "body": response.json(),
            }

    def load_final_relationship_state(
        self,
        requester_user_id: str,
        target_user_ids: list[str],
    ) -> dict[str, object]:
        db = self.session_local()
        try:
            following_count = (
                db.query(main_module.FollowEdge)
                .filter(main_module.FollowEdge.follower_user_id == requester_user_id)
                .count()
            )
            follow_requests = (
                db.query(main_module.FollowRequest)
                .filter(
                    main_module.FollowRequest.requester_user_id == requester_user_id,
                    main_module.FollowRequest.target_user_id.in_(target_user_ids),
                )
                .all()
            )
            new_target_edge_count = (
                db.query(main_module.FollowEdge)
                .filter(
                    main_module.FollowEdge.follower_user_id == requester_user_id,
                    main_module.FollowEdge.followed_user_id.in_(target_user_ids),
                )
                .count()
            )
            accepted_request_ids = sorted(
                request.id
                for request in follow_requests
                if request.status == main_module.FOLLOW_REQUEST_STATUS_ACCEPTED
            )
            return {
                "following_count": following_count,
                "accepted_request_count": len(accepted_request_ids),
                "accepted_request_ids": accepted_request_ids,
                "pending_request_count": sum(
                    1
                    for request in follow_requests
                    if request.status == main_module.FOLLOW_REQUEST_STATUS_PENDING
                ),
                "new_target_edge_count": new_target_edge_count,
            }
        finally:
            db.close()


if __name__ == "__main__":
    unittest.main()
