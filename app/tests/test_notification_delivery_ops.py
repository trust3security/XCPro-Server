import json
import unittest
from datetime import datetime, timezone

from app import main as main_module
from app.scripts import deliver_notifications
from app.scripts import recount_relationship_counters


class NotificationDeliveryOperationsTest(unittest.TestCase):
    def test_build_fcm_data_payload_supports_all_private_follow_event_types(self):
        event_types = [
            main_module.NOTIFICATION_EVENT_FOLLOW_REQUEST_RECEIVED,
            main_module.NOTIFICATION_EVENT_FOLLOW_REQUEST_ACCEPTED,
            main_module.NOTIFICATION_EVENT_FOLLOW_NEW_FOLLOWER,
            main_module.NOTIFICATION_EVENT_FOLLOW_MUTUAL,
        ]

        for event_type in event_types:
            with self.subTest(event_type=event_type):
                payload = {
                    "event_type": event_type,
                    "recipient_user_id": "recipient-user-id",
                    "actor_user_id": "actor-user-id",
                    "follow_request_id": "follow-request-id",
                }
                event = main_module.NotificationOutboxEvent(
                    event_type=event_type,
                    recipient_user_id=payload["recipient_user_id"],
                    actor_user_id=payload["actor_user_id"],
                    follow_request_id=payload["follow_request_id"],
                    payload_json=json.dumps(payload, sort_keys=True),
                )

                self.assertEqual(payload, main_module.build_fcm_data_payload(event))

    def test_load_fcm_runtime_config_uses_xcpro_env(self):
        config = main_module.load_fcm_runtime_config({
            "XCPRO_FCM_PROJECT_ID": "xcpro-firebase",
            "XCPRO_FCM_SERVICE_ACCOUNT_JSON_PATH": "/run/secrets/fcm.json",
            "FIREBASE_PROJECT_ID": "fallback-firebase",
            "GOOGLE_APPLICATION_CREDENTIALS": "/fallback/credentials.json",
        })

        self.assertEqual("xcpro-firebase", config.project_id)
        self.assertEqual("/run/secrets/fcm.json", config.service_account_json_path)

    def test_load_fcm_runtime_config_supports_standard_google_fallbacks(self):
        config = main_module.load_fcm_runtime_config({
            "FIREBASE_PROJECT_ID": "fallback-firebase",
            "GOOGLE_APPLICATION_CREDENTIALS": "/fallback/credentials.json",
        })

        self.assertEqual("fallback-firebase", config.project_id)
        self.assertEqual("/fallback/credentials.json", config.service_account_json_path)

    def test_delivery_script_requires_explicit_confirm_send(self):
        with self.assertRaises(deliver_notifications.NotificationDeliveryScriptError):
            deliver_notifications.run(deliver_notifications.parse_args([]))

    def test_delivery_limits_are_named_defaults_and_clamped(self):
        args = deliver_notifications.parse_args(["--confirm-send"])

        self.assertEqual(main_module.NOTIFICATION_DELIVERY_DEFAULT_LIMIT, args.limit)
        self.assertEqual(1, main_module.normalize_notification_delivery_limit(-5))
        self.assertEqual(
            main_module.NOTIFICATION_DELIVERY_MAX_LIMIT,
            main_module.normalize_notification_delivery_limit(
                main_module.NOTIFICATION_DELIVERY_MAX_LIMIT + 1
            )
        )

    def test_delivery_script_delegates_confirmed_aggregate_delivery(self):
        original_deliver = deliver_notifications.main_module.deliver_pending_notification_events
        calls = []

        def fake_deliver_pending_notification_events(limit, sender=None):
            calls.append((limit, sender))
            return {
                "events_attempted": 1,
                "events_sent": 1,
                "events_retryable_failed": 0,
                "events_failed": 0,
                "token_attempts": 1,
                "tokens_sent": 1,
            }

        deliver_notifications.main_module.deliver_pending_notification_events = (
            fake_deliver_pending_notification_events
        )
        try:
            result = deliver_notifications.run(
                deliver_notifications.parse_args(["--confirm-send", "--limit", "25"])
            )
        finally:
            deliver_notifications.main_module.deliver_pending_notification_events = (
                original_deliver
            )

        self.assertEqual([(25, None)], calls)
        self.assertEqual(True, result["ok"])
        self.assertEqual(1, result["events_attempted"])
        self.assertEqual(1, result["tokens_sent"])

    def test_recount_script_requires_explicit_confirm(self):
        with self.assertRaises(
            recount_relationship_counters.RelationshipCounterRecountScriptError
        ):
            recount_relationship_counters.run(
                recount_relationship_counters.parse_args([])
            )

    def test_recount_script_commits_confirmed_aggregate_recount(self):
        original_recount = (
            recount_relationship_counters.main_module
            .recount_all_user_relationship_counters
        )
        calls = []
        db = FakeDbSession()
        updated_at = datetime(2026, 5, 25, tzinfo=timezone.utc)

        def fake_recount(session, recounted_at):
            calls.append((session, recounted_at))
            return 3

        recount_relationship_counters.main_module.recount_all_user_relationship_counters = (
            fake_recount
        )
        try:
            result = recount_relationship_counters.run(
                recount_relationship_counters.parse_args(["--confirm"]),
                session_factory=lambda: db,
                updated_at=updated_at,
            )
        finally:
            recount_relationship_counters.main_module.recount_all_user_relationship_counters = (
                original_recount
            )

        self.assertEqual([(db, updated_at)], calls)
        self.assertEqual({"ok": True, "recounted_users": 3}, result)
        self.assertEqual(1, db.commit_count)
        self.assertEqual(0, db.rollback_count)
        self.assertEqual(1, db.close_count)

    def test_recount_script_rolls_back_on_failure(self):
        original_recount = (
            recount_relationship_counters.main_module
            .recount_all_user_relationship_counters
        )
        db = FakeDbSession()

        def failing_recount(_session, _recounted_at):
            raise RuntimeError("recount failed")

        recount_relationship_counters.main_module.recount_all_user_relationship_counters = (
            failing_recount
        )
        try:
            with self.assertRaises(RuntimeError):
                recount_relationship_counters.run(
                    recount_relationship_counters.parse_args(["--confirm"]),
                    session_factory=lambda: db,
                )
        finally:
            recount_relationship_counters.main_module.recount_all_user_relationship_counters = (
                original_recount
            )

        self.assertEqual(0, db.commit_count)
        self.assertEqual(1, db.rollback_count)
        self.assertEqual(1, db.close_count)


class FakeDbSession:
    def __init__(self):
        self.commit_count = 0
        self.rollback_count = 0
        self.close_count = 0

    def commit(self):
        self.commit_count += 1

    def rollback(self):
        self.rollback_count += 1

    def close(self):
        self.close_count += 1


if __name__ == "__main__":
    unittest.main()
