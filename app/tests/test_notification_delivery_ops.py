import json
import unittest

from app import main as main_module
from app.scripts import deliver_notifications


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


if __name__ == "__main__":
    unittest.main()
