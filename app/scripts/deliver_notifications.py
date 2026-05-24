import argparse
import json
import sys
from pathlib import Path
from typing import Any


APP_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = APP_ROOT.parent
for path in (REPO_ROOT, APP_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

if "app.main" in sys.modules:
    main_module = sys.modules["app.main"]  # noqa: E402
else:
    try:
        from app import main as main_module  # noqa: E402
    except ModuleNotFoundError:
        import main as main_module  # noqa: E402


class NotificationDeliveryScriptError(ValueError):
    pass


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Deliver pending XCPro private-follow notification outbox events."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=main_module.NOTIFICATION_DELIVERY_DEFAULT_LIMIT,
    )
    parser.add_argument("--confirm-send", action="store_true")
    return parser.parse_args(argv)


def run(args: argparse.Namespace, sender: Any | None = None) -> dict[str, int | bool]:
    if not args.confirm_send:
        raise NotificationDeliveryScriptError("--confirm-send is required.")
    summary = main_module.deliver_pending_notification_events(
        limit=args.limit,
        sender=sender,
    )
    return {
        "ok": True,
        **summary,
    }


def main(argv: list[str] | None = None) -> int:
    try:
        result = run(parse_args(argv))
    except NotificationDeliveryScriptError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, indent=2, sort_keys=True))
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
