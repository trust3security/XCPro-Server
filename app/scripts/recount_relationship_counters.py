import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


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


class RelationshipCounterRecountScriptError(ValueError):
    pass


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Recount cached XCPro follower/following counters."
    )
    parser.add_argument("--confirm", action="store_true")
    return parser.parse_args(argv)


def run(
    args: argparse.Namespace,
    session_factory: Callable[[], Any] | None = None,
    updated_at: datetime | None = None,
) -> dict[str, int | bool]:
    if not args.confirm:
        raise RelationshipCounterRecountScriptError("--confirm is required.")

    db = (session_factory or main_module.SessionLocal)()
    try:
        recounted_users = main_module.recount_all_user_relationship_counters(
            db,
            updated_at or datetime.now(timezone.utc),
        )
        db.commit()
        return {
            "ok": True,
            "recounted_users": recounted_users,
        }
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def main(argv: list[str] | None = None) -> int:
    try:
        result = run(parse_args(argv))
    except RelationshipCounterRecountScriptError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, indent=2, sort_keys=True))
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
