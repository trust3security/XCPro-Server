import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> int:
    try:
        from app import main as main_module
    except RuntimeError as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "errors": [str(exc)],
                    "warnings": [],
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 1

    report = main_module.build_private_follow_preflight_report()
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
