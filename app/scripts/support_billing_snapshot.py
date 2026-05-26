import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Callable

from sqlalchemy import func


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


SHA256_HEX_PATTERN = re.compile(r"^[0-9a-fA-F]{64}$")


class SupportBillingSnapshotScriptError(ValueError):
    pass


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Print a redacted XCPro billing support snapshot."
    )
    identity = parser.add_mutually_exclusive_group(required=True)
    identity.add_argument("--user-id")
    identity.add_argument("--email")
    identity.add_argument("--provider-subject")
    identity.add_argument("--purchase-token-hash")
    identity.add_argument("--purchase-token", dest="raw_purchase_token", help=argparse.SUPPRESS)
    parser.add_argument("--provider", default="google")
    return parser.parse_args(argv)


def run(
    args: argparse.Namespace,
    session_factory: Callable[[], Any] | None = None,
) -> dict[str, Any]:
    if getattr(args, "raw_purchase_token", None) is not None:
        raise SupportBillingSnapshotScriptError(
            "--purchase-token is not supported; use --purchase-token-hash."
        )

    db = (session_factory or main_module.SessionLocal)()
    try:
        user_id = resolve_user_id(db, args)
        return {
            "ok": True,
            "lookup": lookup_summary(args),
            "snapshot": main_module.build_billing_support_snapshot(db, user_id),
        }
    finally:
        db.close()


def resolve_user_id(db, args: argparse.Namespace) -> str:
    if args.user_id:
        user = db.query(main_module.User).filter(main_module.User.id == args.user_id).first()
        if user is None:
            raise SupportBillingSnapshotScriptError("No user found for --user-id.")
        return user.id

    if args.email:
        email = args.email.strip().lower()
        matches = (
            db.query(main_module.AuthIdentity)
            .filter(func.lower(main_module.AuthIdentity.provider_email) == email)
            .all()
        )
        return resolve_single_identity_user_id(matches)

    if args.provider_subject:
        matches = (
            db.query(main_module.AuthIdentity)
            .filter(
                main_module.AuthIdentity.provider == args.provider,
                main_module.AuthIdentity.provider_subject == args.provider_subject,
            )
            .all()
        )
        return resolve_single_identity_user_id(matches)

    if args.purchase_token_hash:
        token_hash = args.purchase_token_hash.strip().lower()
        if not SHA256_HEX_PATTERN.fullmatch(token_hash):
            raise SupportBillingSnapshotScriptError(
                "--purchase-token-hash must be a SHA-256 hex digest."
            )
        purchase = (
            db.query(main_module.BillingGooglePurchase)
            .filter(main_module.BillingGooglePurchase.purchase_token_hash == token_hash)
            .first()
        )
        if purchase is None:
            raise SupportBillingSnapshotScriptError("No purchase found for --purchase-token-hash.")
        return purchase.user_id

    raise SupportBillingSnapshotScriptError("A lookup argument is required.")


def resolve_single_identity_user_id(matches) -> str:
    user_ids = {match.user_id for match in matches}
    if len(user_ids) != 1:
        raise SupportBillingSnapshotScriptError(
            "Expected exactly one matching XCPro account identity."
        )
    return next(iter(user_ids))


def lookup_summary(args: argparse.Namespace) -> dict[str, Any]:
    if args.user_id:
        return {"kind": "userId", "userId": args.user_id}
    if args.email:
        return {"kind": "email", "email": args.email.strip().lower()}
    if args.provider_subject:
        return {
            "kind": "providerSubject",
            "provider": args.provider,
            "providerSubject": args.provider_subject,
        }
    if args.purchase_token_hash:
        return {
            "kind": "purchaseTokenHash",
            "purchaseTokenHash": args.purchase_token_hash.strip().lower(),
        }
    return {"kind": "unknown"}


def main(argv: list[str] | None = None) -> int:
    try:
        result = run(parse_args(argv))
    except SupportBillingSnapshotScriptError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, indent=2, sort_keys=True))
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
