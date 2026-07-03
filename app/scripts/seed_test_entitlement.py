import argparse
import json
import re
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any

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


class ManualSeedError(ValueError):
    pass


OPERATOR_ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]{3,64}$")
SUPPORT_TICKET_PATTERN = re.compile(r"^[A-Za-z0-9._:-]{3,80}$")
PRODUCTION_REPAIR_EVENT_TYPE = "OPERATOR_ENTITLEMENT_REPAIR"
PRODUCTION_SEED_RESULT = "OPERATOR_ENTITLEMENT_SEEDED"
PRODUCTION_CLEAR_RESULT = "OPERATOR_ENTITLEMENT_CLEARED"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Seed or clear a manual B0-A test/operator entitlement snapshot."
    )
    identity = parser.add_mutually_exclusive_group(required=True)
    identity.add_argument("--user-id")
    identity.add_argument("--email")
    identity.add_argument("--provider-subject")
    parser.add_argument("--provider", default="google")
    parser.add_argument("--tier", default="SOARING", choices=["BASIC", "SOARING", "XC", "PRO"])
    parser.add_argument("--period", default="MONTHLY", choices=["MONTHLY", "ANNUAL"])
    parser.add_argument(
        "--status",
        default="ACTIVE",
        choices=sorted(main_module.SUBSCRIPTION_STATUS_VALUES - {"FREE_ACTIVE"}),
    )
    parser.add_argument("--valid-days", type=int, default=30)
    parser.add_argument("--clear", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--confirm-manual-test", action="store_true")
    parser.add_argument("--confirm-production-repair", action="store_true")
    parser.add_argument("--operator-id")
    parser.add_argument("--support-ticket")
    return parser.parse_args(argv)


def run(args: argparse.Namespace) -> dict[str, Any]:
    if not args.confirm_manual_test:
        raise ManualSeedError("--confirm-manual-test is required.")
    validate_production_repair_policy(args)

    db = main_module.SessionLocal()
    try:
        user = resolve_user(db, args)
        if args.clear:
            return clear_snapshot(db, user.id, args)
        return upsert_snapshot(db, user.id, args)
    finally:
        db.close()


def is_production_runtime() -> bool:
    return (
        main_module.PRIVATE_FOLLOW_RUNTIME_CONFIG.runtime_env
        == main_module.RUNTIME_ENV_PROD
    )


def validate_operator_field(
    value: str | None,
    field_name: str,
    pattern: re.Pattern[str],
) -> str:
    normalized = (value or "").strip()
    if not pattern.fullmatch(normalized):
        raise ManualSeedError(
            f"{field_name} is required in production and may contain only "
            "letters, numbers, '.', '_', '-', or ':' as applicable."
        )
    return normalized


def validate_production_repair_policy(args: argparse.Namespace) -> None:
    if not is_production_runtime() or args.dry_run:
        return
    if not args.confirm_production_repair:
        raise ManualSeedError("--confirm-production-repair is required in production.")
    validate_operator_field(args.operator_id, "--operator-id", OPERATOR_ID_PATTERN)
    validate_operator_field(args.support_ticket, "--support-ticket", SUPPORT_TICKET_PATTERN)


def resolve_user(db, args: argparse.Namespace):
    if args.user_id:
        user = db.query(main_module.User).filter(main_module.User.id == args.user_id).first()
        if user is None:
            raise ManualSeedError("No user found for --user-id.")
        return user

    query = db.query(main_module.AuthIdentity)
    if args.provider_subject:
        matches = query.filter(
            main_module.AuthIdentity.provider == args.provider,
            main_module.AuthIdentity.provider_subject == args.provider_subject,
        ).all()
    else:
        email = args.email.strip().lower()
        matches = query.filter(func.lower(main_module.AuthIdentity.provider_email) == email).all()

    user_ids = {match.user_id for match in matches}
    if len(user_ids) != 1:
        raise ManualSeedError("Expected exactly one matching XCPro account identity.")

    user = db.query(main_module.User).filter(main_module.User.id == next(iter(user_ids))).first()
    if user is None:
        raise ManualSeedError("Matched identity has no user row.")
    return user


def clear_snapshot(db, user_id: str, args: argparse.Namespace) -> dict[str, Any]:
    snapshot = (
        db.query(main_module.AccountEntitlementSnapshot)
        .filter(main_module.AccountEntitlementSnapshot.user_id == user_id)
        .first()
    )
    existed = snapshot is not None
    dry_run = args.dry_run
    audit_id = None
    production_repair = is_production_runtime() and not dry_run
    if snapshot is not None and not dry_run:
        db.delete(snapshot)
        audit_id = write_production_repair_audit(
            db=db,
            user_id=user_id,
            action="clear",
            result=PRODUCTION_CLEAR_RESULT,
            args=args,
            values=None,
        )
        db.commit()
    result = {
        "ok": True,
        "action": "clear",
        "dryRun": dry_run,
        "snapshotExisted": existed,
    }
    result.update(public_user_summary(user_id, production_repair))
    if audit_id is not None:
        result["auditId"] = audit_id
    return result


def upsert_snapshot(db, user_id: str, args: argparse.Namespace) -> dict[str, Any]:
    values = build_snapshot_values(args)
    if args.dry_run:
        result = {
            "ok": True,
            "action": "seed",
            "dryRun": True,
            **public_summary(values),
        }
        result.update(public_user_summary(user_id, is_production_runtime()))
        return result

    snapshot = (
        db.query(main_module.AccountEntitlementSnapshot)
        .filter(main_module.AccountEntitlementSnapshot.user_id == user_id)
        .first()
    )
    if snapshot is None:
        snapshot = main_module.AccountEntitlementSnapshot(
            user_id=user_id,
            created_at=values["updated_at"],
            **values,
        )
        db.add(snapshot)
    else:
        for key, value in values.items():
            setattr(snapshot, key, value)
    audit_id = None
    production_repair = is_production_runtime()
    if production_repair:
        audit_id = write_production_repair_audit(
            db=db,
            user_id=user_id,
            action="seed",
            result=PRODUCTION_SEED_RESULT,
            args=args,
            values=values,
        )
    db.commit()
    result = {
        "ok": True,
        "action": "seed",
        "dryRun": False,
        **public_summary(values),
    }
    result.update(public_user_summary(user_id, production_repair))
    if audit_id is not None:
        result["auditId"] = audit_id
    return result


def write_production_repair_audit(
    db,
    user_id: str,
    action: str,
    result: str,
    args: argparse.Namespace,
    values: dict[str, Any] | None,
) -> str | None:
    if not is_production_runtime():
        return None
    operator_id = validate_operator_field(
        args.operator_id,
        "--operator-id",
        OPERATOR_ID_PATTERN,
    )
    support_ticket = validate_operator_field(
        args.support_ticket,
        "--support-ticket",
        SUPPORT_TICKET_PATTERN,
    )
    detail: dict[str, Any] = {
        "source": "OPERATOR_ENTITLEMENT_REPAIR",
        "action": action,
        "operatorId": operator_id,
        "supportTicket": support_ticket,
        "packageName": main_module.XCPRO_RELEASE_PACKAGE_NAME,
    }
    if values is not None:
        detail.update(
            {
                "productId": values["product_id"],
                "basePlanId": values["base_plan_id"],
                "subscriptionStatus": values["status"],
            }
        )
    return main_module.create_billing_audit_record(
        db=db,
        user_id=user_id,
        event_type=PRODUCTION_REPAIR_EVENT_TYPE,
        purchase_token_hash=None,
        result=result,
        detail=detail,
    )


def build_snapshot_values(args: argparse.Namespace) -> dict[str, Any]:
    tier = args.tier
    billing_period = args.period
    status = args.status
    product_id = main_module.PRODUCT_ID_BY_TIER.get(tier)
    base_plan_id = main_module.BASE_PLAN_BY_PERIOD.get(billing_period)

    if product_id is None or base_plan_id is None:
        raise ManualSeedError("Tier or billing period does not map to a known product/base plan.")
    if status in main_module.PAID_CONTINUITY_STATUSES and args.valid_days <= 0:
        raise ManualSeedError("--valid-days must be positive for active paid states.")

    now = main_module.utcnow()
    now_ms = main_module.to_epoch_ms(now)
    valid_until_ms = None
    if status in main_module.PAID_CONTINUITY_STATUSES:
        valid_until_ms = main_module.to_epoch_ms(now + timedelta(days=args.valid_days))

    return {
        "tier": tier,
        "billing_period": billing_period,
        "status": status,
        "source": "GOOGLE_PLAY",
        "verification_state": "VERIFIED",
        "product_id": product_id,
        "base_plan_id": base_plan_id,
        "expiry_time_ms": valid_until_ms,
        "auto_renewing": status != "CANCELED_BUT_ACTIVE",
        "will_lose_access_at_ms": valid_until_ms if status == "CANCELED_BUT_ACTIVE" else None,
        "verified_at_ms": now_ms,
        "fetched_at_ms": now_ms,
        "valid_until_ms": valid_until_ms,
        "stale_after_ms": main_module.PAID_CONTINUITY_STALE_AFTER_MS
        if status in main_module.PAID_CONTINUITY_STATUSES
        else main_module.DENIED_ENTITLEMENT_STALE_AFTER_MS,
        "hard_refresh_after_ms": main_module.PAID_CONTINUITY_HARD_REFRESH_AFTER_MS
        if status in main_module.PAID_CONTINUITY_STATUSES
        else main_module.DENIED_ENTITLEMENT_HARD_REFRESH_AFTER_MS,
        "recovery_action": "NONE",
        "updated_at": now,
    }


def public_summary(values: dict[str, Any]) -> dict[str, Any]:
    return {
        "tier": values["tier"],
        "billingPeriod": values["billing_period"],
        "status": values["status"],
        "productId": values["product_id"],
        "basePlanId": values["base_plan_id"],
        "validUntilMs": values["valid_until_ms"],
    }


def public_user_summary(user_id: str, production_repair: bool) -> dict[str, Any]:
    if production_repair:
        return {"userRef": f"user:{user_id[:8]}"}
    return {"userId": user_id}


def main(argv: list[str] | None = None) -> int:
    try:
        result = run(parse_args(argv))
    except ManualSeedError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, indent=2, sort_keys=True))
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
