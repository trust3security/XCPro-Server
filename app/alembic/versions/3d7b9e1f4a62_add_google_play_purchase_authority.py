"""add google play purchase authority

Revision ID: 3d7b9e1f4a62
Revises: 2b0a1d4f6c9e
Create Date: 2026-05-15 10:15:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "3d7b9e1f4a62"
down_revision: Union[str, Sequence[str], None] = "2b0a1d4f6c9e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "billing_google_purchases",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("user_id", sa.String(), nullable=False),
        sa.Column("package_name", sa.String(length=120), nullable=False),
        sa.Column("product_id", sa.String(length=80), nullable=False),
        sa.Column("base_plan_id", sa.String(length=80), nullable=False),
        sa.Column("purchase_token_hash", sa.String(length=64), nullable=False),
        sa.Column("linked_purchase_token_hash", sa.String(length=64), nullable=True),
        sa.Column("google_subscription_state", sa.String(length=80), nullable=False),
        sa.Column("xcpro_subscription_status", sa.String(length=40), nullable=False),
        sa.Column("acknowledgement_state", sa.String(length=40), nullable=False),
        sa.Column("expiry_time_ms", sa.BigInteger(), nullable=True),
        sa.Column("auto_renewing", sa.Boolean(), nullable=True),
        sa.Column("last_verified_at_ms", sa.BigInteger(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "purchase_token_hash",
            name="uq_billing_google_purchases_purchase_token_hash",
        ),
    )
    op.create_index(
        op.f("ix_billing_google_purchases_id"),
        "billing_google_purchases",
        ["id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_billing_google_purchases_user_id"),
        "billing_google_purchases",
        ["user_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_billing_google_purchases_purchase_token_hash"),
        "billing_google_purchases",
        ["purchase_token_hash"],
        unique=False,
    )

    op.create_table(
        "billing_google_events",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("pubsub_message_id", sa.String(length=255), nullable=False),
        sa.Column("event_type", sa.String(length=80), nullable=False),
        sa.Column("package_name", sa.String(length=120), nullable=True),
        sa.Column("product_id", sa.String(length=80), nullable=True),
        sa.Column("purchase_token_hash", sa.String(length=64), nullable=True),
        sa.Column("published_at", sa.DateTime(), nullable=True),
        sa.Column("processed_at", sa.DateTime(), nullable=True),
        sa.Column("processing_result", sa.String(length=80), nullable=False),
        sa.Column("audit_id", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "pubsub_message_id",
            name="uq_billing_google_events_pubsub_message_id",
        ),
    )
    op.create_index(
        op.f("ix_billing_google_events_id"),
        "billing_google_events",
        ["id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_billing_google_events_pubsub_message_id"),
        "billing_google_events",
        ["pubsub_message_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_billing_google_events_purchase_token_hash"),
        "billing_google_events",
        ["purchase_token_hash"],
        unique=False,
    )

    op.create_table(
        "billing_audit_records",
        sa.Column("audit_id", sa.String(), nullable=False),
        sa.Column("user_id", sa.String(), nullable=True),
        sa.Column("event_type", sa.String(length=80), nullable=False),
        sa.Column("redacted_subject", sa.String(length=160), nullable=False),
        sa.Column("purchase_token_hash", sa.String(length=64), nullable=True),
        sa.Column("result", sa.String(length=80), nullable=False),
        sa.Column("detail_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("audit_id"),
    )
    op.create_index(
        op.f("ix_billing_audit_records_audit_id"),
        "billing_audit_records",
        ["audit_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_billing_audit_records_user_id"),
        "billing_audit_records",
        ["user_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_billing_audit_records_purchase_token_hash"),
        "billing_audit_records",
        ["purchase_token_hash"],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(
        op.f("ix_billing_audit_records_purchase_token_hash"),
        table_name="billing_audit_records",
    )
    op.drop_index(op.f("ix_billing_audit_records_user_id"), table_name="billing_audit_records")
    op.drop_index(op.f("ix_billing_audit_records_audit_id"), table_name="billing_audit_records")
    op.drop_table("billing_audit_records")

    op.drop_index(
        op.f("ix_billing_google_events_purchase_token_hash"),
        table_name="billing_google_events",
    )
    op.drop_index(
        op.f("ix_billing_google_events_pubsub_message_id"),
        table_name="billing_google_events",
    )
    op.drop_index(op.f("ix_billing_google_events_id"), table_name="billing_google_events")
    op.drop_table("billing_google_events")

    op.drop_index(
        op.f("ix_billing_google_purchases_purchase_token_hash"),
        table_name="billing_google_purchases",
    )
    op.drop_index(op.f("ix_billing_google_purchases_user_id"), table_name="billing_google_purchases")
    op.drop_index(op.f("ix_billing_google_purchases_id"), table_name="billing_google_purchases")
    op.drop_table("billing_google_purchases")
