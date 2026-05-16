"""add account entitlement snapshots

Revision ID: 2b0a1d4f6c9e
Revises: 9f4a6d2c1b7e
Create Date: 2026-05-04 01:10:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "2b0a1d4f6c9e"
down_revision: Union[str, Sequence[str], None] = "9f4a6d2c1b7e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "account_entitlement_snapshots",
        sa.Column("user_id", sa.String(), nullable=False),
        sa.Column("tier", sa.String(length=24), nullable=False),
        sa.Column("billing_period", sa.String(length=24), nullable=False),
        sa.Column("status", sa.String(length=40), nullable=False),
        sa.Column("source", sa.String(length=40), nullable=False),
        sa.Column("verification_state", sa.String(length=40), nullable=False),
        sa.Column("product_id", sa.String(length=80), nullable=True),
        sa.Column("base_plan_id", sa.String(length=80), nullable=True),
        sa.Column("expiry_time_ms", sa.BigInteger(), nullable=True),
        sa.Column("auto_renewing", sa.Boolean(), nullable=True),
        sa.Column("will_lose_access_at_ms", sa.BigInteger(), nullable=True),
        sa.Column("verified_at_ms", sa.BigInteger(), nullable=True),
        sa.Column("fetched_at_ms", sa.BigInteger(), nullable=False),
        sa.Column("valid_until_ms", sa.BigInteger(), nullable=True),
        sa.Column("stale_after_ms", sa.BigInteger(), nullable=True),
        sa.Column("hard_refresh_after_ms", sa.BigInteger(), nullable=True),
        sa.Column("recovery_action", sa.String(length=40), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("user_id"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("account_entitlement_snapshots")
