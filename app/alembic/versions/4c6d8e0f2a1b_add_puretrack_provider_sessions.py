"""add puretrack provider sessions

Revision ID: 4c6d8e0f2a1b
Revises: 0d3f5a7c9e12
Create Date: 2026-06-18 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "4c6d8e0f2a1b"
down_revision: Union[str, Sequence[str], None] = "0d3f5a7c9e12"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "puretrack_provider_sessions",
        sa.Column("user_id", sa.String(), nullable=False),
        sa.Column("provider_session_hash", sa.String(length=64), nullable=True),
        sa.Column("user_access", sa.String(length=24), nullable=False),
        sa.Column("account_label", sa.String(length=320), nullable=True),
        sa.Column("verified_at_ms", sa.BigInteger(), nullable=True),
        sa.Column("valid_until_ms", sa.BigInteger(), nullable=True),
        sa.Column("error_code", sa.String(length=80), nullable=True),
        sa.Column("retry_after_ms", sa.BigInteger(), nullable=True),
        sa.Column("audit_id", sa.String(length=80), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint(
            "user_access IN ('UNKNOWN', 'NONE', 'FREE', 'PREMIUM', 'ERROR')",
            name="ck_puretrack_provider_sessions_user_access",
        ),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("user_id"),
    )
    op.create_index(
        op.f("ix_puretrack_provider_sessions_provider_session_hash"),
        "puretrack_provider_sessions",
        ["provider_session_hash"],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(
        op.f("ix_puretrack_provider_sessions_provider_session_hash"),
        table_name="puretrack_provider_sessions",
    )
    op.drop_table("puretrack_provider_sessions")
