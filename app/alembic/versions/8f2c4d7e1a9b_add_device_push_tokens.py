"""add device push tokens

Revision ID: 8f2c4d7e1a9b
Revises: 4e1b8c2d9a0f
Create Date: 2026-05-21 23:10:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "8f2c4d7e1a9b"
down_revision: Union[str, Sequence[str], None] = "4e1b8c2d9a0f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "device_push_tokens",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("user_id", sa.String(), nullable=False),
        sa.Column("platform", sa.String(length=24), nullable=False),
        sa.Column("provider", sa.String(length=24), nullable=False),
        sa.Column("token_hash", sa.String(length=64), nullable=False),
        sa.Column("token_ciphertext", sa.Text(), nullable=False),
        sa.Column("device_id", sa.String(length=160), nullable=False),
        sa.Column("app_version", sa.String(length=80), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("revoked_at", sa.DateTime(), nullable=True),
        sa.CheckConstraint(
            "platform IN ('android')",
            name="ck_device_push_tokens_platform",
        ),
        sa.CheckConstraint(
            "provider IN ('fcm')",
            name="ck_device_push_tokens_provider",
        ),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "user_id",
            "platform",
            "provider",
            "device_id",
            name="uq_device_push_tokens_user_platform_provider_device",
        ),
    )
    op.create_index(
        op.f("ix_device_push_tokens_id"),
        "device_push_tokens",
        ["id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_device_push_tokens_token_hash"),
        "device_push_tokens",
        ["token_hash"],
        unique=False,
    )
    op.create_index(
        op.f("ix_device_push_tokens_user_id"),
        "device_push_tokens",
        ["user_id"],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(op.f("ix_device_push_tokens_user_id"), table_name="device_push_tokens")
    op.drop_index(op.f("ix_device_push_tokens_token_hash"), table_name="device_push_tokens")
    op.drop_index(op.f("ix_device_push_tokens_id"), table_name="device_push_tokens")
    op.drop_table("device_push_tokens")
