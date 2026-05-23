"""add blocks table

Revision ID: 4e1b8c2d9a0f
Revises: 3d7b9e1f4a62
Create Date: 2026-05-21 20:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "4e1b8c2d9a0f"
down_revision: Union[str, Sequence[str], None] = "3d7b9e1f4a62"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "blocks",
        sa.Column("blocker_user_id", sa.String(), nullable=False),
        sa.Column("blocked_user_id", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint(
            "blocker_user_id <> blocked_user_id",
            name="ck_blocks_no_self_block",
        ),
        sa.ForeignKeyConstraint(["blocked_user_id"], ["users.id"]),
        sa.ForeignKeyConstraint(["blocker_user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("blocker_user_id", "blocked_user_id"),
        sa.UniqueConstraint(
            "blocker_user_id",
            "blocked_user_id",
            name="uq_blocks_blocker_blocked",
        ),
    )
    op.create_index(
        op.f("ix_blocks_blocked_user_id"),
        "blocks",
        ["blocked_user_id"],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(op.f("ix_blocks_blocked_user_id"), table_name="blocks")
    op.drop_table("blocks")
