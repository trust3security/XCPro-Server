"""add live session viewers

Revision ID: b8e2f4a7c9d1
Revises: 6b7c8d9e0f1a
Create Date: 2026-05-24 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "b8e2f4a7c9d1"
down_revision: Union[str, Sequence[str], None] = "6b7c8d9e0f1a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "live_session_viewers",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("session_id", sa.String(), nullable=False),
        sa.Column("viewer_user_id", sa.String(), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["session_id"], ["live_sessions.id"]),
        sa.ForeignKeyConstraint(["viewer_user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "session_id",
            "viewer_user_id",
            name="uq_live_session_viewers_session_viewer",
        ),
    )
    op.create_index(
        "ix_live_session_viewers_id",
        "live_session_viewers",
        ["id"],
        unique=False,
    )
    op.create_index(
        "ix_live_session_viewers_session_id",
        "live_session_viewers",
        ["session_id"],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_live_session_viewers_session_id", table_name="live_session_viewers")
    op.drop_index("ix_live_session_viewers_id", table_name="live_session_viewers")
    op.drop_table("live_session_viewers")
