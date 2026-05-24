"""add social graph lookup indexes

Revision ID: e2b7c9a1d4f6
Revises: d6a8f0c2b9e3
Create Date: 2026-05-24 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "e2b7c9a1d4f6"
down_revision: Union[str, Sequence[str], None] = "d6a8f0c2b9e3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_index(
        "ix_follow_requests_requester_status_updated_at",
        "follow_requests",
        ["requester_user_id", "status", "updated_at"],
        unique=False,
    )
    op.create_index(
        "ix_follow_requests_target_status_updated_at",
        "follow_requests",
        ["target_user_id", "status", "updated_at"],
        unique=False,
    )
    op.create_index(
        "ix_live_sessions_owner_status",
        "live_sessions",
        ["owner_user_id", "status"],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_live_sessions_owner_status", table_name="live_sessions")
    op.drop_index(
        "ix_follow_requests_target_status_updated_at",
        table_name="follow_requests",
    )
    op.drop_index(
        "ix_follow_requests_requester_status_updated_at",
        table_name="follow_requests",
    )
