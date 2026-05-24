"""add live session spectator stats

Revision ID: a9c3e7f1d2b4
Revises: f8d2a6c1e9b4
Create Date: 2026-05-24 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "a9c3e7f1d2b4"
down_revision: Union[str, Sequence[str], None] = "f8d2a6c1e9b4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "live_session_spectator_stats",
        sa.Column("session_id", sa.String(), nullable=False),
        sa.Column("first_position_at", sa.DateTime(), nullable=False),
        sa.Column("last_position_at", sa.DateTime(), nullable=False),
        sa.Column("position_count", sa.Integer(), nullable=False),
        sa.Column("highest_altitude_msl_meters", sa.Float(), nullable=False),
        sa.Column("distance_flown_meters", sa.Float(), nullable=False),
        sa.Column("current_climb_sink_ms", sa.Float(), nullable=True),
        sa.Column("best_short_window_climb_ms", sa.Float(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["session_id"], ["live_sessions.id"]),
        sa.PrimaryKeyConstraint("session_id"),
    )
    op.create_index(
        "ix_live_session_spectator_stats_session_id",
        "live_session_spectator_stats",
        ["session_id"],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(
        "ix_live_session_spectator_stats_session_id",
        table_name="live_session_spectator_stats",
    )
    op.drop_table("live_session_spectator_stats")
