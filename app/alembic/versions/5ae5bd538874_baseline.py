"""baseline

Revision ID: 5ae5bd538874
Revises: 
Create Date: 2026-03-19 08:43:49.893177

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '5ae5bd538874'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def create_index_if_missing(
    table_name: str,
    index_name: str,
    columns: list[str],
    *,
    unique: bool = False
) -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing_index_names = {
        index["name"]
        for index in inspector.get_indexes(table_name)
    }
    if index_name not in existing_index_names:
        op.create_index(index_name, table_name, columns, unique=unique)


def upgrade() -> None:
    """Bootstrap the legacy public LiveFollow schema for fresh databases."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing_tables = set(inspector.get_table_names())

    if "live_sessions" not in existing_tables:
        op.create_table(
            "live_sessions",
            sa.Column("id", sa.String(), nullable=False),
            sa.Column("share_code", sa.String(), nullable=False),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column(
                "status",
                sa.String(length=20),
                nullable=False,
                server_default=sa.text("'active'"),
            ),
            sa.Column("last_position_at", sa.DateTime(), nullable=True),
            sa.Column("ended_at", sa.DateTime(), nullable=True),
            sa.Column("write_token_hash", sa.String(length=64), nullable=True),
            sa.PrimaryKeyConstraint("id"),
        )
        create_index_if_missing("live_sessions", "ix_live_sessions_id", ["id"])
        create_index_if_missing(
            "live_sessions",
            "ix_live_sessions_share_code",
            ["share_code"],
            unique=True,
        )

    if "live_positions" not in existing_tables:
        op.create_table(
            "live_positions",
            sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
            sa.Column("session_id", sa.String(), nullable=False),
            sa.Column("lat", sa.Float(), nullable=False),
            sa.Column("lon", sa.Float(), nullable=False),
            sa.Column("alt", sa.Float(), nullable=False),
            sa.Column("speed", sa.Float(), nullable=False),
            sa.Column("heading", sa.Float(), nullable=False),
            sa.Column("timestamp", sa.DateTime(), nullable=False),
            sa.PrimaryKeyConstraint("id"),
        )
        create_index_if_missing("live_positions", "ix_live_positions_session_id", ["session_id"])

    if "live_tasks" not in existing_tables:
        op.create_table(
            "live_tasks",
            sa.Column("id", sa.String(), nullable=False),
            sa.Column("session_id", sa.String(), nullable=False),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.Column("current_revision", sa.Integer(), nullable=False),
            sa.PrimaryKeyConstraint("id"),
        )
        create_index_if_missing("live_tasks", "ix_live_tasks_id", ["id"])
        create_index_if_missing(
            "live_tasks",
            "ix_live_tasks_session_id",
            ["session_id"],
            unique=True,
        )

    if "live_task_revisions" not in existing_tables:
        op.create_table(
            "live_task_revisions",
            sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
            sa.Column("task_id", sa.String(), nullable=False),
            sa.Column("revision", sa.Integer(), nullable=False),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("payload_json", sa.Text(), nullable=False),
            sa.PrimaryKeyConstraint("id"),
        )
        create_index_if_missing(
            "live_task_revisions",
            "ix_live_task_revisions_task_id",
            ["task_id"],
        )


def downgrade() -> None:
    """Downgrade schema."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing_tables = set(inspector.get_table_names())

    if "live_task_revisions" in existing_tables:
        op.drop_table("live_task_revisions")
    if "live_tasks" in existing_tables:
        op.drop_table("live_tasks")
    if "live_positions" in existing_tables:
        op.drop_table("live_positions")
    if "live_sessions" in existing_tables:
        op.drop_table("live_sessions")
