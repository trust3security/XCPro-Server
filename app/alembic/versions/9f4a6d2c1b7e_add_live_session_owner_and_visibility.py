"""add live session owner and visibility

Revision ID: 9f4a6d2c1b7e
Revises: 7c2d6f9b1e4a
Create Date: 2026-03-24 23:10:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "9f4a6d2c1b7e"
down_revision: Union[str, Sequence[str], None] = "7c2d6f9b1e4a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    with op.batch_alter_table("live_sessions") as batch_op:
        batch_op.add_column(
            sa.Column("owner_user_id", sa.String(), nullable=True),
        )
        batch_op.add_column(
            sa.Column(
                "visibility",
                sa.String(length=24),
                nullable=False,
                server_default="public",
            ),
        )
        batch_op.create_index(
            "ix_live_sessions_owner_user_id",
            ["owner_user_id"],
            unique=False,
        )
        batch_op.create_foreign_key(
            "fk_live_sessions_owner_user_id_users",
            "users",
            ["owner_user_id"],
            ["id"],
        )
        batch_op.create_check_constraint(
            "ck_live_sessions_visibility",
            "visibility IN ('off', 'followers', 'public')",
        )


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table("live_sessions") as batch_op:
        batch_op.drop_constraint(
            "ck_live_sessions_visibility",
            type_="check",
        )
        batch_op.drop_constraint(
            "fk_live_sessions_owner_user_id_users",
            type_="foreignkey",
        )
        batch_op.drop_index("ix_live_sessions_owner_user_id")
        batch_op.drop_column("visibility")
        batch_op.drop_column("owner_user_id")
