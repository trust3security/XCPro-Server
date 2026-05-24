"""add user relationship counters

Revision ID: d6a8f0c2b9e3
Revises: b8e2f4a7c9d1
Create Date: 2026-05-24 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "d6a8f0c2b9e3"
down_revision: Union[str, Sequence[str], None] = "b8e2f4a7c9d1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "user_relationship_counters",
        sa.Column("user_id", sa.String(), nullable=False),
        sa.Column("followers_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("following_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("user_id"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("user_relationship_counters")
