"""add favorite follows

Revision ID: f8d2a6c1e9b4
Revises: f4a9c2e7d1b8
Create Date: 2026-05-24 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "f8d2a6c1e9b4"
down_revision: Union[str, Sequence[str], None] = "f4a9c2e7d1b8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "favorite_follows",
        sa.Column("user_id", sa.String(), nullable=False),
        sa.Column("favorite_user_id", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint(
            "user_id <> favorite_user_id",
            name="ck_favorite_follows_no_self",
        ),
        sa.ForeignKeyConstraint(["favorite_user_id"], ["users.id"]),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("user_id", "favorite_user_id"),
    )
    op.create_index(
        "ix_favorite_follows_favorite_user_id",
        "favorite_follows",
        ["favorite_user_id"],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(
        "ix_favorite_follows_favorite_user_id",
        table_name="favorite_follows",
    )
    op.drop_table("favorite_follows")
