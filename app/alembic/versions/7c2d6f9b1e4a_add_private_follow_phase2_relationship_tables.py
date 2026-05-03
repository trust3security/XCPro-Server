"""add private follow phase2 relationship tables

Revision ID: 7c2d6f9b1e4a
Revises: c4b8f5c7a2d1
Create Date: 2026-03-24 20:10:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "7c2d6f9b1e4a"
down_revision: Union[str, Sequence[str], None] = "c4b8f5c7a2d1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "follow_requests",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("requester_user_id", sa.String(), nullable=False),
        sa.Column("target_user_id", sa.String(), nullable=False),
        sa.Column("status", sa.String(length=24), nullable=False),
        sa.Column("responded_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint(
            "status IN ('pending', 'accepted', 'declined')",
            name="ck_follow_requests_status",
        ),
        sa.ForeignKeyConstraint(["requester_user_id"], ["users.id"]),
        sa.ForeignKeyConstraint(["target_user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "requester_user_id",
            "target_user_id",
            name="uq_follow_requests_requester_target",
        ),
    )
    op.create_index(op.f("ix_follow_requests_id"), "follow_requests", ["id"], unique=False)
    op.create_index(
        op.f("ix_follow_requests_requester_user_id"),
        "follow_requests",
        ["requester_user_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_follow_requests_target_user_id"),
        "follow_requests",
        ["target_user_id"],
        unique=False,
    )

    op.create_table(
        "follow_edges",
        sa.Column("follower_user_id", sa.String(), nullable=False),
        sa.Column("followed_user_id", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["followed_user_id"], ["users.id"]),
        sa.ForeignKeyConstraint(["follower_user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("follower_user_id", "followed_user_id"),
    )
    op.create_index(
        "ix_follow_edges_followed_user_id",
        "follow_edges",
        ["followed_user_id"],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_follow_edges_followed_user_id", table_name="follow_edges")
    op.drop_table("follow_edges")
    op.drop_index(op.f("ix_follow_requests_target_user_id"), table_name="follow_requests")
    op.drop_index(op.f("ix_follow_requests_requester_user_id"), table_name="follow_requests")
    op.drop_index(op.f("ix_follow_requests_id"), table_name="follow_requests")
    op.drop_table("follow_requests")
