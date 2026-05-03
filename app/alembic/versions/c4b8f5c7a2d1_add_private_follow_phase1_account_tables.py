"""add private follow phase1 account tables

Revision ID: c4b8f5c7a2d1
Revises: 0a8c8c2d8fd8
Create Date: 2026-03-24 18:20:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "c4b8f5c7a2d1"
down_revision: Union[str, Sequence[str], None] = "0a8c8c2d8fd8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "users",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_users_id"), "users", ["id"], unique=False)

    op.create_table(
        "auth_identities",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("user_id", sa.String(), nullable=False),
        sa.Column("provider", sa.String(length=40), nullable=False),
        sa.Column("provider_subject", sa.String(length=255), nullable=False),
        sa.Column("provider_email", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "provider",
            "provider_subject",
            name="uq_auth_identities_provider_subject",
        ),
    )
    op.create_index(op.f("ix_auth_identities_id"), "auth_identities", ["id"], unique=False)
    op.create_index(op.f("ix_auth_identities_user_id"), "auth_identities", ["user_id"], unique=False)

    op.create_table(
        "pilot_profiles",
        sa.Column("user_id", sa.String(), nullable=False),
        sa.Column("handle", sa.String(length=24), nullable=True),
        sa.Column("handle_normalized", sa.String(length=24), nullable=True),
        sa.Column("display_name", sa.String(length=80), nullable=True),
        sa.Column("comp_number", sa.String(length=24), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("user_id"),
        sa.UniqueConstraint(
            "handle_normalized",
            name="uq_pilot_profiles_handle_normalized",
        ),
    )
    op.create_index(
        op.f("ix_pilot_profiles_handle_normalized"),
        "pilot_profiles",
        ["handle_normalized"],
        unique=False,
    )

    op.create_table(
        "privacy_settings",
        sa.Column("user_id", sa.String(), nullable=False),
        sa.Column("discoverability", sa.String(length=24), nullable=False),
        sa.Column("follow_policy", sa.String(length=32), nullable=False),
        sa.Column("default_live_visibility", sa.String(length=24), nullable=False),
        sa.Column("connection_list_visibility", sa.String(length=24), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint(
            "discoverability IN ('searchable', 'hidden')",
            name="ck_privacy_settings_discoverability",
        ),
        sa.CheckConstraint(
            "follow_policy IN ('approval_required', 'auto_approve', 'closed')",
            name="ck_privacy_settings_follow_policy",
        ),
        sa.CheckConstraint(
            "default_live_visibility IN ('off', 'followers', 'public')",
            name="ck_privacy_settings_default_live_visibility",
        ),
        sa.CheckConstraint(
            "connection_list_visibility IN ('owner_only', 'mutuals_only', 'public')",
            name="ck_privacy_settings_connection_list_visibility",
        ),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("user_id"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("privacy_settings")
    op.drop_index(op.f("ix_pilot_profiles_handle_normalized"), table_name="pilot_profiles")
    op.drop_table("pilot_profiles")
    op.drop_index(op.f("ix_auth_identities_user_id"), table_name="auth_identities")
    op.drop_index(op.f("ix_auth_identities_id"), table_name="auth_identities")
    op.drop_table("auth_identities")
    op.drop_index(op.f("ix_users_id"), table_name="users")
    op.drop_table("users")
