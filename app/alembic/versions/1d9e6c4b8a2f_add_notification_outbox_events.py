"""add notification outbox events

Revision ID: 1d9e6c4b8a2f
Revises: 8f2c4d7e1a9b
Create Date: 2026-05-21 23:40:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "1d9e6c4b8a2f"
down_revision: Union[str, Sequence[str], None] = "8f2c4d7e1a9b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "notification_outbox_events",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("event_type", sa.String(length=80), nullable=False),
        sa.Column("recipient_user_id", sa.String(), nullable=False),
        sa.Column("actor_user_id", sa.String(), nullable=False),
        sa.Column("follow_request_id", sa.String(), nullable=True),
        sa.Column("dedupe_key", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=24), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("last_attempt_at", sa.DateTime(), nullable=True),
        sa.Column("sent_at", sa.DateTime(), nullable=True),
        sa.Column("last_error", sa.String(length=320), nullable=True),
        sa.Column("last_error_retryable", sa.Boolean(), nullable=True),
        sa.Column("payload_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint(
            (
                "event_type IN ("
                "'follow_request_received', "
                "'follow_request_accepted', "
                "'follow_new_follower', "
                "'follow_mutual'"
                ")"
            ),
            name="ck_notification_outbox_events_event_type",
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'sent', 'retryable_failed', 'failed')",
            name="ck_notification_outbox_events_status",
        ),
        sa.ForeignKeyConstraint(["actor_user_id"], ["users.id"]),
        sa.ForeignKeyConstraint(["follow_request_id"], ["follow_requests.id"]),
        sa.ForeignKeyConstraint(["recipient_user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "dedupe_key",
            name="uq_notification_outbox_events_dedupe_key",
        ),
    )
    op.create_index(
        op.f("ix_notification_outbox_events_actor_user_id"),
        "notification_outbox_events",
        ["actor_user_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_notification_outbox_events_event_type"),
        "notification_outbox_events",
        ["event_type"],
        unique=False,
    )
    op.create_index(
        op.f("ix_notification_outbox_events_follow_request_id"),
        "notification_outbox_events",
        ["follow_request_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_notification_outbox_events_id"),
        "notification_outbox_events",
        ["id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_notification_outbox_events_recipient_user_id"),
        "notification_outbox_events",
        ["recipient_user_id"],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(
        op.f("ix_notification_outbox_events_recipient_user_id"),
        table_name="notification_outbox_events",
    )
    op.drop_index(
        op.f("ix_notification_outbox_events_id"),
        table_name="notification_outbox_events",
    )
    op.drop_index(
        op.f("ix_notification_outbox_events_follow_request_id"),
        table_name="notification_outbox_events",
    )
    op.drop_index(
        op.f("ix_notification_outbox_events_event_type"),
        table_name="notification_outbox_events",
    )
    op.drop_index(
        op.f("ix_notification_outbox_events_actor_user_id"),
        table_name="notification_outbox_events",
    )
    op.drop_table("notification_outbox_events")
