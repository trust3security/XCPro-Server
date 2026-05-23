"""expand notification outbox event types

Revision ID: 6b7c8d9e0f1a
Revises: 1d9e6c4b8a2f
Create Date: 2026-05-23 14:35:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "6b7c8d9e0f1a"
down_revision: Union[str, Sequence[str], None] = "1d9e6c4b8a2f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


CHECK_NAME = "ck_notification_outbox_events_event_type"
TABLE_NAME = "notification_outbox_events"
EXPANDED_EVENT_TYPE_CHECK = (
    "event_type IN ("
    "'follow_request_received', "
    "'follow_request_accepted', "
    "'follow_new_follower', "
    "'follow_mutual'"
    ")"
)
ORIGINAL_EVENT_TYPE_CHECK = (
    "event_type IN ("
    "'follow_request_received', "
    "'follow_request_accepted'"
    ")"
)


def upgrade() -> None:
    """Upgrade schema."""
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    op.drop_constraint(CHECK_NAME, TABLE_NAME, type_="check")
    op.create_check_constraint(CHECK_NAME, TABLE_NAME, EXPANDED_EVENT_TYPE_CHECK)


def downgrade() -> None:
    """Downgrade schema."""
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    op.drop_constraint(CHECK_NAME, TABLE_NAME, type_="check")
    op.create_check_constraint(CHECK_NAME, TABLE_NAME, ORIGINAL_EVENT_TYPE_CHECK)
