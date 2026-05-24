"""add notification preferences

Revision ID: f4a9c2e7d1b8
Revises: e2b7c9a1d4f6
Create Date: 2026-05-24 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "f4a9c2e7d1b8"
down_revision: Union[str, Sequence[str], None] = "e2b7c9a1d4f6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "privacy_settings",
        sa.Column(
            "social_notifications_enabled",
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
    )
    op.add_column(
        "privacy_settings",
        sa.Column(
            "live_notifications_enabled",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("privacy_settings", "live_notifications_enabled")
    op.drop_column("privacy_settings", "social_notifications_enabled")
