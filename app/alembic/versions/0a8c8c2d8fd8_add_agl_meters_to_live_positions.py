"""add agl_meters to live_positions

Revision ID: 0a8c8c2d8fd8
Revises: 5ae5bd538874
Create Date: 2026-03-22 13:20:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "0a8c8c2d8fd8"
down_revision: Union[str, Sequence[str], None] = "5ae5bd538874"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("live_positions", sa.Column("agl_meters", sa.Float(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("live_positions", "agl_meters")
