"""add puretrack provider session ciphertext

Revision ID: 5b7a9d0e4c3f
Revises: 4c6d8e0f2a1b
Create Date: 2026-06-19 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "5b7a9d0e4c3f"
down_revision: Union[str, Sequence[str], None] = "4c6d8e0f2a1b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "puretrack_provider_sessions",
        sa.Column("provider_session_ciphertext", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("puretrack_provider_sessions", "provider_session_ciphertext")
