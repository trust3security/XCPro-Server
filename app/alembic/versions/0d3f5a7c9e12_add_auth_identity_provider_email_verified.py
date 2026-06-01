"""add auth identity provider email verified

Revision ID: 0d3f5a7c9e12
Revises: a9c3e7f1d2b4
Create Date: 2026-06-01 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "0d3f5a7c9e12"
down_revision: Union[str, Sequence[str], None] = "a9c3e7f1d2b4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "auth_identities",
        sa.Column("provider_email_verified", sa.Boolean(), nullable=True),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("auth_identities", "provider_email_verified")
