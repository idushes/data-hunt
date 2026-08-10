"""add account token purpose

Revision ID: e42f63c8a10b
Revises: b941c7d8e2f0
Create Date: 2026-08-10 21:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "e42f63c8a10b"
down_revision: Union[str, None] = "b941c7d8e2f0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "account_token",
        sa.Column(
            "purpose",
            sa.String(length=16),
            nullable=False,
            server_default="session",
        ),
    )
    op.create_index(
        "ix_account_token_purpose",
        "account_token",
        ["purpose"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_account_token_purpose", table_name="account_token")
    op.drop_column("account_token", "purpose")
