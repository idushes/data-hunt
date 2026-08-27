"""add account identities

Revision ID: 69cb4e394ee5
Revises: e7c5a91f2d44
Create Date: 2026-08-27 00:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "69cb4e394ee5"
down_revision: Union[str, None] = "e7c5a91f2d44"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "account_identity",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("account_id", sa.String(), nullable=False),
        sa.Column("provider", sa.String(length=32), nullable=False),
        sa.Column("subject", sa.String(length=255), nullable=False),
        sa.Column("created_at", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["account_id"],
            ["account.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "provider",
            "subject",
            name="uq_account_identity_provider_subject",
        ),
    )
    op.create_index(
        op.f("ix_account_identity_account_id"),
        "account_identity",
        ["account_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_account_identity_account_id"),
        table_name="account_identity",
    )
    op.drop_table("account_identity")
