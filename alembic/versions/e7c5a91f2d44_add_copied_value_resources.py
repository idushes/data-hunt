"""add copied value resources

Revision ID: e7c5a91f2d44
Revises: d2f6b33d7ea1
Create Date: 2026-08-18 00:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "e7c5a91f2d44"
down_revision: Union[str, None] = "d2f6b33d7ea1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "account_value_resource",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("account_id", sa.String(), nullable=False),
        sa.Column("resource_id", sa.String(length=22), nullable=False),
        sa.Column("first_copied_at", sa.Integer(), nullable=False),
        sa.Column("last_copied_at", sa.Integer(), nullable=False),
        sa.Column("copy_count", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["account_id"],
            ["account.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["resource_id"],
            ["value_resource.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "account_id",
            "resource_id",
            name="uq_account_value_resource_account_resource",
        ),
    )
    op.create_index(
        "ix_account_value_resource_account_last_copied",
        "account_value_resource",
        ["account_id", "last_copied_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_account_value_resource_account_last_copied",
        table_name="account_value_resource",
    )
    op.drop_table("account_value_resource")
