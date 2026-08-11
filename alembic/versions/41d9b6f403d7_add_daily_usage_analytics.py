"""add daily usage analytics

Revision ID: 41d9b6f403d7
Revises: e42f63c8a10b
Create Date: 2026-08-11 10:45:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "41d9b6f403d7"
down_revision: Union[str, None] = "e42f63c8a10b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "usage_daily",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("day", sa.Integer(), nullable=False),
        sa.Column("account_id", sa.String(), nullable=False),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("status_group", sa.String(length=16), nullable=False),
        sa.Column("request_count", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "day",
            "account_id",
            "source",
            "status_group",
            name="uq_usage_daily_dimension",
        ),
    )
    op.create_index(
        "ix_usage_daily_day_account",
        "usage_daily",
        ["day", "account_id"],
        unique=False,
    )
    op.create_index(
        "ix_usage_daily_day_source",
        "usage_daily",
        ["day", "source"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_usage_daily_day_source", table_name="usage_daily")
    op.drop_index("ix_usage_daily_day_account", table_name="usage_daily")
    op.drop_table("usage_daily")
