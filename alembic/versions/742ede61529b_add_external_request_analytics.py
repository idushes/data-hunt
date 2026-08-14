"""add external request analytics

Revision ID: 742ede61529b
Revises: 41d9b6f403d7
Create Date: 2026-08-14 00:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "742ede61529b"
down_revision: Union[str, None] = "41d9b6f403d7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "external_request_daily",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("day", sa.Integer(), nullable=False),
        sa.Column("provider", sa.String(length=64), nullable=False),
        sa.Column("request_count", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "day",
            "provider",
            name="uq_external_request_daily_dimension",
        ),
    )
    op.create_index(
        "ix_external_request_daily_day",
        "external_request_daily",
        ["day"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_external_request_daily_day",
        table_name="external_request_daily",
    )
    op.drop_table("external_request_daily")
