"""add value resources

Revision ID: b941c7d8e2f0
Revises: d739a5ac27f1
Create Date: 2026-08-10 16:10:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "b941c7d8e2f0"
down_revision: Union[str, None] = "d739a5ac27f1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "value_resource",
        sa.Column("id", sa.String(length=22), nullable=False),
        sa.Column("fingerprint", sa.String(length=64), nullable=False),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("key", sa.Text(), nullable=True),
        sa.Column("column", sa.String(length=128), nullable=True),
        sa.Column("parameters", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_value_resource_fingerprint",
        "value_resource",
        ["fingerprint"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_value_resource_fingerprint",
        table_name="value_resource",
    )
    op.drop_table("value_resource")
