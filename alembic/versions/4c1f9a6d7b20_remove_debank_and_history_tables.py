"""remove DeBank and history tables

Revision ID: 4c1f9a6d7b20
Revises: f8ee65f87beb
Create Date: 2026-08-10 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "4c1f9a6d7b20"
down_revision: Union[str, None] = "f8ee65f87beb"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_table("token_price_history")
    op.drop_table("address_history")
    op.drop_table("cex_dict")
    op.drop_table("token_dict")
    op.drop_table("project_dict")
    op.drop_index(
        op.f("ix_debank_request_account_id"), table_name="debank_request"
    )
    op.drop_table("debank_request")


def downgrade() -> None:
    op.create_table(
        "debank_request",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("account_id", sa.String(), nullable=True),
        sa.Column("path", sa.String(), nullable=False),
        sa.Column("params", sa.String(), nullable=True),
        sa.Column("response_json", sa.String(), nullable=True),
        sa.Column("status", sa.String(), nullable=True),
        sa.Column("cost", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_debank_request_account_id"),
        "debank_request",
        ["account_id"],
        unique=False,
    )
    op.create_table(
        "project_dict",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("chain", sa.String(), nullable=True),
        sa.Column("logo_url", sa.String(), nullable=True),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("site_url", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("id"),
    )
    op.create_table(
        "token_dict",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("chain", sa.String(), nullable=False),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("symbol", sa.String(), nullable=True),
        sa.Column("display_symbol", sa.String(), nullable=True),
        sa.Column("optimized_symbol", sa.String(), nullable=True),
        sa.Column("decimals", sa.Integer(), nullable=True),
        sa.Column("logo_url", sa.String(), nullable=True),
        sa.Column("protocol_id", sa.String(), nullable=True),
        sa.Column("is_verified", sa.Boolean(), nullable=True),
        sa.Column("is_core", sa.Boolean(), nullable=True),
        sa.Column("is_wallet", sa.Boolean(), nullable=True),
        sa.Column("is_scam", sa.Boolean(), nullable=True),
        sa.Column("is_suspicious", sa.Boolean(), nullable=True),
        sa.Column("credit_score", sa.Float(), nullable=True),
        sa.Column("total_supply", sa.Float(), nullable=True),
        sa.Column("time_at", sa.Float(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "cex_dict",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("cex_id", sa.String(), nullable=False),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("logo_url", sa.String(), nullable=True),
        sa.Column("is_deposit", sa.Boolean(), nullable=True),
        sa.Column("is_collect", sa.Boolean(), nullable=True),
        sa.Column("is_gastopup", sa.Boolean(), nullable=True),
        sa.Column("is_vault", sa.Boolean(), nullable=True),
        sa.Column("is_withdraw", sa.Boolean(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "address_history",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("chain", sa.String(), nullable=False),
        sa.Column("address", sa.String(), nullable=False),
        sa.Column("cate_id", sa.String(), nullable=True),
        sa.Column("time_at", sa.Integer(), nullable=True),
        sa.Column("is_scam", sa.Boolean(), nullable=True),
        sa.Column(
            "json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("prices_synced", sa.Boolean(), nullable=False),
        sa.PrimaryKeyConstraint("id", "chain", "address"),
    )
    op.create_table(
        "token_price_history",
        sa.Column("token_id", sa.String(), nullable=False),
        sa.Column("chain", sa.String(), nullable=False),
        sa.Column("date", sa.String(), nullable=False),
        sa.Column("price", sa.Float(), nullable=False),
        sa.PrimaryKeyConstraint("token_id", "chain", "date"),
    )
