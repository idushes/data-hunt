"""add feature requests

Revision ID: d739a5ac27f1
Revises: 4c1f9a6d7b20
Create Date: 2026-08-10 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "d739a5ac27f1"
down_revision: Union[str, None] = "4c1f9a6d7b20"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "feature_request",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("title", sa.String(length=120), nullable=False),
        sa.Column("normalized_title", sa.String(length=120), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("category", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_by_account_id", sa.String(), nullable=False),
        sa.Column("created_at", sa.Integer(), nullable=False),
        sa.Column("updated_at", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["created_by_account_id"], ["account.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_feature_request_normalized_title",
        "feature_request",
        ["normalized_title"],
        unique=True,
    )
    op.create_index(
        "ix_feature_request_category", "feature_request", ["category"]
    )
    op.create_index("ix_feature_request_status", "feature_request", ["status"])
    op.create_index(
        "ix_feature_request_created_by_account_id",
        "feature_request",
        ["created_by_account_id"],
    )

    op.create_table(
        "feature_request_vote",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("feature_request_id", sa.Integer(), nullable=False),
        sa.Column("account_id", sa.String(), nullable=False),
        sa.Column("created_at", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["feature_request_id"], ["feature_request.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["account_id"], ["account.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "feature_request_id",
            "account_id",
            name="uq_feature_request_vote_request_account",
        ),
    )
    op.create_index(
        "ix_feature_request_vote_feature_request_id",
        "feature_request_vote",
        ["feature_request_id"],
    )
    op.create_index(
        "ix_feature_request_vote_account_id",
        "feature_request_vote",
        ["account_id"],
    )

    op.create_table(
        "feature_request_feedback",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("feature_request_id", sa.Integer(), nullable=False),
        sa.Column("account_id", sa.String(), nullable=False),
        sa.Column("verdict", sa.String(length=24), nullable=False),
        sa.Column("comment", sa.Text(), nullable=True),
        sa.Column("created_at", sa.Integer(), nullable=False),
        sa.Column("updated_at", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["feature_request_id"], ["feature_request.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["account_id"], ["account.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "feature_request_id",
            "account_id",
            name="uq_feature_request_feedback_request_account",
        ),
    )
    op.create_index(
        "ix_feature_request_feedback_feature_request_id",
        "feature_request_feedback",
        ["feature_request_id"],
    )
    op.create_index(
        "ix_feature_request_feedback_account_id",
        "feature_request_feedback",
        ["account_id"],
    )
    op.create_index(
        "ix_feature_request_feedback_verdict",
        "feature_request_feedback",
        ["verdict"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_feature_request_feedback_verdict",
        table_name="feature_request_feedback",
    )
    op.drop_index(
        "ix_feature_request_feedback_account_id",
        table_name="feature_request_feedback",
    )
    op.drop_index(
        "ix_feature_request_feedback_feature_request_id",
        table_name="feature_request_feedback",
    )
    op.drop_table("feature_request_feedback")
    op.drop_index(
        "ix_feature_request_vote_account_id",
        table_name="feature_request_vote",
    )
    op.drop_index(
        "ix_feature_request_vote_feature_request_id",
        table_name="feature_request_vote",
    )
    op.drop_table("feature_request_vote")
    op.drop_index(
        "ix_feature_request_created_by_account_id",
        table_name="feature_request",
    )
    op.drop_index("ix_feature_request_status", table_name="feature_request")
    op.drop_index("ix_feature_request_category", table_name="feature_request")
    op.drop_index(
        "ix_feature_request_normalized_title",
        table_name="feature_request",
    )
    op.drop_table("feature_request")
