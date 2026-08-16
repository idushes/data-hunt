"""add anonymous auth funnel events

Revision ID: b8c91f8a0d12
Revises: 742ede61529b
Create Date: 2026-08-16 10:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "b8c91f8a0d12"
down_revision: Union[str, None] = "742ede61529b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "auth_funnel_event",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("anonymous_session_id", sa.String(length=36), nullable=False),
        sa.Column("day", sa.Integer(), nullable=False),
        sa.Column("event_name", sa.String(length=32), nullable=False),
        sa.Column("utm_source", sa.String(length=48), nullable=True),
        sa.Column("utm_medium", sa.String(length=48), nullable=True),
        sa.Column("utm_campaign", sa.String(length=96), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "day",
            "anonymous_session_id",
            "event_name",
            name="uq_auth_funnel_event_day_session_event",
        ),
    )
    op.create_index("ix_auth_funnel_event_day", "auth_funnel_event", ["day"], unique=False)
    op.create_index(
        "ix_auth_funnel_event_day_event",
        "auth_funnel_event",
        ["day", "event_name"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_auth_funnel_event_day_event", table_name="auth_funnel_event")
    op.drop_index("ix_auth_funnel_event_day", table_name="auth_funnel_event")
    op.drop_table("auth_funnel_event")
