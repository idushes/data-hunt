"""Add private fund report values and durable jobs.

Revision ID: 72a1ce490bf6
Revises: 69cb4e394ee5
"""

from alembic import op
import sqlalchemy as sa

revision = "72a1ce490bf6"
down_revision = "69cb4e394ee5"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "fund_report_source",
        sa.Column("id", sa.String(32), primary_key=True),
        sa.Column(
            "account_id",
            sa.String(),
            sa.ForeignKey("account.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("amount", sa.Numeric(24, 2)),
        sa.Column("currency", sa.String(3)),
        sa.Column("as_of", sa.Date()),
        sa.Column("updated_at", sa.Integer()),
        sa.Column("lease_token", sa.String(36)),
        sa.Column("lease_until", sa.Integer(), nullable=False),
    )
    op.create_table(
        "fund_report_job",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "source_id",
            sa.String(32),
            sa.ForeignKey("fund_report_source.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("email_id", sa.String(36), nullable=False),
        sa.Column("attachment_id", sa.String(36), nullable=False),
        sa.Column("file_hash", sa.String(64)),
        sa.Column("status", sa.String(24), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.Integer(), nullable=False),
        sa.Column("next_attempt_at", sa.Integer(), nullable=False),
        sa.Column("error_code", sa.String(64)),
        sa.Column("notification_status", sa.String(24), nullable=False),
        sa.Column("notification_attempts", sa.Integer(), nullable=False),
        sa.Column("notification_started_at", sa.Integer()),
        sa.UniqueConstraint(
            "source_id", "email_id", "attachment_id", name="uq_fund_report_attachment"
        ),
        sa.UniqueConstraint("source_id", "file_hash", name="uq_fund_report_file_hash"),
    )
    op.create_index(
        "ix_fund_report_due", "fund_report_job", ["status", "next_attempt_at"]
    )


def downgrade():
    op.drop_index("ix_fund_report_due", table_name="fund_report_job")
    op.drop_table("fund_report_job")
    op.drop_table("fund_report_source")
