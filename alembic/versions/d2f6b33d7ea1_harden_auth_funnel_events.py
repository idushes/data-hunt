"""harden anonymous auth funnel events

Revision ID: d2f6b33d7ea1
Revises: b8c91f8a0d12
Create Date: 2026-08-16 11:00:00.000000
"""

from typing import Sequence, Union

from alembic import op


revision: str = "d2f6b33d7ea1"
down_revision: Union[str, None] = "b8c91f8a0d12"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


EVENT_CHECK = (
    "event_name IN ('sheets_view', 'login_clicked', 'wallet_missing', "
    "'wallet_connection_rejected', 'signature_requested', "
    "'signature_rejected', 'login_succeeded', 'login_failed', "
    "'table_loaded', 'formula_copied')"
)
UUID_V4_CHECK = (
    "anonymous_session_id ~ "
    "'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'"
)


def upgrade() -> None:
    op.create_check_constraint("ck_auth_funnel_event_name", "auth_funnel_event", EVENT_CHECK)
    op.create_check_constraint(
        "ck_auth_funnel_event_session_uuid_v4",
        "auth_funnel_event",
        UUID_V4_CHECK,
    )


def downgrade() -> None:
    op.drop_constraint("ck_auth_funnel_event_session_uuid_v4", "auth_funnel_event", type_="check")
    op.drop_constraint("ck_auth_funnel_event_name", "auth_funnel_event", type_="check")
