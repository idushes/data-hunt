"""add_token_price_history_and_prices_synced

Revision ID: f8ee65f87beb
Revises: 12325e4b50cd
Create Date: 2026-02-23 19:32:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f8ee65f87beb'
down_revision: Union[str, None] = '12325e4b50cd'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create token_price_history table
    op.create_table(
        'token_price_history',
        sa.Column('token_id', sa.String(), nullable=False),
        sa.Column('chain', sa.String(), nullable=False),
        sa.Column('date', sa.String(), nullable=False),
        sa.Column('price', sa.Float(), nullable=False),
        sa.PrimaryKeyConstraint('token_id', 'chain', 'date')
    )

    # Add prices_synced to address_history
    op.add_column('address_history', sa.Column('prices_synced', sa.Boolean(), server_default='false', nullable=False))


def downgrade() -> None:
    op.drop_column('address_history', 'prices_synced')
    op.drop_table('token_price_history')
