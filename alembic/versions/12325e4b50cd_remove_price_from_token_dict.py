"""remove_price_from_token_dict

Revision ID: 12325e4b50cd
Revises: 75503c3455a8
Create Date: 2026-02-23 18:54:40.901829

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '12325e4b50cd'
down_revision: Union[str, None] = '75503c3455a8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_column('token_dict', 'price')
    op.drop_column('token_dict', 'price_24h_change')


def downgrade() -> None:
    op.add_column('token_dict', sa.Column('price_24h_change', sa.Float(), nullable=True))
    op.add_column('token_dict', sa.Column('price', sa.Float(), server_default='0.0', nullable=True))
