"""Cache giacenza_lotto and box columns in matlot_tracking

Revision ID: f6a7b8c9d0e1
Revises: e5f6a7b8c9d0
Create Date: 2026-03-02 15:00:00.000000

These columns cache MOSYS metadata so the data endpoint can read from SQLite
only, without hitting MOSYS on every search/sort/scroll request.
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f6a7b8c9d0e1'
down_revision = 'e5f6a7b8c9d0'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('matlot_tracking',
        sa.Column('giacenza_lotto', sa.Integer, nullable=True, server_default='0')
    )
    op.add_column('matlot_tracking',
        sa.Column('box', sa.String(100), nullable=True, server_default='')
    )


def downgrade():
    op.drop_column('matlot_tracking', 'box')
    op.drop_column('matlot_tracking', 'giacenza_lotto')
