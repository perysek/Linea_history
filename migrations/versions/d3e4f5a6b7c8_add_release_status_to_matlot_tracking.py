"""Add release_status column to matlot_tracking

Revision ID: d3e4f5a6b7c8
Revises: c1d2e3f4a5b6
Create Date: 2026-03-02 13:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd3e4f5a6b7c8'
down_revision = 'c1d2e3f4a5b6'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('matlot_tracking',
        sa.Column('release_status', sa.String(10), nullable=False, server_default='N')
    )


def downgrade():
    op.drop_column('matlot_tracking', 'release_status')
