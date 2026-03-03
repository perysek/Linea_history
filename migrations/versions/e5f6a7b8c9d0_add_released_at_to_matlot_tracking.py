"""Add released_at column to matlot_tracking

Revision ID: e5f6a7b8c9d0
Revises: d3e4f5a6b7c8
Create Date: 2026-03-02 14:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e5f6a7b8c9d0'
down_revision = 'd3e4f5a6b7c8'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('matlot_tracking',
        sa.Column('released_at', sa.DateTime, nullable=True)
    )


def downgrade():
    op.drop_column('matlot_tracking', 'released_at')
