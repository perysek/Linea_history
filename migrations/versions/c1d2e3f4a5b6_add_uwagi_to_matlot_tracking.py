"""Add uwagi column to matlot_tracking

Revision ID: c1d2e3f4a5b6
Revises: a3f81c920b44
Create Date: 2026-03-02 12:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c1d2e3f4a5b6'
down_revision = 'a3f81c920b44'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('matlot_tracking',
        sa.Column('uwagi', sa.String(500), nullable=True)
    )


def downgrade():
    op.drop_column('matlot_tracking', 'uwagi')
