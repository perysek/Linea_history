"""Add matlot_tracking table for incoming raw material inspection

Revision ID: a3f81c920b44
Revises: 1648cf2d4935
Create Date: 2026-03-02 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a3f81c920b44'
down_revision = 'fbbfe52bc671'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'matlot_tracking',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('codice_materiale', sa.String(length=100), nullable=False),
        sa.Column('lotto', sa.String(length=100), nullable=False),
        sa.Column('prima_vista', sa.Date(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('codice_materiale', 'lotto', name='uq_matlot_batch'),
    )


def downgrade():
    op.drop_table('matlot_tracking')
