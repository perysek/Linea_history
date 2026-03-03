"""matlot_tracking: add withdrawn_at/withdrawal_reason; expand unique key to include box.

Revision ID: b2c3d4e5f6a7
Revises: f6a7b8c9d0e1
Create Date: 2026-03-03 10:00:00.000000

TASK0  — withdrawn_at (DateTime) and withdrawal_reason (String) columns track S→N
         reversals so giorni_disabled can be derived without a separate flag column.
TASK3  — unique constraint expands from (codice_materiale, lotto) to
         (codice_materiale, lotto, box) so that the same material+lot present in
         multiple MOSYS warehouse locations is tracked as separate rows.
         SQLite requires batch_alter_table to recreate the table with the new constraint.
"""
from alembic import op
import sqlalchemy as sa

revision = 'b2c3d4e5f6a7'
down_revision = 'f6a7b8c9d0e1'
branch_labels = None
depends_on = None


def upgrade():
    # New columns for withdrawal tracking (TASK0)
    op.add_column('matlot_tracking',
        sa.Column('withdrawn_at', sa.DateTime, nullable=True)
    )
    op.add_column('matlot_tracking',
        sa.Column('withdrawal_reason', sa.String(500), nullable=True)
    )

    # Expand unique constraint to include box (TASK3).
    # batch_alter_table recreates the table — the only safe way to change
    # constraints on SQLite.
    with op.batch_alter_table('matlot_tracking', schema=None) as batch_op:
        batch_op.drop_constraint('uq_matlot_batch', type_='unique')
        batch_op.create_unique_constraint(
            'uq_matlot_batch', ['codice_materiale', 'lotto', 'box']
        )


def downgrade():
    with op.batch_alter_table('matlot_tracking', schema=None) as batch_op:
        batch_op.drop_constraint('uq_matlot_batch', type_='unique')
        batch_op.create_unique_constraint(
            'uq_matlot_batch', ['codice_materiale', 'lotto']
        )
    op.drop_column('matlot_tracking', 'withdrawal_reason')
    op.drop_column('matlot_tracking', 'withdrawn_at')
