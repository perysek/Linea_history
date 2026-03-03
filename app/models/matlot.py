"""MATLOT incoming raw material inspection model."""
from datetime import datetime
from app import db


class MatlotTracking(db.Model):
    """Tracks MATLOT batches present in MOSYS with local release status.

    MATLOT in MOSYS has no creation date column, so this table records the
    first time each batch was seen (prima_vista). The release_status column
    replaces MOSYS LOTTO_VERIFICATO as the source of truth for workflow state —
    MOSYS is written to only as a best-effort parallel write.

    release_status values:
        'N' — pending, awaiting certificate approval
        'S' — released / approved by operator in LINEA

    Unique key: (codice_materiale, lotto, box) — each MOSYS warehouse location
    for the same material+lot is a distinct tracking row.

    giorni_disabled is derived (not stored): True when a 't'-prefix item has
    been withdrawn (release_status='N' AND withdrawn_at IS NOT NULL).

    Lifecycle:
        - Row created: first time the MOSYS sync sees this (codice, lotto, box)
        - release_status N→S: operator approves in LINEA
        - release_status S→N: operator withdraws — withdrawn_at/withdrawal_reason set
        - Row deleted: batch disappears from MOSYS AND release_status == 'S'
    """
    __tablename__ = 'matlot_tracking'

    id = db.Column(db.Integer, primary_key=True)
    codice_materiale = db.Column(db.String(100), nullable=False)
    lotto = db.Column(db.String(100), nullable=False)
    prima_vista = db.Column(db.Date, nullable=False)

    # MOSYS metadata — cached on every sync so data reads never need MOSYS
    giacenza_lotto = db.Column(db.Integer, nullable=True, default=0)
    box = db.Column(db.String(100), nullable=True, default='')

    release_status = db.Column(db.String(10), nullable=False, default='N')
    released_at = db.Column(db.DateTime, nullable=True)
    uwagi = db.Column(db.String(500), nullable=True)

    # Withdrawal tracking (TASK0)
    withdrawn_at = db.Column(db.DateTime, nullable=True)
    withdrawal_reason = db.Column(db.String(500), nullable=True)

    __table_args__ = (
        # Expanded from (codice_materiale, lotto) to include box (TASK3):
        # the same material+lot can appear in multiple MOSYS warehouse locations.
        db.UniqueConstraint('codice_materiale', 'lotto', 'box', name='uq_matlot_batch'),
    )

    def __repr__(self):
        return f'<MatlotTracking {self.codice_materiale}/{self.lotto}@{self.box} since {self.prima_vista}>'
