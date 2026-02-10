"""COLLAUDO table model - Quality control data."""
from app import db
from sqlalchemy import Column, String


class Collaudo(db.Model):
    """COLLAUDO table - Quality control/inspection data."""

    __tablename__ = 'STAAMPDB.COLLAUDO'
    __table_args__ = {'extend_existing': True}

    # Primary key
    COMMESSA = Column(String, primary_key=True)

    # Fields
    DATA_COLLAUDO = Column(String)  # YYYYMMDD format
    PRESSA = Column(String)         # Machine/Press
    ARTICOLO = Column(String)       # Article code
    STAMPO_I = Column(String)       # Mold part I
    STAMPO_P = Column(String)       # Mold part P

    @property
    def stampo_combined(self):
        """Combine STAMPO_I and STAMPO_P into a single string."""
        stampo_i = self.STAMPO_I or ''
        stampo_p = self.STAMPO_P or ''
        return (stampo_i + stampo_p).strip()

    def __repr__(self):
        return f"<Collaudo {self.COMMESSA}>"
