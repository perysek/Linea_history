"""NOTCOJAN table model - Production notes and comments."""
from app import db
from sqlalchemy import Column, String


class Notcojan(db.Model):
    """NOTCOJAN table - Production notes from LINEA."""

    __tablename__ = 'STAAMPDB.NOTCOJAN'
    __table_args__ = {'extend_existing': True}

    # Primary keys
    COMMESSA = Column(String, primary_key=True)
    DATA = Column(String, primary_key=True)  # YYYYMMDD format
    ORA = Column(String, primary_key=True)   # HHMM format

    # Note columns
    NOTE_01 = Column(String)
    NOTE_02 = Column(String)
    NOTE_03 = Column(String)
    NOTE_04 = Column(String)
    NOTE_05 = Column(String)
    NOTE_06 = Column(String)
    NOTE_07 = Column(String)
    NOTE_08 = Column(String)
    NOTE_09 = Column(String)
    NOTE_10 = Column(String)

    # Other fields
    NUMERO_NC = Column(String)
    TIPO_NOTA = Column(String)

    @property
    def formatted_date(self):
        """Convert YYYYMMDD to YYYY/MM/DD format."""
        if self.DATA and len(self.DATA) == 8:
            return f"{self.DATA[:4]}/{self.DATA[4:6]}/{self.DATA[6:8]}"
        return self.DATA or ''

    @property
    def formatted_time(self):
        """Convert HHMM to HH:MM format."""
        if self.ORA and len(self.ORA) >= 4:
            return f"{self.ORA[:2]}:{self.ORA[2:4]}"
        return self.ORA or ''

    @property
    def combined_notes(self):
        """Combine all NOTE fields into a single string."""
        notes = [
            self.NOTE_01, self.NOTE_02, self.NOTE_03, self.NOTE_04, self.NOTE_05,
            self.NOTE_06, self.NOTE_07, self.NOTE_08, self.NOTE_09, self.NOTE_10
        ]
        # Strip whitespace and filter empty notes
        cleaned_notes = [str(n).strip() for n in notes if n and str(n).strip()]
        return ' '.join(cleaned_notes).strip()

    def __repr__(self):
        return f"<Notcojan {self.COMMESSA} {self.DATA} {self.ORA}>"
