"""Sorting area data models."""
from app import db


class KategoriaZrodlaDanych(db.Model):
    """Department/category data model."""
    __tablename__ = 'dzialy'

    id = db.Column(db.Integer, primary_key=True)
    opis_kategorii = db.Column(db.String(100), unique=True)
    koszt_pracy = db.Column(db.Float, nullable=True)

    def __repr__(self):
        return f'<Kategoria {self.opis_kategorii}>'


class Operator(db.Model):
    """Operator information model."""
    __tablename__ = 'operatorzy'

    id = db.Column(db.Integer, primary_key=True)
    nr_operatora = db.Column(db.Integer, unique=True)
    imie_nazwisko = db.Column(db.String(100))
    dzial_id = db.Column(db.Integer, db.ForeignKey('dzialy.id'))
    dzial = db.relationship("KategoriaZrodlaDanych")

    def __repr__(self):
        return f'<Operator {self.nr_operatora} - {self.imie_nazwisko}>'


class DaneRaportu(db.Model):
    """Main report data for sorting/control results."""
    __tablename__ = 'dane_z_raportow'

    id = db.Column(db.Integer, primary_key=True)
    nr_raportu = db.Column(db.String(50))
    operator_id = db.Column(db.Integer, db.ForeignKey('operatorzy.id'))
    operator = db.relationship("Operator")
    nr_niezgodnosci = db.Column(db.String(50))
    # MOSYS cached data columns
    data_niezgodnosci = db.Column(db.Date, nullable=True)
    nr_zamowienia = db.Column(db.String(50), nullable=True)
    kod_detalu = db.Column(db.String(50), nullable=True)
    nr_instrukcji = db.Column(db.String(50))
    selekcja_na_biezaco = db.Column(db.Boolean, default=False)
    ilosc_detali_sprawdzonych = db.Column(db.Integer)
    braki_defekty = db.relationship(
        "BrakiDefektyRaportu",
        back_populates="raport",
        cascade="all, delete-orphan"
    )
    zalecana_wydajnosc = db.Column(db.Float, nullable=True)
    czas_pracy = db.Column(db.Float)
    uwagi = db.Column(db.Text, nullable=True)
    uwagi_do_wydajnosci = db.Column(db.Text, nullable=True)
    data_selekcji = db.Column(db.Date)

    @property
    def total_defects(self):
        """Calculate total defects for this report."""
        return sum(d.ilosc for d in self.braki_defekty) if self.braki_defekty else 0

    @property
    def rzeczywista_wydajnosc(self):
        """Calculate actual performance (parts per hour)."""
        if self.czas_pracy and self.czas_pracy > 0:
            return self.ilosc_detali_sprawdzonych / self.czas_pracy
        return 0

    @property
    def efektywnosc(self):
        """Calculate efficiency percentage."""
        if self.zalecana_wydajnosc and self.zalecana_wydajnosc > 0 and self.rzeczywista_wydajnosc:
            return (self.rzeczywista_wydajnosc / self.zalecana_wydajnosc) * 100
        return 0

    def __repr__(self):
        return f'<Raport {self.nr_raportu}>'


class BrakiDefektyRaportu(db.Model):
    """Defects/issues related to reports."""
    __tablename__ = 'braki_defekty_raportow'

    id = db.Column(db.Integer, primary_key=True)
    raport_id = db.Column(db.Integer, db.ForeignKey('dane_z_raportow.id'))
    raport = db.relationship("DaneRaportu", back_populates="braki_defekty")
    defekt = db.Column(db.String(200))
    ilosc = db.Column(db.Integer)

    def __repr__(self):
        return f'<Defekt {self.defekt}: {self.ilosc}>'
