"""Placeholder routes for features under development."""
from flask import Blueprint, render_template

placeholder_bp = Blueprint('placeholder', __name__)


@placeholder_bp.route('/wykaz-zablokowanych')
def wykaz_zablokowanych():
    """Lista zablokowanych detali - under construction."""
    return render_template('placeholder/wykaz_zablokowanych.html')


@placeholder_bp.route('/dane-selekcji')
def dane_selekcji():
    """Dane z selekcji - under construction."""
    return render_template('placeholder/dane_selekcji.html')


@placeholder_bp.route('/analiza-danych')
def analiza_danych():
    """Analiza danych - under construction."""
    return render_template('placeholder/analiza_danych.html')


@placeholder_bp.route('/dane-zamowien')
def dane_zamowien():
    """Dane zamówień produkcyjnych - under construction."""
    return render_template('placeholder/dane_zamowien.html')


@placeholder_bp.route('/utrzymanie-form')
def utrzymanie_form():
    """Utrzymanie form - under construction."""
    return render_template('placeholder/utrzymanie_form.html')


@placeholder_bp.route('/kontrola-jakosci')
def kontrola_jakosci():
    """Kontrola jakości - under construction."""
    return render_template('placeholder/kontrola_jakosci.html')
