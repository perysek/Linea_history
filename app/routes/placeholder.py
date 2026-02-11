"""Placeholder routes for features under development."""
from flask import Blueprint, render_template, jsonify

placeholder_bp = Blueprint('placeholder', __name__)


@placeholder_bp.route('/wykaz-zablokowanych')
def wykaz_zablokowanych():
    """Lista zablokowanych detali z MOSYS."""
    try:
        from MOSYS_data_functions import get_all_blocked_parts
        parts = get_all_blocked_parts()

        # Format dates for display
        for part in parts:
            if part.get('data_niezgodnosci'):
                part['data_niezgodnosci'] = part['data_niezgodnosci'].strftime('%Y-%m-%d')

            # Format production date range
            min_date = part.get('data_produkcji_min')
            max_date = part.get('data_produkcji_max')

            if min_date and max_date:
                if min_date == max_date:
                    part['produced'] = min_date.strftime('%Y/%m/%d')
                else:
                    part['produced'] = f"{min_date.strftime('%Y/%m/%d')} - {max_date.strftime('%Y/%m/%d')}"
            else:
                part['produced'] = '-'

        total_blocked = sum(p['ilosc_zablokowanych'] for p in parts)
        return render_template('placeholder/wykaz_zablokowanych.html', parts=parts, total_blocked=total_blocked)
    except Exception as e:
        print(f"Error fetching blocked parts: {e}")
        # Return None to trigger MOSYS connection error display
        return render_template('placeholder/wykaz_zablokowanych.html', parts=None, total_blocked=0)


@placeholder_bp.route('/wykaz-zablokowanych/boxes/<nr_niezgodnosci>')
def get_blocked_boxes(nr_niezgodnosci):
    """Get box details for a specific NC number."""
    try:
        from MOSYS_data_functions import get_blocked_boxes_details
        boxes = get_blocked_boxes_details(nr_niezgodnosci)
        return jsonify({'success': True, 'boxes': boxes})
    except Exception as e:
        print(f"Error fetching boxes for {nr_niezgodnosci}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


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
