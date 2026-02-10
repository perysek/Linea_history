"""LINEA routes - Production notes view."""
import re
from datetime import date, timedelta
from flask import Blueprint, render_template, request, jsonify
from app.database import execute_query, mosys_to_date, mosys_godz, get_stampi_riparaz

linea_bp = Blueprint('linea', __name__, url_prefix='/linea')


def extract_codice_riparazione(uwaga):
    """Extract CODICE_RIPARAZIONE from UWAGA if it matches pattern."""
    if not uwaga:
        return None

    # Pattern: "CREATO FOGLIO ROSSO N. xxxxxxxx"
    match = re.search(r'CREATO FOGLIO ROSSO N\.\s*(\S+)', uwaga, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return None


def combine_notes(row):
    """Combine NOTE_01 through NOTE_10 into a single string."""
    notes = []
    for i in range(1, 11):
        note_key = f'NOTE_0{i}' if i < 10 else f'NOTE_{i}'
        note_value = row.get(note_key, '')
        if note_value and str(note_value).strip():
            notes.append(str(note_value).strip())
    return ' '.join(notes)


def get_linea_records(start_date, end_date, search_filters=None, sort_field='DATA', sort_dir='desc'):
    """Fetch LINEA records with optional filtering and sorting."""

    # Base query - join NOTCOJAN with COLLAUDO
    query = '''
        SELECT NOTCOJAN.COMMESSA, NOTCOJAN.DATA, NOTCOJAN.ORA,
        NOTCOJAN.NOTE_01, NOTCOJAN.NOTE_02, NOTCOJAN.NOTE_03, NOTCOJAN.NOTE_04, NOTCOJAN.NOTE_05,
        NOTCOJAN.NOTE_06, NOTCOJAN.NOTE_07, NOTCOJAN.NOTE_08, NOTCOJAN.NOTE_09, NOTCOJAN.NOTE_10,
        NOTCOJAN.NUMERO_NC, NOTCOJAN.TIPO_NOTA,
        COLLAUDO.PRESSA, COLLAUDO.ARTICOLO, COLLAUDO.STAMPO_I, COLLAUDO.STAMPO_P
        FROM STAAMPDB.NOTCOJAN NOTCOJAN
        LEFT JOIN STAAMPDB.COLLAUDO COLLAUDO ON NOTCOJAN.COMMESSA = COLLAUDO.COMMESSA
        WHERE (NOTCOJAN.DATA >= ? AND NOTCOJAN.DATA <= ?)
    '''

    params = [start_date, end_date]

    # Add search filters
    if search_filters:
        if search_filters.get('COMM'):
            query += " AND NOTCOJAN.COMMESSA LIKE ?"
            params.append(f"%{search_filters['COMM']}%")
        if search_filters.get('NR_NIEZG'):
            query += " AND NOTCOJAN.NUMERO_NC LIKE ?"
            params.append(f"%{search_filters['NR_NIEZG']}%")
        if search_filters.get('TYP_UWAGI'):
            query += " AND NOTCOJAN.TIPO_NOTA LIKE ?"
            params.append(f"%{search_filters['TYP_UWAGI']}%")
        if search_filters.get('MASZYNA'):
            query += " AND COLLAUDO.PRESSA LIKE ?"
            params.append(f"%{search_filters['MASZYNA']}%")
        if search_filters.get('KOD_DETALU'):
            query += " AND COLLAUDO.ARTICOLO LIKE ?"
            params.append(f"%{search_filters['KOD_DETALU']}%")

    # Add sorting
    sort_column_map = {
        'COMM': 'NOTCOJAN.COMMESSA',
        'DATA': 'NOTCOJAN.DATA',
        'GODZ': 'NOTCOJAN.ORA',
        'NR_NIEZG': 'NOTCOJAN.NUMERO_NC',
        'TYP_UWAGI': 'NOTCOJAN.TIPO_NOTA',
        'MASZYNA': 'COLLAUDO.PRESSA',
        'KOD_DETALU': 'COLLAUDO.ARTICOLO',
    }

    if sort_field in sort_column_map:
        order_clause = f" ORDER BY {sort_column_map[sort_field]}"
        if sort_dir == 'desc':
            order_clause += " DESC"
        else:
            order_clause += " ASC"
        query += order_clause
    else:
        # Default sorting
        query += " ORDER BY NOTCOJAN.COMMESSA, NOTCOJAN.DATA, NOTCOJAN.ORA ASC"

    # Execute query
    rows = execute_query(query, tuple(params))

    # Transform results
    records = []
    for row in rows:
        uwaga = combine_notes(row)
        nr_formy = (row.get('STAMPO_I', '') or '') + (row.get('STAMPO_P', '') or '')

        # Check if UWAGA contains CODICE_RIPARAZIONE pattern
        codice_rip = extract_codice_riparazione(uwaga)

        record = {
            'COMM': row.get('COMMESSA', '') or '',
            'DATA': mosys_to_date(row.get('DATA', '')),
            'GODZ': mosys_godz(row.get('ORA', '')),
            'NR_NIEZG': row.get('NUMERO_NC', '') or '',
            'TYP_UWAGI': row.get('TIPO_NOTA', '') or '',
            'UWAGA': uwaga,
            'MASZYNA': row.get('PRESSA', '') or '',
            'KOD_DETALU': row.get('ARTICOLO', '') or '',
            'NR_FORMY': nr_formy,
            'CODICE_RIPARAZIONE': codice_rip  # Add this field
        }
        records.append(record)

    # Client-side filtering for computed fields (UWAGA, NR_FORMY)
    if search_filters:
        if search_filters.get('UWAGA'):
            search_uwaga = search_filters['UWAGA'].lower()
            records = [r for r in records if search_uwaga in r['UWAGA'].lower()]

        if search_filters.get('NR_FORMY'):
            search_formy = search_filters['NR_FORMY'].lower()
            records = [r for r in records if search_formy in r['NR_FORMY'].lower()]

    return records


@linea_bp.route('/')
def index():
    """Main LINEA table view."""
    # Get filter parameters
    days = request.args.get('days', type=int)
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')

    # Calculate date range
    if date_from and date_to:
        # Custom range selected
        start_date = date_from.replace('-', '')  # Convert YYYY-MM-DD to YYYYMMDD
        end_date = date_to.replace('-', '')
        active_preset = None
    elif days:
        # Preset button selected
        end_date = date.today().strftime("%Y%m%d")
        start_date = (date.today() - timedelta(days=days)).strftime("%Y%m%d")
        active_preset = days
    else:
        # Default: 30 days
        end_date = date.today().strftime("%Y%m%d")
        start_date = (date.today() - timedelta(days=30)).strftime("%Y%m%d")
        active_preset = 30

    # Get records
    try:
        records = get_linea_records(start_date, end_date)
    except Exception as e:
        print(f"Error fetching records: {e}")
        records = []

    return render_template('linea/index.html',
                         records=records,
                         active_preset=active_preset,
                         date_from=date_from,
                         date_to=date_to)


@linea_bp.route('/api/search')
def search_records():
    """AJAX endpoint for searching and filtering records."""
    # Get date range parameters
    days = request.args.get('days', 30, type=int)
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')

    # Get column search filters
    search_filters = {
        'COMM': request.args.get('search_COMM', '').strip(),
        'NR_NIEZG': request.args.get('search_NR_NIEZG', '').strip(),
        'TYP_UWAGI': request.args.get('search_TYP_UWAGI', '').strip(),
        'UWAGA': request.args.get('search_UWAGA', '').strip(),
        'MASZYNA': request.args.get('search_MASZYNA', '').strip(),
        'KOD_DETALU': request.args.get('search_KOD_DETALU', '').strip(),
        'NR_FORMY': request.args.get('search_NR_FORMY', '').strip(),
    }

    # Remove empty filters
    search_filters = {k: v for k, v in search_filters.items() if v}

    # Get sort parameters
    sort_field = request.args.get('sort', 'DATA')
    sort_dir = request.args.get('dir', 'desc')

    # Calculate date range
    if date_from and date_to:
        start_date = date_from.replace('-', '')
        end_date = date_to.replace('-', '')
    else:
        end_date = date.today().strftime("%Y%m%d")
        start_date = (date.today() - timedelta(days=days)).strftime("%Y%m%d")

    # Get records
    try:
        records = get_linea_records(start_date, end_date, search_filters, sort_field, sort_dir)

        return jsonify({
            'success': True,
            'records': records,
            'total': len(records)
        })
    except Exception as e:
        print(f"Error in search: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'records': [],
            'total': 0
        }), 500


@linea_bp.route('/api/riparaz/<codice_riparazione>')
def get_riparaz_details(codice_riparazione):
    """AJAX endpoint for fetching repair details."""
    try:
        records = get_stampi_riparaz(codice_riparazione)

        return jsonify({
            'success': True,
            'records': records,
            'codice_riparazione': codice_riparazione,
            'total': len(records)
        })
    except Exception as e:
        print(f"Error fetching riparaz: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'records': [],
            'total': 0
        }), 500
