"""Database connection utilities using raw pyodbc."""
import pyodbc
from contextlib import contextmanager


def get_connection():
    """Create a connection to the Pervasive database."""
    conn = pyodbc.connect(
        "DSN=STAAMP_DB;"
        "ArrayFetchOn=1;"
        "ArrayBufferSize=8;"
        "TransportHint=TCP;"
        "DecimalSymbol=,;",
        readonly=True,
        autocommit=True
    )
    return conn


@contextmanager
def get_cursor():
    """Context manager for database cursor."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        yield cursor
    finally:
        cursor.close()
        conn.close()


def execute_query(query, params=None):
    """Execute a query and return all results as a list of dictionaries."""
    with get_cursor() as cursor:
        cursor.execute(query, params or ())

        # Get column names
        columns = [column[0] for column in cursor.description]

        # Fetch all rows and convert to dictionaries
        results = []
        for row in cursor.fetchall():
            row_dict = {}
            for i, value in enumerate(row):
                # Strip whitespace from string values
                if isinstance(value, str):
                    row_dict[columns[i]] = value.strip()
                else:
                    row_dict[columns[i]] = value
            results.append(row_dict)

        return results


def mosys_to_date(mosys_d):
    """Convert MOSYS date format YYYYMMDD to YYYY/MM/DD."""
    mosys_d = str(mosys_d) if mosys_d else ''
    if len(mosys_d) == 8:
        return f"{mosys_d[:4]}/{mosys_d[4:6]}/{mosys_d[6:8]}"
    return mosys_d


def mosys_godz(mosys_g):
    """Convert MOSYS time format HHMM to HH:MM."""
    mosys_g = str(mosys_g) if mosys_g else ''
    if len(mosys_g) >= 4:
        return f"{mosys_g[:2]}:{mosys_g[2:4]}"
    return mosys_g


def get_stampi_riparaz(codice_riparazione):
    """
    Get repair records for a specific CODICE_RIPARAZIONE.
    Based on get_stampi_riparaz() from functions_old.py
    """
    query = '''
        SELECT RIPARAZ.CODICE_STAMPO, RIPARAZ.COMMESSA, RIPARAZ.CODICE_RIPARAZIONE,
        RIPARAZ.DATA_INIZIO, RIPARAZ.ORA_INIZIO, RIPARAZ.OPER_INIZIO,
        RIPARAZ.STATO_RIPARAZIONE,
        RIPARAZ.NOTE01, RIPARAZ.NOTE02, RIPARAZ.NOTE03, RIPARAZ.NOTE04, RIPARAZ.NOTE05,
        RIPARAZ.NOTE06, RIPARAZ.NOTE07, RIPARAZ.NOTE08, RIPARAZ.NOTE09, RIPARAZ.NOTE10,
        RIPARAZ.DATA_FINE, RIPARAZ.ORA_FINE, RIPARAZ.OPER_FINE,
        RIPARAZ.DATA_COLLAUDO, RIPARAZ.ORA_COLLAUDO, RIPARAZ.OPER_COLLAUDO,
        RIPARAZ.FLAG_FARE_CONTROLLI, RIPARAZ.FLAG_PROVA_URGENTE, RIPARAZ.NUMERO_NONCONF
        FROM STAAMPDB.RIPARAZ RIPARAZ
        WHERE RIPARAZ.CODICE_RIPARAZIONE = ?
        ORDER BY RIPARAZ.DATA_INIZIO ASC
    '''

    rows = execute_query(query, (codice_riparazione,))

    # Transform results
    records = []
    for row in rows:
        # Combine notes
        notes = []
        for i in range(1, 11):
            note_key = f'NOTE0{i}' if i < 10 else f'NOTE{i}'
            note_value = row.get(note_key, '')
            if note_value and str(note_value).strip():
                notes.append(str(note_value).strip())
        uwaga = ' '.join(notes)

        record = {
            'CODICE_STAMPO': row.get('CODICE_STAMPO', '') or '',
            'COMMESSA': row.get('COMMESSA', '') or '',
            'CODICE_RIPARAZIONE': row.get('CODICE_RIPARAZIONE', '') or '',
            'DATA_INIZIO': mosys_to_date(row.get('DATA_INIZIO', '')),
            'ORA_INIZIO': mosys_godz(row.get('ORA_INIZIO', '')),
            'DATA_INIZIO_RAW': row.get('DATA_INIZIO', '') or '',  # Raw for calculations
            'ORA_INIZIO_RAW': row.get('ORA_INIZIO', '') or '',    # Raw for calculations
            'OPER_INIZIO': row.get('OPER_INIZIO', '') or '',
            'STATO_RIPARAZIONE': row.get('STATO_RIPARAZIONE', '') or '',
            'UWAGA': uwaga,
            'DATA_FINE': mosys_to_date(row.get('DATA_FINE', '')),
            'ORA_FINE': mosys_godz(row.get('ORA_FINE', '')),
            'DATA_FINE_RAW': row.get('DATA_FINE', '') or '',      # Raw for calculations
            'ORA_FINE_RAW': row.get('ORA_FINE', '') or '',        # Raw for calculations
            'OPER_FINE': row.get('OPER_FINE', '') or '',
            'DATA_COLLAUDO': mosys_to_date(row.get('DATA_COLLAUDO', '')),
            'ORA_COLLAUDO': mosys_godz(row.get('ORA_COLLAUDO', '')),
            'OPER_COLLAUDO': row.get('OPER_COLLAUDO', '') or '',
            'FLAG_FARE_CONTROLLI': row.get('FLAG_FARE_CONTROLLI', '') or '',
            'FLAG_PROVA_URGENTE': row.get('FLAG_PROVA_URGENTE', '') or '',
            'NUMERO_NONCONF': row.get('NUMERO_NONCONF', '') or ''
        }
        records.append(record)

    return records
