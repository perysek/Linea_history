import pandas as pd
import pyodbc
from contextlib import contextmanager
from datetime import datetime

# Centralize the connection string
CONNECTION_STRING = (
	"DSN=STAAMP_DB;ArrayFetchOn=1;ArrayBufferSize=8;TransportHint=TCP;DecimalSymbol=,;;")


@contextmanager
def pervasive_connection(readonly: bool = True):
	"""A context manager for handling database connections."""
	conn_str = f"{CONNECTION_STRING}readonly={'True' if readonly else 'False'};"
	conn = None
	try:
		conn = pyodbc.connect(conn_str)
		yield conn
	except pyodbc.Error as e:
		print(f"Database connection error: {e}")
		raise
	finally:
		if conn:
			conn.close()


def get_pervasive(query: str, params: tuple = None) -> pd.DataFrame:
	"""Executes a read-only query and returns a cleaned pandas DataFrame."""
	with pervasive_connection(readonly=True) as conn:
		df = pd.read_sql(query, conn, params=params)
	
	# More efficient whitespace stripping
	for col in df.select_dtypes(include=['object']).columns:
		df[col] = df[col].str.strip()
	
	return df


def parse_mosys_date(date_value):
	"""Parse MOSYS date (YYYYMMDD format) to Python date object."""
	if date_value is None:
		return None
	if isinstance(date_value, str):
		date_str = date_value.strip()
		if len(date_str) == 8 and date_str.isdigit():
			# YYYYMMDD format
			try:
				return datetime.strptime(date_str, '%Y%m%d').date()
			except:
				return None
		elif '-' in date_str:
			# YYYY-MM-DD format
			try:
				return datetime.strptime(date_str, '%Y-%m-%d').date()
			except:
				return None
	return None


def get_niezgodnosc_details(nr_niezgodnosci: str) -> dict:
	"""
	Get data_niezgodnosci and nr_zamowienia for a single nr_niezgodnosci.
	Returns: {'data_niezgodnosci': date or None, 'nr_zamowienia': str or None}
	"""
	if not nr_niezgodnosci:
		return {'data_niezgodnosci': None, 'nr_zamowienia': None}
	
	query = '''
		SELECT NOTCOJAN.DATA, NOTCOJAN.COMMESSA
		FROM STAAMPDB.NOTCOJAN NOTCOJAN
		WHERE NOTCOJAN.NUMERO_NC = ?
	'''
	try:
		df = get_pervasive(query, (nr_niezgodnosci,))
		if not df.empty:
			data = parse_mosys_date(df.iloc[0]['DATA'])
			return {
				'data_niezgodnosci': data,
				'nr_zamowienia': df.iloc[0]['COMMESSA']
			}
	except Exception as e:
		print(f"Error fetching niezgodnosc details for {nr_niezgodnosci}: {e}")
	
	return {'data_niezgodnosci': None, 'nr_zamowienia': None}


# noinspection D
def get_nc_history(nr_niezgodnosci: str) -> list:
	"""
	Get history of updates for a given nr_niezgodnosci.
	Returns: list of dicts with keys: data_wpisu, godzina_wpisu, tekst_wpisu, typ_uwagi
	"""
	if not nr_niezgodnosci:
		return []
	
	# Strip whitespace from the parameter
	nr_niezgodnosci = str(nr_niezgodnosci).strip()
	
	query = '''
		SELECT NOTCOJAN.DATA, NOTCOJAN.ORA,
		NOTCOJAN.NOTE_01, NOTCOJAN.NOTE_02, NOTCOJAN.NOTE_03, NOTCOJAN.NOTE_04, NOTCOJAN.NOTE_05,
		NOTCOJAN.NOTE_06, NOTCOJAN.NOTE_07, NOTCOJAN.NOTE_08, NOTCOJAN.NOTE_09, NOTCOJAN.NOTE_10,
		NOTCOJAN.TIPO_NOTA
		FROM STAAMPDB.NOTCOJAN NOTCOJAN
		WHERE NOTCOJAN.NUMERO_NC = ?
		ORDER BY NOTCOJAN.DATA ASC, NOTCOJAN.ORA ASC
	'''
	try:
		df = get_pervasive(query, (nr_niezgodnosci,))
		if df.empty:
			return []
		
		history = []
		for _, row in df.iterrows():
			# Join all NOTE columns with space separator
			# Try both NOTE_01 and NOTE01 formats
			notes = []
			for i in range(1, 11):
				note = None
				# Try different column name formats
				for col_name in [f'NOTE_{i:02d}', f'NOTE{i:02d}', f'NOTE_{i}', f'NOTE{i}']:
					if col_name in row.index:
						note = row[col_name]
						break
				if note and str(note).strip():
					notes.append(str(note).strip())
			tekst = ' '.join(notes)
			
			# Parse date
			data_wpisu = parse_mosys_date(row['DATA'])
			
			# Format time (ORA might be HHMM or HHMMSS format)
			godzina = row.get('ORA', '')
			if godzina and len(str(godzina)) >= 4:
				godzina_str = str(godzina).zfill(6)[:4]
				godzina_wpisu = f"{godzina_str[:2]}:{godzina_str[2:4]}"
			else:
				godzina_wpisu = '-'
			
			history.append({
				'data_wpisu': data_wpisu,
				'godzina_wpisu': godzina_wpisu,
				'tekst_wpisu': tekst,
				'typ_uwagi': row.get('TIPO_NOTA', '')
			})
		
		return history
	except Exception as e:
		print(f"Error fetching NC history for {nr_niezgodnosci}: {e}")
		return []

def get_part_number(nr_zamowienia: str) -> str:
	"""
	Get kod_detalu (part number) for a given production order.
	Returns: part_number string or None
	"""
	if not nr_zamowienia:
		return None
	
	query = '''
		SELECT COLLAUDO.ARTICOLO
		FROM STAAMPDB.COLLAUDO COLLAUDO
		WHERE COLLAUDO.COMMESSA = ?
	'''
	try:
		df = get_pervasive(query, (nr_zamowienia,))
		if not df.empty:
			return df.iloc[0]['ARTICOLO']
	except Exception as e:
		print(f"Error fetching part number for {nr_zamowienia}: {e}")
	
	return None


def get_blocked_parts_qty(nr_niezgodnosci: str) -> int:
	"""
	Calculate total quantity of parts currently blocked for a given nr_niezgodnosci.

	Uses a single JOIN between SEGCONF and MAGCONF and sums
	(QT_CONTENUTA - QT_PRELEV) — the net remaining quantity in each box —
	excluding boxes that have been fully withdrawn (net qty <= 0).

	Returns: Total net quantity of blocked parts
	"""
	if not nr_niezgodnosci:
		return 0

	query = '''
		SELECT SUM(MAGCONF.QT_CONTENUTA - MAGCONF.QT_PRELEV) AS TOTAL
		FROM STAAMPDB.SEGCONF SEGCONF
		INNER JOIN STAAMPDB.MAGCONF MAGCONF
			ON SEGCONF.NUMERO_CONFEZIONE = MAGCONF.NUMERO_CONFEZIONE
		WHERE SEGCONF.NUMERO_NON_CONF = ?
		  AND (MAGCONF.QT_CONTENUTA - MAGCONF.QT_PRELEV) > 0
	'''
	try:
		df = get_pervasive(query, (nr_niezgodnosci,))
		if df.empty:
			return 0
		total = df.iloc[0]['TOTAL_BLOCKED']
		return int(total) if total else 0
	except Exception as e:
		print(f"Error calculating blocked parts qty for {nr_niezgodnosci}: {e}")
		return 0


def get_batch_niezgodnosc_details(nr_niezgodnosci_list: list) -> dict:
	"""
	Batch fetch data for multiple nr_niezgodnosci values.
	Returns: {nr_niezgodnosci: {'data_niezgodnosci': date, 'nr_zamowienia': str, 'kod_detalu': str}}
	"""
	if not nr_niezgodnosci_list:
		return {}
	
	# Filter out empty/None values
	nr_list = [nr for nr in nr_niezgodnosci_list if nr]
	if not nr_list:
		return {}
	
	result = {}

	# First query: get DATA, COMMESSA and NC description notes (TIPO_NOTA='NC' = initial description entry)
	placeholders = ','.join(['?' for _ in nr_list])
	query = f'''
		SELECT NOTCOJAN.NUMERO_NC, NOTCOJAN.DATA, NOTCOJAN.COMMESSA,
		NOTCOJAN.NOTE_01, NOTCOJAN.NOTE_02, NOTCOJAN.NOTE_03, NOTCOJAN.NOTE_04, NOTCOJAN.NOTE_05,
		NOTCOJAN.NOTE_06, NOTCOJAN.NOTE_07, NOTCOJAN.NOTE_08, NOTCOJAN.NOTE_09, NOTCOJAN.NOTE_10
		FROM STAAMPDB.NOTCOJAN NOTCOJAN
		WHERE NOTCOJAN.NUMERO_NC IN ({placeholders})
		AND NOTCOJAN.TIPO_NOTA = 'NC'
		ORDER BY NOTCOJAN.DATA ASC, NOTCOJAN.ORA ASC
	'''

	try:
		df = get_pervasive(query, tuple(nr_list))

		# Build intermediate results — take first row per NC (oldest entry = NC description)
		commessa_to_fetch = set()
		for _, row in df.iterrows():
			nr = row['NUMERO_NC']
			if nr not in result:
				# Parse notes into description text
				notes = []
				for i in range(1, 11):
					note = None
					for col_name in [f'NOTE_{i:02d}', f'NOTE{i:02d}', f'NOTE_{i}', f'NOTE{i}']:
						if col_name in row.index:
							note = row[col_name]
							break
					if note and str(note).strip():
						notes.append(str(note).strip())
				result[nr] = {
					'data_niezgodnosci': parse_mosys_date(row['DATA']),
					'nr_zamowienia': row['COMMESSA'],
					'kod_detalu': None,
					'opis_niezgodnosci': ' '.join(notes),
				}
			if row['COMMESSA']:
				commessa_to_fetch.add(row['COMMESSA'])
		
		# Second query: get ARTICOLO for all COMMESSA values
		if commessa_to_fetch:
			placeholders = ','.join(['?' for _ in commessa_to_fetch])
			query = f'''
				SELECT COLLAUDO.COMMESSA, COLLAUDO.ARTICOLO
				FROM STAAMPDB.COLLAUDO COLLAUDO
				WHERE COLLAUDO.COMMESSA IN ({placeholders})
			'''
			df_parts = get_pervasive(query, tuple(commessa_to_fetch))
			
			# Map COMMESSA to ARTICOLO
			commessa_to_articolo = dict(zip(df_parts['COMMESSA'], df_parts['ARTICOLO']))
			
			# Update results with kod_detalu
			for nr, data in result.items():
				if data['nr_zamowienia'] in commessa_to_articolo:
					data['kod_detalu'] = commessa_to_articolo[data['nr_zamowienia']]
	
	except Exception as e:
		print(f"Error in batch fetch: {e}")
	
	return result


# noinspection D
def get_all_blocked_parts() -> list:
	"""
	Get all part numbers currently blocked (segregated) in MOSYS.

	Steps:
	1. Query SEGCONF joined with MAGCONF to get all NC numbers with blocked qty, box count, and date range
	2. Batch-fetch data_niezgodnosci, nr_zamowienia, kod_detalu for all NC numbers
	3. For each NC, get the first history entry text as "Opis niezgodnosci"

	Returns: list of dicts with keys:
		kod_detalu, nr_niezgodnosci, data_niezgodnosci, opis_niezgodnosci,
		ilosc_opakowan, ilosc_zablokowanych, data_produkcji_min, data_produkcji_max
	"""
	# Step 1: Get all NC numbers with their total blocked quantities, box count, and production date range
	query = '''
		SELECT SEGCONF.NUMERO_NON_CONF,
		       COUNT(DISTINCT MAGCONF.NUMERO_CONFEZIONE) AS BOX_COUNT,
		       SUM(MAGCONF.QT_CONTENUTA - MAGCONF.QT_PRELEV) AS TOTAL_QTY,
		       MIN(MAGCONF.DATA_CARICO) AS MIN_DATE,
		       MAX(MAGCONF.DATA_CARICO) AS MAX_DATE
		FROM STAAMPDB.SEGCONF SEGCONF
		INNER JOIN STAAMPDB.MAGCONF MAGCONF
			ON SEGCONF.NUMERO_CONFEZIONE = MAGCONF.NUMERO_CONFEZIONE
		WHERE SEGCONF.NUMERO_NON_CONF > 202300000
		GROUP BY SEGCONF.NUMERO_NON_CONF
	'''
	df = get_pervasive(query)

	if df.empty:
		return []

	# Build initial list with NC numbers, quantities, box counts, and date ranges
	nc_data_map = {}
	for _, row in df.iterrows():
		nc = row['NUMERO_NON_CONF']
		qty = int(row['TOTAL_QTY']) if row['TOTAL_QTY'] else 0
		box_count = int(row['BOX_COUNT']) if row['BOX_COUNT'] else 0
		min_date = parse_mosys_date(row['MIN_DATE'])
		max_date = parse_mosys_date(row['MAX_DATE'])

		if qty > 0:
			nc_data_map[nc] = {
				'qty': qty,
				'box_count': box_count,
				'min_date': min_date,
				'max_date': max_date
			}

	if not nc_data_map:
		return []

	nc_list = list(nc_data_map.keys())

	# Step 2: Batch-fetch details (data_niezgodnosci, nr_zamowienia, kod_detalu)
	details = get_batch_niezgodnosc_details(nc_list)

	# Step 3: Get first history entry text for each NC
	placeholders = ','.join(['?' for _ in nc_list])
	query_notes = f'''
		SELECT NOTCOJAN.NUMERO_NC, NOTCOJAN.DATA, NOTCOJAN.ORA,
		NOTCOJAN.NOTE_01, NOTCOJAN.NOTE_02, NOTCOJAN.NOTE_03, NOTCOJAN.NOTE_04, NOTCOJAN.NOTE_05,
		NOTCOJAN.NOTE_06, NOTCOJAN.NOTE_07, NOTCOJAN.NOTE_08, NOTCOJAN.NOTE_09, NOTCOJAN.NOTE_10
		FROM STAAMPDB.NOTCOJAN NOTCOJAN
		WHERE NOTCOJAN.NUMERO_NC IN ({placeholders})
		ORDER BY NOTCOJAN.DATA ASC, NOTCOJAN.ORA ASC
	'''
	first_notes = {}
	try:
		df_notes = get_pervasive(query_notes, tuple(nc_list))
		if not df_notes.empty:
			# Group by NC and take first row for each
			for nc in nc_list:
				nc_rows = df_notes[df_notes['NUMERO_NC'] == nc]
				if not nc_rows.empty:
					row = nc_rows.iloc[0]
					notes = []
					for i in range(1, 11):
						note = None
						for col_name in [f'NOTE_{i:02d}', f'NOTE{i:02d}', f'NOTE_{i}', f'NOTE{i}']:
							if col_name in row.index:
								note = row[col_name]
								break
						if note and str(note).strip():
							notes.append(str(note).strip())
					first_notes[nc] = ' '.join(notes)
	except Exception as e:
		print(f"Error fetching first notes for blocked parts: {e}")

	# Combine everything
	results = []
	for nc in nc_list:
		detail = details.get(nc, {})
		nc_data = nc_data_map[nc]
		results.append({
			'kod_detalu': detail.get('kod_detalu', ''),
			'nr_niezgodnosci': nc,
			'data_niezgodnosci': detail.get('data_niezgodnosci'),
			'opis_niezgodnosci': first_notes.get(nc, ''),
			'ilosc_opakowan': nc_data['box_count'],
			'ilosc_zablokowanych': nc_data['qty'],
			'data_produkcji_min': nc_data['min_date'],
			'data_produkcji_max': nc_data['max_date'],
		})

	return results


def get_blocked_parts_by_part_code() -> list:
	"""
	Get stock summary grouped by part code (CODICE_ARTICOLO).

	Two server-side GROUP BY queries minimise ODBC data transfer:
	  Q1 (INNER JOIN, server GROUP BY) → w_tym_zabl per part code + list of blocked part codes
	  Q2 (MAGCONF only, IN clause,    → na_stanie per part code
	       server GROUP BY)

	Only part codes that have at least some blocked stock are returned.

	Returns list of dicts with keys: kod_detalu, na_stanie, w_tym_zabl, w_tym_dostep
	"""
	import time
	T = time.perf_counter

	t0 = T()

	# ── Q1: blocked qty per part code ──────────────────────────────────────
	# INNER JOIN — only boxes linked to a 2023+ NC.  GROUP BY happens on the
	# server so only one aggregated row per part code crosses the ODBC wire.
	query_blocked = '''
		SELECT MAGCONF.CODICE_ARTICOLO,
		       SUM(MAGCONF.QT_CONTENUTA - MAGCONF.QT_PRELEV) AS W_TYM_ZABL
		FROM STAAMPDB.SEGCONF SEGCONF
		INNER JOIN STAAMPDB.MAGCONF MAGCONF
			ON MAGCONF.NUMERO_CONFEZIONE = SEGCONF.NUMERO_CONFEZIONE
		WHERE SEGCONF.NUMERO_NON_CONF > 202300000
		  AND (MAGCONF.QT_CONTENUTA - MAGCONF.QT_PRELEV) > 0
		  AND MAGCONF.BOX_Z <> '055'
		GROUP BY MAGCONF.CODICE_ARTICOLO
	'''
	try:
		df_blocked = get_pervasive(query_blocked)
	except Exception as e:
		print(f"[get_blocked_parts_by_part_code] Q1 error: {e}")
		return []

	t1 = T()
	print(f"[TIMER] Q1 blocked (INNER JOIN + GROUP BY): {(t1 - t0) * 1000:.1f}ms — {len(df_blocked)} blocked part codes")

	if df_blocked.empty:
		return []

	part_codes = df_blocked['CODICE_ARTICOLO'].tolist()
	blocked_map = {row['CODICE_ARTICOLO']: int(row['W_TYM_ZABL'] or 0)
	               for _, row in df_blocked.iterrows()}

	t2 = T()
	print(f"[TIMER] build blocked_map: {(t2 - t1) * 1000:.1f}ms")

	# ── Q2: total stock for only the blocked part codes ─────────────────────
	# IN clause limits MAGCONF scan to relevant part codes only.
	# GROUP BY on server — one row per part code over ODBC.
	placeholders = ','.join(['?' for _ in part_codes])
	query_stock = f'''
		SELECT MAGCONF.CODICE_ARTICOLO,
		       SUM(MAGCONF.QT_CONTENUTA - MAGCONF.QT_PRELEV) AS NA_STANIE
		FROM STAAMPDB.MAGCONF MAGCONF
		WHERE (MAGCONF.QT_CONTENUTA - MAGCONF.QT_PRELEV) > 0
		  AND MAGCONF.BOX_Z <> '055'
		  AND MAGCONF.CODICE_ARTICOLO IN ({placeholders})
		GROUP BY MAGCONF.CODICE_ARTICOLO
	'''
	try:
		df_stock = get_pervasive(query_stock, tuple(part_codes))
		stock_map = {row['CODICE_ARTICOLO']: int(row['NA_STANIE'] or 0)
		             for _, row in df_stock.iterrows()}
	except Exception as e:
		print(f"[get_blocked_parts_by_part_code] Q2 error: {e}")
		stock_map = {}

	t3 = T()
	print(f"[TIMER] Q2 total stock (IN + GROUP BY): {(t3 - t2) * 1000:.1f}ms — {len(stock_map)} rows")

	# ── Build result list ────────────────────────────────────────────────────
	results = []
	for part_code, zabl in blocked_map.items():
		na = stock_map.get(part_code, 0)
		results.append({
			'kod_detalu': part_code,
			'na_stanie': na,
			'w_tym_zabl': zabl,
			'w_tym_dostep': max(0, na - zabl),
		})

	t4 = T()
	print(f"[TIMER] build results list: {(t4 - t3) * 1000:.1f}ms — {len(results)} rows")
	print(f"[TIMER] TOTAL get_blocked_parts_by_part_code: {(t4 - t0) * 1000:.1f}ms")

	return results


# noinspection D
def get_blocked_boxes_details(nr_niezgodnosci: str) -> list:
	"""
	Get detailed information about all boxes for a specific NC number.

	Returns: list of dicts with keys:
		numero_confezione, data_carico, oper_carico, qt_blocked,
		box_x, box_y, box_z
	"""
	if not nr_niezgodnosci:
		return []

	query = '''
		SELECT MAGCONF.NUMERO_CONFEZIONE,
		       MAGCONF.DATA_CARICO,
		       MAGCONF.OPER_CARICO,
		       (MAGCONF.QT_CONTENUTA - MAGCONF.QT_PRELEV) AS QT_BLOCKED,
		       MAGCONF.BOX_X,
		       MAGCONF.BOX_Y,
		       MAGCONF.BOX_Z
		FROM STAAMPDB.SEGCONF SEGCONF
		INNER JOIN STAAMPDB.MAGCONF MAGCONF
			ON SEGCONF.NUMERO_CONFEZIONE = MAGCONF.NUMERO_CONFEZIONE
		WHERE SEGCONF.NUMERO_NON_CONF = ?
		  AND (MAGCONF.QT_CONTENUTA - MAGCONF.QT_PRELEV) > 0
		ORDER BY MAGCONF.DATA_CARICO DESC
	'''

	try:
		df = get_pervasive(query, (nr_niezgodnosci,))
		if df.empty:
			return []

		results = []
		for _, row in df.iterrows():
			# Format date from YYYYMMDD to YYYY/MM/DD
			date_carico = parse_mosys_date(row['DATA_CARICO'])
			date_str = date_carico.strftime('%Y/%m/%d') if date_carico else '-'

			# Format location as X|Y|Z
			box_x = str(row['BOX_X']).strip() if row['BOX_X'] else ''
			box_y = str(row['BOX_Y']).strip() if row['BOX_Y'] else ''
			box_z = str(row['BOX_Z']).strip() if row['BOX_Z'] else ''
			location = f"{box_x}|{box_y}|{box_z}" if any([box_x, box_y, box_z]) else '-'

			results.append({
				'numero_confezione': row['NUMERO_CONFEZIONE'],
				'data_carico': date_str,
				'oper_carico': row['OPER_CARICO'] if row['OPER_CARICO'] else '-',
				'qt_blocked': int(row['QT_BLOCKED']) if row['QT_BLOCKED'] else 0,
				'location': location,
			})

		return results
	except Exception as e:
		print(f"Error fetching blocked boxes details for {nr_niezgodnosci}: {e}")
		return []


# ── MATLOT – incoming raw material inspection ─────────────────────────────────

def get_matlot_batches() -> pd.DataFrame:
	"""Fetch all MATLOT batches from MOSYS including their current release status.

	MOSYS is read-only from LINEA's perspective. The returned LOTTO_VERIFICATO
	value is used only when seeding a new row into matlot_tracking — it is never
	written back to MOSYS during a normal sync.

	Returns a DataFrame with columns:
	    CODICE_MATERIALE, LOTTO, GIACENZA_LOTTO, BOX_X, BOX_Y, BOX_Z,
	    LOTTO_VERIFICATO, NOME_COMMERCIALE, INSERTI_DESCRIZIONE
	Returns an empty DataFrame on error.
	"""
	query = """
		SELECT
			m.CODICE_MATERIALE,
			m.LOTTO,
			m.GIACENZA_LOTTO,
			m.BOX_X,
			m.BOX_Y,
			m.BOX_Z,
			m.LOTTO_VERIFICATO,
			p.NOME_COMMERCIALE,
			i.DESCRIZIONE AS INSERTI_DESCRIZIONE
		FROM STAAMPDB.MATLOT m
		LEFT JOIN STAAMPDB.MATPRI  p ON p.CODICE = m.CODICE_MATERIALE
		LEFT JOIN STAAMPDB.INSERTI i ON i.CODICE = m.CODICE_MATERIALE
		ORDER BY m.CODICE_MATERIALE, m.LOTTO
	"""
	try:
		return get_pervasive(query)
	except Exception as e:
		print(f"Error fetching MATLOT batches: {e}")
		return pd.DataFrame()


def get_matlot_verified_batches() -> set:
	"""Return the set of (CODICE_MATERIALE, LOTTO) pairs where LOTTO_VERIFICATO = 'S'.

	Used by _sync_from_mosys drift-correction to detect batches that MOSYS considers
	released but LINEA's matlot_tracking still has as pending (release_status='N').

	Returns:
	    set of (codice_materiale, lotto) tuples — empty set on error or no results.
	"""
	query = """
		SELECT CODICE_MATERIALE, LOTTO
		FROM STAAMPDB.MATLOT
		WHERE LOTTO_VERIFICATO = 'S'
	"""
	try:
		df = get_pervasive(query)
		if df.empty:
			return set()
		return {
			(str(r['CODICE_MATERIALE']).strip(), str(r['LOTTO']).strip())
			for _, r in df.iterrows()
		}
	except Exception as e:
		print(f"Error fetching verified MATLOT batches: {e}")
		return set()


def auto_approve_matlot_batches() -> int:
	"""Bulk-set LOTTO_VERIFICATO='S' in MOSYS for batches matching the auto-approve rule.

	Auto-approve criteria (applied together):
	    - CODICE_MATERIALE starts with 't' (case-insensitive)
	    - LOTTO starts with '0AV' OR '0BU'
	    - Current LOTTO_VERIFICATO = 'N'  (avoids unnecessary writes)

	These batches are auto-released in MOSYS and excluded from the SQLite
	matlot_tracking table — they are not displayed or tracked in LINEA.

	Returns:
	    Number of MOSYS rows updated (0 on error or when nothing to update).
	"""
	query = """
		UPDATE STAAMPDB.MATLOT
		SET LOTTO_VERIFICATO = 'S'
		WHERE LOTTO_VERIFICATO = 'N'
		  AND LOWER(CODICE_MATERIALE) LIKE 't%'
		  AND (LOTTO LIKE '0AV%' OR LOTTO LIKE '0BU%')
	"""
	try:
		with pervasive_connection(readonly=False) as conn:
			cursor = conn.cursor()
			cursor.execute(query)
			count = cursor.rowcount
			conn.commit()
		return count
	except Exception as e:
		print(f"Error auto-approving MATLOT batches: {e}")
		return 0


def get_insert_codes() -> set:
	"""Return the set of all CODICE values from STAAMPDB.INSERTI.

	Used by _ensure_caches_loaded() in matlot.py to populate _insert_codes
	immediately after a server restart without requiring a full sync.

	Returns:
	    set of codice strings (stripped); empty set on error.
	"""
	query = "SELECT CODICE FROM STAAMPDB.INSERTI"
	try:
		df = get_pervasive(query)
		if df is None or df.empty:
			return set()
		return {str(row.get('CODICE') or '').strip()
		        for _, row in df.iterrows()
		        if str(row.get('CODICE') or '').strip()}
	except Exception as e:
		print(f"Error fetching INSERTI codes: {e}")
		return set()


def get_insert_names() -> dict:
	"""Return a dict of {codice: name} for all INSERTI records.

	Uses a direct SELECT (no JOIN) so that Python-side stripping handles any
	fixed-length CHAR padding that would silently break a SQL JOIN equality.

	Tries DESCRIZIONE first; falls back to NOME_COMMERCIALE if DESCRIZIONE
	is absent or yields no results (different MOSYS installations may differ).

	Returns:
	    dict mapping stripped codice strings to their display names; {} on error.
	"""
	for name_col in ('DESCRIZIONE', 'NOME_COMMERCIALE'):
		try:
			query = f"SELECT CODICE, {name_col} AS NOME FROM STAAMPDB.INSERTI"
			df = get_pervasive(query)
			if df is None or df.empty:
				continue
			result = {}
			for _, row in df.iterrows():
				codice = str(row.get('CODICE') or '').strip()
				nome   = str(row.get('NOME')   or '').strip()
				if codice and nome:
					result[codice] = nome
			if result:
				return result
		except Exception:
			continue
	return {}


def _is_mosys_writable(codice_materiale: str) -> bool:
	"""Return True if MOSYS LOTTO_VERIFICATO should be kept in sync for this material.

	Only materials whose CODICE_MATERIALE starts with a tracked prefix are written
	back to MOSYS.  All other material types are managed externally and must not
	have their LOTTO_VERIFICATO overwritten by LINEA.

	Tracked prefixes:
	    't' / 'T'  — surowce (raw materials, case-insensitive)
	    'I'        — inserty (inserts, case-sensitive capital I)
	    'HPR'      — HPR materials (case-sensitive)
	"""
	c = (codice_materiale or '').strip()
	return (
		c.lower().startswith('t')
		or c.startswith('I')
		or c.startswith('HPR')
	)


def update_matlot_lotto_status(codice_materiale: str, lotto: str, new_status: str) -> bool:
	"""Write release status back to MOSYS MATLOT.LOTTO_VERIFICATO.

	Called as a parallel write alongside the primary SQLite update — the SQLite
	matlot_tracking.release_status remains the source of truth. This write keeps
	MOSYS in sync for downstream systems that read LOTTO_VERIFICATO.

	Skips the write silently for materials not in the tracked-prefix list
	(see _is_mosys_writable).  Returns True in that case so callers treat it as
	a no-op success rather than an error.

	Args:
	    codice_materiale: raw material code (CODICE_MATERIALE)
	    lotto:            batch number (LOTTO)
	    new_status:       new status value ('S' or 'N')

	Returns:
	    True on success or skip, False on failure (caller should log but not block).
	"""
	if not _is_mosys_writable(codice_materiale):
		return True  # silently skip — not a LINEA-managed material type

	query = """
		UPDATE STAAMPDB.MATLOT
		SET LOTTO_VERIFICATO = ?
		WHERE CODICE_MATERIALE = ? AND LOTTO = ?
	"""
	try:
		with pervasive_connection(readonly=False) as conn:
			cursor = conn.cursor()
			cursor.execute(query, (new_status, codice_materiale, lotto))
			conn.commit()
		return True
	except Exception as e:
		print(f"Error updating MATLOT.LOTTO_VERIFICATO for {codice_materiale}/{lotto}: {e}")
		return False