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
	Calculate total quantity of parts blocked (segregated) for a given nr_niezgodnosci.

	Method:
	1. Get all box numbers (NUMERO_CONFEZIONE) from SEGCONF for this nr_niezgodnosci
	2. Get quantities (QT_CONTENUTA) from MAGCONF for each box
	3. Sum all quantities

	Returns: Total quantity of blocked parts
	"""
	if not nr_niezgodnosci:
		return 0

	try:
		# Step 1: Get all box numbers for this NC
		query_boxes = '''
			SELECT SEGCONF.NUMERO_CONFEZIONE
			FROM STAAMPDB.SEGCONF SEGCONF
			WHERE SEGCONF.NUMERO_NON_CONF = ?
		'''
		df_boxes = get_pervasive(query_boxes, (nr_niezgodnosci,))

		if df_boxes.empty:
			return 0

		box_numbers = df_boxes['NUMERO_CONFEZIONE'].tolist()

		# Step 2: Get quantities for all boxes
		placeholders = ','.join(['?' for _ in box_numbers])
		query_qty = f'''
			SELECT MAGCONF.QT_CONTENUTA
			FROM STAAMPDB.MAGCONF MAGCONF
			WHERE MAGCONF.NUMERO_CONFEZIONE IN ({placeholders})
		'''
		df_qty = get_pervasive(query_qty, tuple(box_numbers))

		if df_qty.empty:
			return 0

		# Step 3: Sum all quantities
		total_qty = df_qty['QT_CONTENUTA'].sum()
		return int(total_qty) if total_qty else 0

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
	
	# First query: get DATA and COMMESSA for all nr_niezgodnosci
	placeholders = ','.join(['?' for _ in nr_list])
	query = f'''
		SELECT NOTCOJAN.NUMERO_NC, NOTCOJAN.DATA, NOTCOJAN.COMMESSA
		FROM STAAMPDB.NOTCOJAN NOTCOJAN
		WHERE NOTCOJAN.NUMERO_NC IN ({placeholders})
	'''
	
	try:
		df = get_pervasive(query, tuple(nr_list))
		
		# Build intermediate results
		commessa_to_fetch = set()
		for _, row in df.iterrows():
			nr = row['NUMERO_NC']
			result[nr] = {
				'data_niezgodnosci': parse_mosys_date(row['DATA']),
				'nr_zamowienia': row['COMMESSA'],
				'kod_detalu': None
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
			AND SEGCONF.NUMERO_NON_CONF <> '888888888'
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