"""
Excel data synchronization utility.

Automatically checks for and imports new rows from the Excel file
into the database when the sorting results page is loaded.
"""

import os
import time
import pandas as pd
import openpyxl
import re
from datetime import datetime
from flask import current_app
from app import db
from app.models.sorting_area import DaneRaportu, BrakiDefektyRaportu, Operator
from MOSYS_data_functions import get_batch_niezgodnosc_details


# Simple in-memory cache to track last sync time
_last_sync_time = 0
_sync_interval = 300  # 5 minutes (in seconds)


def parse_defects_from_uwagi(uwagi_text):
    """
    Parse defects from the Uwagi column.
    Example: "działania korygujące na pęcherze x52, nadpalenia x0"
    Returns list of tuples: [(defect_name, count), ...]
    """
    if not uwagi_text:
        return []

    defects = []
    # Pattern to match defects like "pęcherze x52" or "nadpalenia x0"
    pattern = r'([a-zA-ZąćęłńóśźżĄĆĘŁŃÓŚŹŻ\s]+)\s*x\s*(\d+)'
    matches = re.findall(pattern, uwagi_text)

    for defect_name, count in matches:
        defect_name = defect_name.strip()
        count = int(count)
        if count > 0:  # Only add defects with count > 0
            defects.append((defect_name, count))

    return defects


def convert_to_boolean(value):
    """Convert Excel value to boolean for 'Selekcja na bieżąco' field."""
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ('x', 'tak', 'yes', 'true', '1')
    return bool(value)


def sync_new_excel_data(force=False):
    """
    Check for new rows in Excel and import them into the database.

    Uses a time-based cache to avoid checking on every request.
    Only syncs if more than _sync_interval seconds have passed since last sync.

    Args:
        force: If True, bypass the time cache and force a sync check

    Returns:
        dict: Statistics about the sync operation
            - checked: bool, whether sync was performed
            - new_records: int, number of new records imported
            - errors: int, number of errors encountered
            - message: str, status message
    """
    global _last_sync_time

    current_time = time.time()

    # Check if we should skip this sync (too soon since last sync)
    if not force and (current_time - _last_sync_time) < _sync_interval:
        time_until_next = int(_sync_interval - (current_time - _last_sync_time))
        return {
            'checked': False,
            'new_records': 0,
            'errors': 0,
            'message': f'Skipped sync (next check in {time_until_next}s)'
        }

    # Update last sync time
    _last_sync_time = current_time

    # Excel file configuration
    excel_file = r'G:\DOCUMENT\qualita\System Zarządzania Jakością\Cele jakościowe\PPM wewnętrzny koszty złej jakości (2023).xlsm'
    sheet_name = 'dane'

    # Column mapping (Excel columns are 1-indexed)
    COL_LP = 1  # nr_raportu
    COL_DATA_NIEZGODNOSCI = 2
    COL_NR_NIEZGODNOSCI = 5
    COL_SELEKCJA_NA_BIEZACO = 7
    COL_ILOSC_SPRAWDZONYCH = 9
    COL_ILOSC_WADLIWYCH = 11
    COL_ZALECANA_WYDAJNOSC = 13
    COL_CZAS_PRACY = 19
    COL_UWAGI = 21
    COL_UWAGI_DO_WYDAJNOSCI = 22
    COL_DATA_SELEKCJI = 26

    # Fixed values
    OPERATOR_ID = 2
    NR_INSTRUKCJI = 'wg raportu'

    try:
        # Check if Excel file exists
        if not os.path.exists(excel_file):
            return {
                'checked': True,
                'new_records': 0,
                'errors': 1,
                'message': f'Excel file not found: {excel_file}'
            }

        # Step 1: Load existing (nr_raportu, nr_niezgodnosci) combinations from database
        current_app.logger.info('Loading existing records from database...')
        existing_records = db.session.query(
            DaneRaportu.nr_raportu,
            DaneRaportu.nr_niezgodnosci
        ).all()

        # Create a set of tuples for fast lookup
        existing_keys = {(str(r.nr_raportu), str(r.nr_niezgodnosci)) for r in existing_records}
        current_app.logger.info(f'Found {len(existing_keys)} existing records in database')

        # Step 2: Load Excel file with proper cleanup
        current_app.logger.info(f'Loading Excel file: {excel_file}')
        excel_data = None
        total_rows = 0

        try:
            # Use openpyxl with explicit file handling for better resource management
            # Load workbook with data_only=True to get cached formula values
            # This ensures the file is properly closed after reading
            wb = openpyxl.load_workbook(excel_file, read_only=True, data_only=True)
            try:
                ws = wb[sheet_name]

                # Read all data into memory
                excel_data = [None]  # Add None at index 0 so excel_data[1] = Excel row 1
                for row in ws.iter_rows(values_only=True):
                    excel_data.append(list(row))

                total_rows = len(excel_data) - 1
                current_app.logger.info(f'Loaded {total_rows} rows from Excel')
            finally:
                # Explicitly close the workbook to release file handle
                wb.close()
                current_app.logger.debug('Excel file closed successfully')

        except Exception as e:
            current_app.logger.error(f'Error loading Excel file: {e}')
            return {
                'checked': True,
                'new_records': 0,
                'errors': 1,
                'message': f'Failed to load Excel: {str(e)}'
            }

        # Step 3: Scan Excel for new rows (skip header row 1, start from row 2)
        new_rows = []
        nr_niezgodnosci_list = []

        current_app.logger.info('Scanning Excel for new records...')
        for row_num in range(2, len(excel_data)):  # Start from row 2 (skip header)
            try:
                row = excel_data[row_num]

                # Convert pandas NaN to None
                row = [None if (isinstance(cell, float) and pd.isna(cell)) else cell for cell in row]

                # Extract key fields
                nr_raportu = str(row[COL_LP - 1]) if row[COL_LP - 1] is not None else None
                nr_niezgodnosci = row[COL_NR_NIEZGODNOSCI - 1]

                # Skip if nr_niezgodnosci is empty
                if not nr_niezgodnosci:
                    continue

                nr_niezgodnosci = str(nr_niezgodnosci).strip()

                # Check if this combination already exists in database
                key = (nr_raportu, nr_niezgodnosci)
                if key in existing_keys:
                    continue  # Already in database, skip

                # This is a new row!
                nr_niezgodnosci_list.append(nr_niezgodnosci)

                # Extract all data for this row
                row_data = {
                    'row_num': row_num,
                    'nr_raportu': nr_raportu,
                    'nr_niezgodnosci': nr_niezgodnosci,
                    'selekcja_na_biezaco': convert_to_boolean(row[COL_SELEKCJA_NA_BIEZACO - 1]),
                    'ilosc_detali_sprawdzonych': row[COL_ILOSC_SPRAWDZONYCH - 1],
                    'ilosc_wadliwych': row[COL_ILOSC_WADLIWYCH - 1],
                    'zalecana_wydajnosc': row[COL_ZALECANA_WYDAJNOSC - 1],
                    'czas_pracy': row[COL_CZAS_PRACY - 1],
                    'uwagi': row[COL_UWAGI - 1],
                    'uwagi_do_wydajnosci': row[COL_UWAGI_DO_WYDAJNOSCI - 1],
                    'data_selekcji': row[COL_DATA_SELEKCJI - 1],
                }

                # Convert datetime to date for data_selekcji
                if isinstance(row_data['data_selekcji'], datetime):
                    row_data['data_selekcji'] = row_data['data_selekcji'].date()

                new_rows.append(row_data)

            except Exception as e:
                current_app.logger.error(f'Error reading Excel row {row_num}: {e}')
                continue

        if not new_rows:
            return {
                'checked': True,
                'new_records': 0,
                'errors': 0,
                'message': 'No new records found in Excel'
            }

        current_app.logger.info(f'Found {len(new_rows)} new records to import')

        # Step 4: Batch fetch MOSYS data for all new records
        current_app.logger.info(f'Fetching MOSYS data for {len(nr_niezgodnosci_list)} NC numbers...')
        try:
            mosys_data = get_batch_niezgodnosc_details(nr_niezgodnosci_list)
            current_app.logger.info(f'Successfully fetched MOSYS data for {len(mosys_data)} records')
        except Exception as e:
            current_app.logger.error(f'Error fetching MOSYS data: {e}')
            mosys_data = {}

        # Step 5: Import new records into database
        current_app.logger.info('Importing new records into database...')
        imported_count = 0
        error_count = 0

        for record in new_rows:
            try:
                nr_niezg = record['nr_niezgodnosci']

                # Get MOSYS data for this record
                mosys_info = mosys_data.get(nr_niezg, {})
                data_niezgodnosci = mosys_info.get('data_niezgodnosci')
                nr_zamowienia = mosys_info.get('nr_zamowienia')
                kod_detalu = mosys_info.get('kod_detalu')

                # Create DaneRaportu record
                raport = DaneRaportu(
                    nr_raportu=record['nr_raportu'],
                    operator_id=OPERATOR_ID,
                    nr_niezgodnosci=nr_niezg,
                    data_niezgodnosci=data_niezgodnosci,  # From MOSYS
                    nr_zamowienia=nr_zamowienia,  # From MOSYS
                    kod_detalu=kod_detalu,  # From MOSYS
                    nr_instrukcji=NR_INSTRUKCJI,  # Fixed value
                    selekcja_na_biezaco=record['selekcja_na_biezaco'],
                    ilosc_detali_sprawdzonych=record['ilosc_detali_sprawdzonych'],
                    zalecana_wydajnosc=record['zalecana_wydajnosc'],
                    czas_pracy=record['czas_pracy'],
                    uwagi=record['uwagi'],
                    uwagi_do_wydajnosci=record['uwagi_do_wydajnosci'],
                    data_selekcji=record['data_selekcji']
                )

                db.session.add(raport)
                db.session.flush()  # Get the ID for the raport

                # Parse and add defects from Uwagi column
                defects = parse_defects_from_uwagi(record['uwagi'])
                for defect_name, count in defects:
                    defekt = BrakiDefektyRaportu(
                        raport_id=raport.id,
                        defekt=defect_name,
                        ilosc=count
                    )
                    db.session.add(defekt)

                # If no specific defects parsed, use total count from ilosc_wadliwych
                ilosc_wadliwych = record['ilosc_wadliwych']
                if ilosc_wadliwych and ilosc_wadliwych > 0 and not defects:
                    defekt = BrakiDefektyRaportu(
                        raport_id=raport.id,
                        defekt=record['uwagi'] if record['uwagi'] else "Niespecyfikowane",
                        ilosc=ilosc_wadliwych
                    )
                    db.session.add(defekt)

                imported_count += 1

            except Exception as e:
                error_count += 1
                current_app.logger.error(f'Error importing row {record.get("row_num", "?")}: {e}')
                db.session.rollback()
                continue

        # Commit all new records
        if imported_count > 0:
            db.session.commit()
            current_app.logger.info(f'Successfully imported {imported_count} new records')

        return {
            'checked': True,
            'new_records': imported_count,
            'errors': error_count,
            'message': f'Imported {imported_count} new records' if imported_count > 0 else 'No new records imported'
        }

    except Exception as e:
        current_app.logger.error(f'Excel sync error: {e}')
        return {
            'checked': True,
            'new_records': 0,
            'errors': 1,
            'message': f'Sync failed: {str(e)}'
        }


def set_sync_interval(seconds):
    """
    Set the minimum interval between automatic syncs.

    Args:
        seconds: Minimum seconds between sync checks (default: 300 = 5 minutes)
    """
    global _sync_interval
    _sync_interval = max(60, seconds)  # Minimum 1 minute


def force_sync():
    """
    Force an immediate sync, bypassing the time cache.

    Returns:
        dict: Sync operation statistics
    """
    return sync_new_excel_data(force=True)
