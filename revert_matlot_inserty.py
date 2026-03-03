"""One-time rollback: restore MOSYS LOTTO_VERIFICATO = 'S' for old 'I%' batches.

Background
----------
The drift-correction pass (TODO 2 in _sync_from_mosys) reset LOTTO_VERIFICATO
from 'S' → 'N' for 'I%' rows where MOSYS had 'S' but local matlot_tracking had
release_status = 'N'.  New 'I%' rows (first seen today) already had 'N' and are
unchanged.

Rollback logic
--------------
  OLD row  (prima_vista < today, in matlot_tracking): was 'S' in MOSYS → restore 'S'
  NEW row  (prima_vista = today, or not yet in tracking): was 'N' → skip

Usage
-----
    python revert_matlot_inserty.py           # live run
    python revert_matlot_inserty.py --dry-run  # preview without writing to MOSYS
"""
import argparse
import sqlite3
import sys
import os
from datetime import date

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

DB_PATH = os.path.join(PROJECT_ROOT, 'linea.db')
TODAY_STR = date.today().isoformat()   # e.g. '2026-03-02'


def read_old_inserty_rows() -> list[tuple[str, str]]:
    """Return (codice_materiale, lotto) for 'I%' rows tracked BEFORE today."""
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT codice_materiale, lotto "
            "FROM matlot_tracking "
            "WHERE codice_materiale LIKE 'I%' "
            "  AND release_status = 'N' "
            "  AND prima_vista < ?",
            (TODAY_STR,),
        )
        return cur.fetchall()
    finally:
        conn.close()


def read_new_inserty_rows() -> list[tuple[str, str]]:
    """Return (codice_materiale, lotto) for 'I%' rows first seen TODAY (new)."""
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT codice_materiale, lotto "
            "FROM matlot_tracking "
            "WHERE codice_materiale LIKE 'I%' "
            "  AND prima_vista = ?",
            (TODAY_STR,),
        )
        return cur.fetchall()
    finally:
        conn.close()


def main(dry_run: bool) -> None:
    try:
        old_rows = read_old_inserty_rows()
        new_rows = read_new_inserty_rows()
    except Exception as e:
        print(f"[ERROR] Cannot read linea.db: {e}")
        sys.exit(1)

    print(f"Old 'I%' rows (prima_vista < today, release_status='N'): {len(old_rows)}")
    print(f"  → will restore LOTTO_VERIFICATO = 'S'  (these were changed by drift correction)")
    print(f"New 'I%' rows (prima_vista = today):                       {len(new_rows)}")
    print(f"  → skipped  (MOSYS already had 'N' before the app ran)")

    if not old_rows:
        print("\nNothing to rollback.")
        return

    if dry_run:
        print("\n[DRY-RUN] No MOSYS writes will be performed.")
        print("Would restore the following rows to LOTTO_VERIFICATO = 'S':")
        for codice, lotto in old_rows:
            print(f"  {codice} / {lotto}")
        return

    from MOSYS_data_functions import update_matlot_lotto_status

    ok_count = 0
    fail_count = 0
    print()

    for codice, lotto in old_rows:
        ok = update_matlot_lotto_status(codice, lotto, 'S')
        status = '[OK]' if ok else '[FAILED]'
        print(f"  {codice} / {lotto}  →  'S'  {status}")
        if ok:
            ok_count += 1
        else:
            fail_count += 1

    print(f"\nDone: {ok_count} restored to 'S', {fail_count} failed.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Rollback MOSYS LOTTO_VERIFICATO for old 'I%' batches to 'S'."
    )
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview without writing to MOSYS')
    args = parser.parse_args()
    main(dry_run=args.dry_run)
