"""One-off script: mark PWZ material batches as verified in MOSYS.

Sets LOTTO_VERIFICATO = 'S' for all MATLOT rows where:
    - CODICE_MATERIALE starts with 'PWZ' (case-insensitive)
    - LOTTO_VERIFICATO is currently 'N' (unverified only — avoids unnecessary writes)

Run with --dry-run to preview affected row count without committing.

Usage:
    python update_matlot_pwz.py            # execute update
    python update_matlot_pwz.py --dry-run  # preview only
"""
import sys
from MOSYS_data_functions import pervasive_connection, get_pervasive

PREVIEW_QUERY = """
    SELECT COUNT(*) AS cnt
    FROM STAAMPDB.MATLOT
    WHERE LOTTO_VERIFICATO = 'N'
      AND LOWER(CODICE_MATERIALE) LIKE 'pwz%'
"""

UPDATE_QUERY = """
    UPDATE STAAMPDB.MATLOT
    SET LOTTO_VERIFICATO = 'S'
    WHERE LOTTO_VERIFICATO = 'N'
      AND LOWER(CODICE_MATERIALE) LIKE 'pwz%'
"""


def preview_affected_rows() -> int:
    """Return count of rows that would be updated."""
    df = get_pervasive(PREVIEW_QUERY)
    return int(df.iloc[0]['cnt']) if not df.empty else 0


def run_update() -> int:
    """Execute the UPDATE and return number of rows changed."""
    with pervasive_connection(readonly=False) as conn:
        cursor = conn.cursor()
        cursor.execute(UPDATE_QUERY)
        count = cursor.rowcount
        conn.commit()
    return count


def main():
    dry_run = '--dry-run' in sys.argv

    print("MATLOT PWZ batch verification update")
    print("=" * 40)

    affected = preview_affected_rows()
    print(f"Rows matching criteria (LOTTO_VERIFICATO='N', CODICE_MATERIALE LIKE 'PWZ%'): {affected}")

    if affected == 0:
        print("Nothing to update — exiting.")
        return

    if dry_run:
        print("[DRY RUN] No changes committed.")
        return

    confirm = input(f"\nProceed with updating {affected} rows? [y/N] ").strip().lower()
    if confirm != 'y':
        print("Aborted.")
        return

    updated = run_update()
    print(f"Done. {updated} rows updated (LOTTO_VERIFICATO set to 'S').")


if __name__ == '__main__':
    main()
