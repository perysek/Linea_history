"""One-off script: set LOTTO_VERIFICATO = 'S' in MOSYS MATLOT table for rows where
    CODICE_MATERIALE starts with 'I'  AND
    LOTTO does not start with '2026'

Exclusions (never updated):
    CODICE_MATERIALE starts with 'IGBU' or 'I127'  AND  LOTTO starts with '2025'

The exclusion is enforced in Python (not SQL) because the Pervasive SQL engine
does not reliably handle compound NOT (... AND ...) expressions.

Usage:
    python fix_matlot_lott_verificato.py           # dry-run (preview only, no changes)
    python fix_matlot_lott_verificato.py --commit  # apply changes to MOSYS
"""
import sys
from MOSYS_data_functions import get_pervasive, pervasive_connection

# Broad fetch — base filter only; exclusion applied in Python below
FETCH_QUERY = """
    SELECT CODICE_MATERIALE, LOTTO, LOTTO_VERIFICATO
    FROM STAAMPDB.MATLOT
    WHERE CODICE_MATERIALE LIKE 'I%'
      AND LOTTO NOT LIKE '2026%'
    ORDER BY CODICE_MATERIALE, LOTTO
"""

UPDATE_ROW_QUERY = """
    UPDATE STAAMPDB.MATLOT
    SET LOTTO_VERIFICATO = 'S'
    WHERE CODICE_MATERIALE = ? AND LOTTO = ?
"""


def _should_skip(codice: str, lotto: str) -> bool:
    """Return True if this row must NOT be updated (exclusion rule)."""
    return (
        (codice.startswith('IGBU') or codice.startswith('I127'))
        and lotto.startswith('2025')
    )


def preview():
    """Fetch qualifying rows and filter in Python. Returns list of (codice, lotto) to update."""
    print("Fetching rows from MOSYS…")
    df = get_pervasive(FETCH_QUERY)

    if df.empty:
        print("No rows match the base criteria.")
        return []

    to_update = []
    skipped = []

    for _, row in df.iterrows():
        codice = str(row['CODICE_MATERIALE']).strip()
        lotto  = str(row['LOTTO']).strip()
        lotto_ver = str(row['LOTTO_VERIFICATO']).strip()

        if _should_skip(codice, lotto):
            skipped.append((codice, lotto, lotto_ver))
        else:
            to_update.append((codice, lotto, lotto_ver))

    print(f"\n{'CODICE_MATERIALE':<22} {'LOTTO':<30} {'LOTTO_VERIFICATO'}")
    print("-" * 72)
    for codice, lotto, lotto_ver in to_update:
        print(f"{codice:<22} {lotto:<30} {lotto_ver}")

    print(f"\nWill update : {len(to_update)} rows")

    if skipped:
        print(f"\nSkipped (IGBU/I127 + 2025 exclusion): {len(skipped)} rows")
        for codice, lotto, _ in skipped:
            print(f"  SKIP  {codice:<22} {lotto}")

    return [(c, l) for c, l, _ in to_update]


def apply_update(rows: list[tuple[str, str]]):
    """Update each qualifying row individually using its exact (codice, lotto) key."""
    print(f"\nApplying UPDATE to {len(rows)} rows in MOSYS…")
    ok_count = 0
    fail_count = 0

    try:
        with pervasive_connection(readonly=False) as conn:
            cursor = conn.cursor()
            for codice, lotto in rows:
                try:
                    cursor.execute(UPDATE_ROW_QUERY, (codice, lotto))
                    ok_count += 1
                except Exception as e:
                    print(f"  ERROR updating {codice}/{lotto}: {e}")
                    fail_count += 1
            conn.commit()
    except Exception as e:
        print(f"Connection/commit error: {e}")
        return False

    print(f"Done. Updated: {ok_count}  Failed: {fail_count}")
    return fail_count == 0


def main():
    commit = "--commit" in sys.argv

    rows = preview()

    if not rows:
        print("Nothing to update.")
        return

    if not commit:
        print("\n[DRY-RUN] No changes written. Re-run with --commit to apply.")
    else:
        ok = apply_update(rows)
        if not ok:
            print("Some rows failed — check errors above.")
            sys.exit(1)
        print("MOSYS MATLOT.LOTTO_VERIFICATO updated successfully.")


if __name__ == "__main__":
    main()
