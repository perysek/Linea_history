"""One-off script: set LOTTO_VERIFICATO = 'S' in MOSYS MATLOT and
release_status = 'S' in SQLite matlot_tracking for rows where:

    CODICE_MATERIALE starts with 'T' (case-insensitive)
    AND one of:
        (A) LOTTO starts with '202' but NOT '2026'  (years 2020-2025)
        (B) LOTTO starts with '201'                  (years 2010-2019)
        (C) LOTTO starts with '0BU'

Exact filter enforced in Python to avoid Pervasive SQL NOT quirks.

SQLite rows not found in matlot_tracking are skipped with a warning —
run a MOSYS refresh first if you expect them to be there.

Usage:
    python fix_matlot_surowce.py           # dry-run (preview, no changes)
    python fix_matlot_surowce.py --commit  # apply to MOSYS and SQLite
"""
import sys
from datetime import datetime
from MOSYS_data_functions import get_pervasive, pervasive_connection

FETCH_QUERY = """
    SELECT CODICE_MATERIALE, LOTTO, LOTTO_VERIFICATO
    FROM STAAMPDB.MATLOT
    WHERE (CODICE_MATERIALE LIKE 'T%' OR CODICE_MATERIALE LIKE 't%')
    ORDER BY CODICE_MATERIALE, LOTTO
"""

UPDATE_ROW_QUERY = """
    UPDATE STAAMPDB.MATLOT
    SET LOTTO_VERIFICATO = 'S'
    WHERE CODICE_MATERIALE = ? AND LOTTO = ?
"""


def _matches(codice: str, lotto: str) -> bool:
    """Return True if this row should be updated."""
    if not codice.upper().startswith('T'):
        return False
    return (
        (lotto.startswith('202') and not lotto.startswith('2026'))
        or lotto.startswith('201')
        or lotto.upper().startswith('0BU')
    )


def preview() -> list[tuple[str, str]]:
    print("Fetching T* rows from MOSYS…")
    df = get_pervasive(FETCH_QUERY)

    if df.empty:
        print("No rows returned.")
        return []

    to_update = []
    skipped   = []

    for _, row in df.iterrows():
        codice    = str(row['CODICE_MATERIALE']).strip()
        lotto     = str(row['LOTTO']).strip()
        lotto_ver = str(row['LOTTO_VERIFICATO']).strip()

        if _matches(codice, lotto):
            to_update.append((codice, lotto, lotto_ver))
        else:
            skipped.append((codice, lotto))

    print(f"\n{'CODICE_MATERIALE':<22} {'LOTTO':<30} {'LOTTO_VERIFICATO'}")
    print('-' * 72)
    for codice, lotto, lotto_ver in to_update:
        print(f"  {codice:<22} {lotto:<30} {lotto_ver}")

    print(f"\nWill update : {len(to_update)} rows")
    print(f"Skipped     : {len(skipped)} rows (2026 lots, unmatched lotto prefix, non-T material, or already S)")

    return [(c, l) for c, l, _ in to_update]


def apply_mosys(rows: list[tuple[str, str]]) -> tuple[int, int]:
    print(f"\n── Updating MOSYS ({len(rows)} rows) ─────────────────────────────────")
    ok = fail = 0
    with pervasive_connection(readonly=False) as conn:
        cursor = conn.cursor()
        for codice, lotto in rows:
            try:
                cursor.execute(UPDATE_ROW_QUERY, (codice, lotto))
                ok += 1
            except Exception as e:
                print(f"  ERROR {codice}/{lotto}: {e}")
                fail += 1
        conn.commit()
    print(f"MOSYS: {ok} updated, {fail} failed")
    return ok, fail


def apply_sqlite(rows: list[tuple[str, str]]) -> tuple[int, int, int]:
    from app import create_app, db
    from app.models.matlot import MatlotTracking

    print(f"\n── Updating SQLite ({len(rows)} rows) ────────────────────────────────")
    app = create_app('development')
    ok = fail = skipped = 0
    now = datetime.now()

    with app.app_context():
        for codice, lotto in rows:
            tracking = MatlotTracking.query.filter_by(
                codice_materiale=codice, lotto=lotto
            ).first()
            if tracking is None:
                print(f"  SKIP (not in matlot_tracking): {codice}/{lotto}")
                skipped += 1
                continue
            if tracking.release_status == 'S':
                print(f"  SKIP (already S): {codice}/{lotto}")
                skipped += 1
                continue
            tracking.release_status = 'S'
            tracking.released_at    = now
            ok += 1

        try:
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            print(f"  SQLite commit error: {e}")
            fail += ok
            ok = 0

    print(f"SQLite: {ok} updated, {fail} failed, {skipped} skipped")
    return ok, fail, skipped


def main():
    commit = '--commit' in sys.argv
    rows = preview()

    if not rows:
        print("Nothing to update.")
        return

    if not commit:
        print("\n[DRY-RUN] No changes written. Re-run with --commit to apply.")
        return

    _, mosys_fail   = apply_mosys(rows)
    _, sqlite_fail, _ = apply_sqlite(rows)

    if mosys_fail or sqlite_fail:
        print("\nSome updates failed — check errors above.")
        sys.exit(1)
    print("\nDone.")


if __name__ == '__main__':
    main()
