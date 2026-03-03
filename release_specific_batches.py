"""One-off script: set release_status = 'S' in SQLite AND LOTTO_VERIFICATO = 'S'
in MOSYS for the specific list of batches below.

Both writes use exact (CODICE_MATERIALE, LOTTO) keys.
SQLite rows not found in matlot_tracking are skipped with a warning
(they may not have been synced yet — run a MOSYS refresh first).

Usage:
    python release_specific_batches.py           # dry-run (preview, no changes)
    python release_specific_batches.py --commit  # apply to both SQLite and MOSYS
"""
import sys
from datetime import datetime

# ── target list ───────────────────────────────────────────────────────────────
# Parsed from provided table. Duplicates removed.
BATCHES: list[tuple[str, str]] = sorted(set([
    ('I018359006',   '2025-12-11/2503160'),
    ('I030405A',     '0AV0251119/0013362086'),
    ('I030405A',     '2023-10-16/0013362086'),
    ('I033357005',   '0AV0211221/1803295'),
    ('I033357005',   '2018-09-07/1803295'),
    ('I033357005',   '2018-09-15/1803295'),
    ('I033357005',   '2018-09-27/1803295'),
    ('I033357005',   '2018-10-09/1803295'),
    ('I033357005',   '2022-12-27/1803295'),
    ('I033358005',   '0AV0211221/1803296'),
    ('I033358005',   '2018-10-09/1803296'),
    ('I0924443A',    '0AV0251119/0013406363'),
    ('I0924443A',    '2023-10-16/0013406363'),
    ('I0924443A',    '2023-10-16/0013410797'),
    ('I12710065720', '2024-10-04/20241978'),   # duplicate removed
    ('I21A015G044',  '2024-05-23/240983'),
    ('I227209',      '0AV0221017/17-2473'),
    ('I227209',      '0AV1221017/17-2473'),
    ('I227209',      '2018-02-05/17-2473'),
    ('I34149958',    '2025-08-27/2503576'),
    ('I34149967',    '2024-06-19/2402595'),
    ('I34149967',    '2024-06-19/2403385'),
    ('I34149967',    '2024-08-13/24-03385'),
    ('I34149968',    '2024-07-10/2403248'),
    ('I34149968',    '2024-08-13/24-03248'),
    ('I34253746',    '2025-11-24/2505199'),
    ('I34253748',    '2025-10-31/2504646'),
    ('I34276657',    '2025-11-27/2505201'),
    ('I348276',      '0AV0251119/1'),
    ('I348276',      '2024-05-23/1'),
    ('I348276',      '2024-05-23/2'),
    ('I348276',      '2024-05-23/8'),
    ('I348276-1',    '2024-11-21/1'),
    ('I348276-1',    '2024-11-21/11'),
    ('I348276-1',    '2024-11-21/2'),  # duplicate removed
    ('I348276-1',    '2024-11-21/3'),
    ('I348277',      '0AV0251119/3'),
    ('I348277',      '2024-01-18/3'),
    ('I348277',      '2024-05-23/2'),
    ('I348277',      '2024-05-23/3'),
    ('I348277-1',    '2024-11-21/1'),  # duplicate removed
    ('I348277-1',    '2024-11-21/2'),
    ('I348277-1',    '2024-11-21/9'),
    ('I7X1,4XX8,7.B', '2023-12-14/231323'),
    ('I7X1,4XX8,7.B', '2023-12-14/231324'),
    ('IGBU00000265', '2024-07-23/23072024'),
    ('IHPR22877',    '0AV0240113/19678'),
    ('IHPR22877',    '0AV0240113/19679'),
    ('IHPR22877',    '0AV0251230/20400'),
    ('IV5400617',    '2014-01-30/ 131690'),
    ('IV5400617',    '2014-01-30/131476'),
]))

MOSYS_UPDATE_QUERY = """
    UPDATE STAAMPDB.MATLOT
    SET LOTTO_VERIFICATO = 'S'
    WHERE CODICE_MATERIALE = ? AND LOTTO = ?
"""


def update_mosys(rows: list[tuple[str, str]]) -> tuple[int, int]:
    from MOSYS_data_functions import pervasive_connection
    ok = fail = 0
    with pervasive_connection(readonly=False) as conn:
        cursor = conn.cursor()
        for codice, lotto in rows:
            try:
                cursor.execute(MOSYS_UPDATE_QUERY, (codice, lotto))
                ok += 1
            except Exception as e:
                print(f"  MOSYS ERROR {codice}/{lotto}: {e}")
                fail += 1
        conn.commit()
    return ok, fail


def update_sqlite(rows: list[tuple[str, str]]) -> tuple[int, int, int]:
    from app import create_app, db
    from app.models.matlot import MatlotTracking

    app = create_app('development')
    ok = fail = skipped = 0
    now = datetime.now()

    with app.app_context():
        for codice, lotto in rows:
            tracking = MatlotTracking.query.filter_by(
                codice_materiale=codice, lotto=lotto
            ).first()
            if tracking is None:
                print(f"  SQLite SKIP (not found): {codice}/{lotto}")
                skipped += 1
                continue
            if tracking.release_status == 'S':
                print(f"  SQLite SKIP (already S): {codice}/{lotto}")
                skipped += 1
                continue
            try:
                tracking.release_status = 'S'
                tracking.released_at    = now
                ok += 1
            except Exception as e:
                print(f"  SQLite ERROR {codice}/{lotto}: {e}")
                fail += 1
        try:
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            print(f"  SQLite commit error: {e}")
            fail += ok
            ok = 0

    return ok, fail, skipped


def main():
    commit = '--commit' in sys.argv

    print(f"Batches to release: {len(BATCHES)}")
    print(f"\n{'CODICE_MATERIALE':<22} {'LOTTO'}")
    print('-' * 60)
    for codice, lotto in BATCHES:
        print(f"  {codice:<22} {lotto}")

    if not commit:
        print(f"\n[DRY-RUN] No changes written. Re-run with --commit to apply.")
        return

    print(f"\n── Updating MOSYS ──────────────────────────────────────────")
    mosys_ok, mosys_fail = update_mosys(BATCHES)
    print(f"MOSYS: {mosys_ok} updated, {mosys_fail} failed")

    print(f"\n── Updating SQLite ─────────────────────────────────────────")
    sqlite_ok, sqlite_fail, sqlite_skip = update_sqlite(BATCHES)
    print(f"SQLite: {sqlite_ok} updated, {sqlite_fail} failed, {sqlite_skip} skipped")

    if mosys_fail or sqlite_fail:
        print("\nSome updates failed — check errors above.")
        sys.exit(1)
    print("\nDone.")


if __name__ == '__main__':
    main()
