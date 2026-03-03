"""cleanup_matlot.py — three-step MATLOT data cleanup script.

Step 1 — MOSYS UPDATE:
    Set LOTTO_VERIFICATO = 'S' for every MATLOT row where:
      - CODICE_MATERIALE does NOT start with 't' or 'i' (case-insensitive)
      - LOTTO_VERIFICATO = 'N'  (only update rows that need changing)

Step 2 — SQLite DELETE:
    Remove matlot_tracking rows matching the same codice criteria
    (does not start with 't' or 'i') AND release_status = 'N'.
    These batches don't belong to surowce/inserty categories and
    should not be tracked in LINEA.

Step 3 — SQLite UPDATE:
    For any matlot_tracking row where release_status = 'S' but
    released_at is NULL, set released_at = prima_vista (at noon).

Usage:
    python cleanup_matlot.py            # apply all three steps
    python cleanup_matlot.py --dry-run  # preview without writing
"""
import argparse
import sys
from datetime import datetime, time


def main():
    parser = argparse.ArgumentParser(description='Three-step MATLOT cleanup.')
    parser.add_argument('--dry-run', action='store_true',
                        help='Print planned changes without writing to any database.')
    args = parser.parse_args()

    dry = args.dry_run
    if dry:
        print("DRY RUN — no changes will be written.\n")

    # ── Step 1: MOSYS UPDATE ──────────────────────────────────────────────────
    print("=" * 60)
    print("STEP 1 — MOSYS: set LOTTO_VERIFICATO = 'S' for non-t/i rows")
    print("=" * 60)

    mosys_updated = 0
    try:
        from MOSYS_data_functions import pervasive_connection, get_pervasive
        query_count = """
            SELECT COUNT(*) AS cnt
            FROM STAAMPDB.MATLOT
            WHERE LOTTO_VERIFICATO = 'N'
              AND CODICE_MATERIALE NOT LIKE 't%'
              AND CODICE_MATERIALE NOT LIKE 'T%'
              AND CODICE_MATERIALE NOT LIKE 'i%'
              AND CODICE_MATERIALE NOT LIKE 'I%'
        """
        query_update = """
            UPDATE STAAMPDB.MATLOT
            SET LOTTO_VERIFICATO = 'S'
            WHERE LOTTO_VERIFICATO = 'N'
              AND CODICE_MATERIALE NOT LIKE 't%'
              AND CODICE_MATERIALE NOT LIKE 'T%'
              AND CODICE_MATERIALE NOT LIKE 'i%'
              AND CODICE_MATERIALE NOT LIKE 'I%'
        """
        # Count matching rows first (works for both dry-run and live)
        count_df = get_pervasive(query_count)
        mosys_updated = int(count_df.iloc[0, 0]) if not count_df.empty else 0

        if dry:
            print(f"  Would update {mosys_updated} MOSYS rows → LOTTO_VERIFICATO = 'S'")
        else:
            with pervasive_connection(readonly=False) as conn:
                cursor = conn.cursor()
                cursor.execute(query_update)
                conn.commit()
            print(f"  Updated {mosys_updated} MOSYS rows → LOTTO_VERIFICATO = 'S'")
    except Exception as e:
        print(f"  ERROR in MOSYS step: {e}", file=sys.stderr)
        print("  Continuing with SQLite steps...\n")

    # ── Steps 2 & 3: SQLite via Flask / SQLAlchemy ────────────────────────────
    from app import create_app, db
    from app.models.matlot import MatlotTracking

    app = create_app('development')

    with app.app_context():

        # ── Step 2: DELETE non-t/i pending rows from SQLite ───────────────────
        print()
        print("=" * 60)
        print("STEP 2 — SQLite: DELETE non-t/i rows with release_status = 'N'")
        print("=" * 60)

        to_delete = MatlotTracking.query.filter(
            MatlotTracking.release_status == 'N',
            ~MatlotTracking.codice_materiale.ilike('t%'),
            ~MatlotTracking.codice_materiale.ilike('i%'),
        ).all()

        if not to_delete:
            print("  Nothing to delete.")
        else:
            for r in to_delete:
                print(f"  {'WOULD DELETE' if dry else 'DELETE'} "
                      f"{r.codice_materiale}/{r.lotto}@{r.box or '-'}"
                      f"  status={r.release_status}")
                if not dry:
                    db.session.delete(r)

        if not dry and to_delete:
            db.session.commit()
            print(f"  Deleted {len(to_delete)} rows.")

        # ── Step 3: UPDATE released_at = prima_vista where NULL ───────────────
        print()
        print("=" * 60)
        print("STEP 3 — SQLite: fill released_at = prima_vista for S rows with NULL")
        print("=" * 60)

        missing_rel = MatlotTracking.query.filter(
            MatlotTracking.release_status == 'S',
            MatlotTracking.released_at.is_(None),
        ).all()

        if not missing_rel:
            print("  Nothing to update.")
        else:
            for r in missing_rel:
                # released_at is DateTime — store as noon to avoid midnight edge cases
                new_dt = datetime.combine(r.prima_vista, time(12, 0, 0))
                print(f"  {'WOULD SET' if dry else 'SET'} "
                      f"{r.codice_materiale}/{r.lotto}@{r.box or '-'}"
                      f"  released_at ← {new_dt.strftime('%d.%m.%Y %H:%M')}")
                if not dry:
                    r.released_at = new_dt

        if not dry and missing_rel:
            db.session.commit()
            print(f"  Updated {len(missing_rel)} rows.")

    # ── Summary ───────────────────────────────────────────────────────────────
    print()
    print("=" * 60)
    print("SUMMARY" + ("  (DRY RUN)" if dry else ""))
    print("=" * 60)
    print(f"  MOSYS LOTTO_VERIFICATO → 'S' : {mosys_updated}")
    print(f"  SQLite rows deleted          : {len(to_delete)}")
    print(f"  SQLite released_at filled    : {len(missing_rel)}")
    if dry:
        print("\nNo changes were written.")


if __name__ == '__main__':
    main()
