"""update_matlot_dates.py — backfill prima_vista and released_at from lotto text.

For each row in matlot_tracking:
  - prima_vista  ← date parsed from the first 10 characters of `lotto` (YYYY-MM-DD).
  - released_at  ← prima_vista + random 1 or 2 days (only when release_status == 'S').

Rows where the lotto field does not start with a valid YYYY-MM-DD are skipped.
released_at is never changed for rows that are not in 'S' status.

Usage:
    python update_matlot_dates.py           # apply updates
    python update_matlot_dates.py --dry-run # preview without writing to DB
"""
import argparse
import random
import re
import sys
from datetime import date, datetime, time, timedelta

RE_DATE = re.compile(r'^\d{4}-\d{2}-\d{2}')


def parse_lotto_date(lotto: str) -> date | None:
    """Return a date object from the first 10 chars of lotto, or None if invalid."""
    raw = (lotto or '').strip()[:10]
    if not RE_DATE.match(raw):
        return None
    try:
        y, m, d = raw.split('-')
        parsed = date(int(y), int(m), int(d))
    except ValueError:
        return None
    return parsed


def main():
    parser = argparse.ArgumentParser(description='Backfill matlot_tracking dates from lotto text.')
    parser.add_argument('--dry-run', action='store_true',
                        help='Print planned changes without writing to the database.')
    args = parser.parse_args()

    # Bootstrap Flask so SQLAlchemy models are available
    from app import create_app, db
    from app.models.matlot import MatlotTracking

    app = create_app('development')

    updated_prima  = 0
    updated_rel    = 0
    skipped        = 0

    with app.app_context():
        rows: list[MatlotTracking] = MatlotTracking.query.all()
        print(f"Loaded {len(rows)} rows from matlot_tracking.\n")

        for t in rows:
            new_prima = parse_lotto_date(t.lotto)

            if new_prima is None:
                print(f"  SKIP  {t.codice_materiale}/{t.lotto}  — no YYYY-MM-DD prefix")
                skipped += 1
                continue

            prima_changed = new_prima != t.prima_vista
            if prima_changed:
                print(f"  prima_vista  {t.codice_materiale}/{t.lotto}: "
                      f"{t.prima_vista} → {new_prima}")
                if not args.dry_run:
                    t.prima_vista = new_prima
                updated_prima += 1

            if t.release_status == 'S':
                days = random.randint(1, 2)
                new_rel_date = new_prima + timedelta(days=days)
                # released_at is DateTime — store as noon to avoid midnight/UTC edge cases
                new_rel_dt = datetime.combine(new_rel_date, time(12, 0, 0))
                old_rel = t.released_at.date() if t.released_at else None
                rel_changed = old_rel != new_rel_date
                if rel_changed or prima_changed:
                    print(f"  released_at  {t.codice_materiale}/{t.lotto}: "
                          f"{t.released_at} → {new_rel_dt}  (+{days}d)")
                    if not args.dry_run:
                        t.released_at = new_rel_dt
                    updated_rel += 1

        print()
        if args.dry_run:
            print("DRY RUN — no changes written.")
        else:
            db.session.commit()
            print("Changes committed.")

        print(f"\nSummary:")
        print(f"  prima_vista updated : {updated_prima}")
        print(f"  released_at updated : {updated_rel}")
        print(f"  skipped (no date)   : {skipped}")


if __name__ == '__main__':
    main()
