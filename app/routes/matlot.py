"""MATLOT incoming raw material inspection routes.

Architecture:
  - matlot_tracking (SQLite) is the source of truth for release_status.
  - MOSYS is fetched on explicit refresh only; batch metadata (giacenza, box)
    is cached in matlot_tracking so the data endpoint reads SQLite exclusively.
  - On release: SQLite updated first (primary), MOSYS written second (best-effort).
"""
from flask import Blueprint, render_template, jsonify, request, current_app
from datetime import date, datetime

from app import db
from app.models.matlot import MatlotTracking

matlot_bp = Blueprint('matlot', __name__)

# ── helpers ───────────────────────────────────────────────────────────────────

VALID_SORT_FIELDS = {
    'CODICE_MATERIALE': 'codice_materiale',
    'LOTTO':            'lotto',
    'GIACENZA_LOTTO':   'giacenza_lotto',
    'BOX':              'box',
    'GIORNI':           'giorni',
    'PRIMA_VISTA':      'prima_vista',
    'RELEASED_AT':      'released_at',
}


def _sync_from_mosys():
    """Fetch all MATLOT batches from MOSYS and upsert into matlot_tracking.

    New rows (not yet in SQLite) are seeded with the actual LOTTO_VERIFICATO
    value from MOSYS, so pre-existing released batches are imported as 'S'
    and pre-existing pending batches as 'N'. This treats all current MOSYS
    rows as "already known" on first sync and avoids false new-batch alerts.

    Existing rows: only giacenza_lotto and box are refreshed.
    release_status and released_at are never overwritten.

    MOSYS is read-only — this function never writes back to MOSYS.

    Cleanup rule: tracking rows that are gone from MOSYS AND already released
    are deleted (fully processed). Pending rows absent from MOSYS are
    preserved to handle sync lag.

    Returns:
        tuple(int synced_count, str|None error_message)
    """
    try:
        from MOSYS_data_functions import get_matlot_batches
        df = get_matlot_batches()
    except Exception as e:
        msg = f"MOSYS fetch failed: {e}"
        current_app.logger.error(f"MATLOT _sync_from_mosys: {msg}")
        return 0, msg

    today = date.today()
    mosys_keys = set()

    if df is not None and not df.empty:
        for _, row in df.iterrows():
            codice = str(row.get('CODICE_MATERIALE') or '').strip()
            lotto  = str(row.get('LOTTO') or '').strip()
            if not codice or not lotto:
                continue

            mosys_keys.add((codice, lotto))

            giacenza = row.get('GIACENZA_LOTTO')
            try:
                giacenza = int(giacenza) if giacenza is not None else 0
            except (ValueError, TypeError):
                giacenza = 0

            box_parts = [
                str(row.get('BOX_X') or '').strip(),
                str(row.get('BOX_Y') or '').strip(),
                str(row.get('BOX_Z') or '').strip(),
            ]
            box = '-'.join(p for p in box_parts if p) or '-'

            tracking = MatlotTracking.query.filter_by(
                codice_materiale=codice, lotto=lotto
            ).first()

            if tracking is None:
                # Seed with MOSYS's actual status so pre-existing released
                # batches don't appear as new pending items.
                mosys_status = str(row.get('LOTTO_VERIFICATO') or '').strip()
                release_status = mosys_status if mosys_status in ('N', 'S') else 'N'

                tracking = MatlotTracking(
                    codice_materiale=codice,
                    lotto=lotto,
                    prima_vista=today,
                    release_status=release_status,
                    giacenza_lotto=giacenza,
                    box=box,
                )
                db.session.add(tracking)
                current_app.logger.info(
                    f"MATLOT new batch: {codice}/{lotto} → release_status='{release_status}'"
                )
            else:
                # Refresh cached metadata only; never touch release_status/released_at
                tracking.giacenza_lotto = giacenza
                tracking.box = box

    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        msg = f"SQLite commit error: {e}"
        current_app.logger.error(f"MATLOT _sync_from_mosys: {msg}")
        return 0, msg

    # Remove fully-processed rows that are no longer in MOSYS
    _cleanup_tracking(mosys_keys)

    return len(mosys_keys), None


def _get_tracking_rows():
    """Read all matlot_tracking rows from SQLite. No MOSYS call.

    Returns a list of dicts with keys:
        codice_materiale, lotto, giacenza_lotto, box, prima_vista,
        giorni, is_past_due, release_status, released_at
    """
    today = date.today()
    rows = MatlotTracking.query.all()
    result = []
    for t in rows:
        prima_vista = t.prima_vista
        giorni = (today - prima_vista).days
        released_at_str = (
            t.released_at.strftime('%d.%m.%Y %H:%M')
            if t.released_at else ''
        )
        result.append({
            'codice_materiale': t.codice_materiale,
            'lotto':            t.lotto,
            'giacenza_lotto':   t.giacenza_lotto or 0,
            'box':              t.box or '-',
            'prima_vista':      prima_vista.strftime('%d.%m.%Y'),
            'giorni':           giorni,
            'is_past_due':      giorni > 2,
            'release_status':   t.release_status,
            'released_at':      released_at_str,
        })
    return result


def _cleanup_tracking(active_mosys_keys: set):
    """Delete tracking rows for batches gone from MOSYS AND already released."""
    try:
        stale = MatlotTracking.query.all()
        for t in stale:
            if (t.codice_materiale, t.lotto) not in active_mosys_keys:
                if t.release_status == 'S':
                    db.session.delete(t)
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"MATLOT cleanup error: {e}")


# ── page route ────────────────────────────────────────────────────────────────

@matlot_bp.route('/matlot-status')
def matlot_status():
    """Raw material incoming inspection — MATLOT certificate monitoring view."""
    return render_template('matlot/matlot_status.html')


# ── API: refresh (MOSYS sync) ─────────────────────────────────────────────────

@matlot_bp.route('/api/matlot-refresh', methods=['POST'])
def api_matlot_refresh():
    """Sync MOSYS → matlot_tracking. Called on page load and Refresh button."""
    synced, error = _sync_from_mosys()
    if error:
        return jsonify({
            'success':   False,
            'error':     error,
            'synced':    synced,
            'timestamp': datetime.now().strftime('%H:%M:%S'),
        }), 500
    return jsonify({
        'success':   True,
        'synced':    synced,
        'timestamp': datetime.now().strftime('%H:%M:%S'),
    })


# ── API: list ─────────────────────────────────────────────────────────────────

@matlot_bp.route('/api/matlot-status')
def api_matlot_status():
    """Return matlot_tracking rows (SQLite only — no MOSYS call).

    Expects a prior call to /api/matlot-refresh to have synced fresh MOSYS data.
    """
    sort_field = request.args.get('sort', 'CODICE_MATERIALE')
    sort_dir   = request.args.get('dir', 'asc')
    limit      = request.args.get('limit', 100, type=int)
    offset     = request.args.get('offset', 0, type=int)

    category = request.args.get('category', '').strip().lower()  # 'surowce', 'inserty', or ''
    status   = request.args.get('status', 'N').strip().upper()   # 'N', 'S', or 'ALL'

    search = {
        'CODICE_MATERIALE': request.args.get('search_CODICE_MATERIALE', '').lower(),
        'LOTTO':            request.args.get('search_LOTTO', '').lower(),
        'BOX':              request.args.get('search_BOX', '').lower(),
    }

    try:
        rows = _get_tracking_rows()

        # Status filter (ALL / N-pending / S-released)
        if status in ('N', 'S'):
            rows = [r for r in rows if r['release_status'] == status]

        # Category toggle filter
        if category == 'surowce':
            rows = [r for r in rows if r['codice_materiale'].lower().startswith('t')]
        elif category == 'inserty':
            rows = [r for r in rows if r['codice_materiale'].lower().startswith('i')]

        # Column search filter
        for col, val in search.items():
            if val:
                key = VALID_SORT_FIELDS.get(col, col.lower())
                rows = [r for r in rows if val in str(r.get(key) or '').lower()]

        total_count    = len(rows)
        past_due_count = sum(1 for r in rows if r['is_past_due'])

        # Sort
        sort_key = VALID_SORT_FIELDS.get(sort_field, 'codice_materiale')
        reverse  = sort_dir == 'desc'
        numeric_keys = {'giacenza_lotto', 'giorni'}

        if sort_key in numeric_keys:
            rows.sort(key=lambda r: r.get(sort_key) or 0, reverse=reverse)
        else:
            rows.sort(key=lambda r: str(r.get(sort_key) or '').lower(), reverse=reverse)

        # Paginate
        page = rows[offset: offset + limit]

        return jsonify({
            'success':        True,
            'rows':           page,
            'past_due_count': past_due_count,
            'pagination': {
                'total':    total_count,
                'limit':    limit,
                'offset':   offset,
                'loaded':   len(page),
                'has_more': offset + limit < total_count,
            },
        })
    except Exception as e:
        current_app.logger.error(f"api_matlot_status error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


# ── API: release ──────────────────────────────────────────────────────────────

@matlot_bp.route('/api/matlot-status/release', methods=['POST'])
def api_matlot_release():
    """Release a batch: update SQLite (primary) then MOSYS (best-effort parallel).

    SQLite release_status N → S is always committed first. MOSYS
    LOTTO_VERIFICATO write is attempted afterwards; its failure is logged but
    does not roll back the SQLite change or fail the request.
    """
    data   = request.get_json(silent=True) or {}
    codice = str(data.get('codice_materiale') or '').strip()
    lotto  = str(data.get('lotto') or '').strip()
    uwagi  = str(data.get('uwagi') or '').strip()

    if not codice or not lotto:
        return jsonify({'success': False, 'error': 'codice_materiale and lotto required'}), 400

    try:
        tracking = MatlotTracking.query.filter_by(
            codice_materiale=codice, lotto=lotto
        ).first()

        if not tracking:
            return jsonify({'success': False, 'error': 'Batch not found in tracking'}), 404

        if tracking.release_status == 'S':
            return jsonify({'success': False, 'error': 'Batch already released'}), 409

        # Primary write: SQLite
        tracking.release_status = 'S'
        tracking.released_at    = datetime.now()
        if uwagi:
            tracking.uwagi = uwagi
        db.session.commit()

    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"api_matlot_release SQLite error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

    # Secondary write: MOSYS (best-effort — does not affect the response)
    try:
        from MOSYS_data_functions import update_matlot_lotto_status
        ok = update_matlot_lotto_status(codice, lotto, 'S')
        if not ok:
            current_app.logger.warning(
                f"MOSYS parallel write failed for {codice}/{lotto} — SQLite already committed"
            )
    except Exception as mosys_err:
        current_app.logger.warning(
            f"MOSYS parallel write exception for {codice}/{lotto}: {mosys_err}"
        )

    return jsonify({'success': True})
