"""MATLOT incoming raw material inspection routes.

Architecture:
  - matlot_tracking (SQLite) is the source of truth for release_status.
  - MOSYS is fetched on explicit refresh only; batch metadata (giacenza, box)
    is cached in matlot_tracking so the data endpoint reads SQLite exclusively.
  - Unique key: (codice_materiale, lotto, box) — TASK3: each MOSYS warehouse
    location for the same material+lot is a separate tracking row.
  - On release/withdraw: SQLite updated first (primary), MOSYS written second (best-effort).
"""
from flask import Blueprint, render_template, jsonify, request, current_app
from datetime import date, datetime
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

from app import db
from app.models.matlot import MatlotTracking

matlot_bp = Blueprint('matlot', __name__)

# In-memory caches populated during MOSYS sync; persist for process lifetime.
# _material_names: codice_materiale → display name (MATPRI or INSERTI)
# _insert_codes:   set of codice_materiale values found in INSERTI (= are inserts)
_material_names: dict = {}
_insert_codes:   set  = set()

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


def _is_auto_approved(codice: str, lotto: str) -> bool:
    """Return True for batches that are auto-approved and excluded from tracking.

    Rule: CODICE_MATERIALE starts with 't' (case-insensitive)
          AND LOTTO starts with '0AV' or '0BU'.

    These batches are bulk-released in MOSYS during sync and never added
    to the local matlot_tracking table.
    """
    return (
        codice.lower().startswith('t')
        and (lotto.startswith('0AV') or lotto.startswith('0BU'))
    )


def _sync_from_mosys():
    """Fetch all MATLOT batches from MOSYS and upsert into matlot_tracking.

    Unique key is (codice_materiale, lotto, box) — TASK3 fix: the same
    material+lot can appear in multiple MOSYS warehouse locations (different
    BOX_X/BOX_Y/BOX_Z); previously only the first row per codice+lotto was
    tracked because the query used .first() on a (codice, lotto) index.

    New rows: seeded with the actual LOTTO_VERIFICATO value from MOSYS so
    pre-existing released batches import as 'S' and pending ones as 'N'.
    Existing rows: only giacenza_lotto is refreshed; box is part of the key.
    release_status, released_at, uwagi, withdrawn_at never overwritten.

    Cleanup rule: tracking rows gone from MOSYS AND already released are deleted.
    Pending rows absent from MOSYS are preserved to handle sync lag.

    Auto-approve rule: batches matching _is_auto_approved() are set to
    LOTTO_VERIFICATO='S' in MOSYS and excluded from matlot_tracking entirely.
    Any existing SQLite tracking rows for them are deleted during sync.

    Returns:
        tuple(int synced_count, str|None error_message)
    """
    try:
        from MOSYS_data_functions import get_matlot_batches, auto_approve_matlot_batches
        df = get_matlot_batches()
    except Exception as e:
        msg = f"MOSYS fetch failed: {e}"
        current_app.logger.error(f"MATLOT _sync_from_mosys: {msg}")
        return 0, msg

    # Step 1: Bulk-approve matching batches in MOSYS (LOTTO_VERIFICATO N → S).
    # Done before the main loop so the df already reflects the updated status
    # if MOSYS returns fresh data.
    try:
        from MOSYS_data_functions import auto_approve_matlot_batches
        approved_count = auto_approve_matlot_batches()
        if approved_count:
            current_app.logger.info(
                f"MATLOT auto-approve: set LOTTO_VERIFICATO='S' for {approved_count} MOSYS rows"
            )
    except Exception as e:
        current_app.logger.warning(f"MATLOT auto-approve failed (non-fatal): {e}")

    today = date.today()
    mosys_keys = set()  # (codice, lotto, box) tuples seen in this sync

    if df is not None and not df.empty:
        # Build caches from this sync's JOIN data.
        # NOME_COMMERCIALE comes from MATPRI (surowce); INSERTI_DESCRIZIONE from
        # INSERTI (inserty). Whichever is non-empty wins for the name cache.
        # Any code matched by INSERTI is authoritative as an insert.
        global _material_names, _insert_codes
        _insert_codes = set()
        for _, row in df.iterrows():
            codice_key    = str(row.get('CODICE_MATERIALE')    or '').strip()
            nome          = str(row.get('NOME_COMMERCIALE')    or '').strip()
            inserti_desc  = str(row.get('INSERTI_DESCRIZIONE') or '').strip()
            if not codice_key:
                continue
            if inserti_desc:
                _insert_codes.add(codice_key)
            display_name = nome or inserti_desc
            if display_name:
                _material_names[codice_key] = display_name

        for _, row in df.iterrows():
            codice = str(row.get('CODICE_MATERIALE') or '').strip()
            lotto  = str(row.get('LOTTO') or '').strip()
            if not codice or not lotto:
                continue

            # Skip codes that are neither a known insert (in INSERTI) nor a
            # surowiec (t-prefix). These are unrelated MATLOT rows not tracked
            # by this application.
            if codice not in _insert_codes and not codice.lower().startswith('t'):
                continue

            # Skip insert batches whose LOTTO starts with '0AV' — these are
            # bulk-handled outside LINEA and should not appear in the tracking table.
            if codice in _insert_codes and lotto.startswith('0AV'):
                continue

            # Step 2: Skip auto-approved batches — delete any stale SQLite rows
            # for them and do not add them to mosys_keys or tracking.
            if _is_auto_approved(codice, lotto):
                box_parts = [
                    str(row.get('BOX_X') or '').strip(),
                    str(row.get('BOX_Y') or '').strip(),
                    str(row.get('BOX_Z') or '').strip(),
                ]
                box = '-'.join(p for p in box_parts if p) or '-'
                stale = MatlotTracking.query.filter_by(
                    codice_materiale=codice, lotto=lotto, box=box
                ).first()
                if stale:
                    db.session.delete(stale)
                    current_app.logger.info(
                        f"MATLOT auto-approve: removed stale tracking row {codice}/{lotto}@{box}"
                    )
                continue

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

            # TASK3: key now includes box so each warehouse location is distinct
            mosys_keys.add((codice, lotto, box))

            tracking = MatlotTracking.query.filter_by(
                codice_materiale=codice, lotto=lotto, box=box
            ).first()

            if tracking is None:
                # Always seed as 'N' (pending). MOSYS incorrectly defaults
                # LOTTO_VERIFICATO='S' for every new batch, so its value is
                # not reliable for seeding. LINEA is the source of truth.
                tracking = MatlotTracking(
                    codice_materiale=codice,
                    lotto=lotto,
                    box=box,
                    prima_vista=today,
                    release_status='N',
                    giacenza_lotto=giacenza,
                )
                db.session.add(tracking)
                current_app.logger.info(
                    f"MATLOT new batch: {codice}/{lotto}@{box} → release_status='N'"
                )
                # Best-effort: correct MOSYS status from its default 'S' → 'N'
                mosys_status = str(row.get('LOTTO_VERIFICATO') or '').strip()
                if mosys_status == 'S':
                    try:
                        from MOSYS_data_functions import update_matlot_lotto_status
                        update_matlot_lotto_status(codice, lotto, 'N')
                    except Exception as mosys_err:
                        current_app.logger.warning(
                            f"MATLOT could not reset MOSYS status for new batch "
                            f"{codice}/{lotto}@{box}: {mosys_err}"
                        )
            else:
                pass  # existing row — no fields refreshed on re-sync

    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        msg = f"SQLite commit error: {e}"
        current_app.logger.error(f"MATLOT _sync_from_mosys: {msg}")
        return 0, msg

    return len(mosys_keys), None


def _get_tracking_rows():
    """Read all matlot_tracking rows from SQLite. No MOSYS call.

    giorni_disabled is computed — True when a 't'-prefix batch has been
    withdrawn (release_status='N' with withdrawn_at set), so the GIORNI
    waiting counter does not count time after a withdrawal.

    Returns a list of dicts with keys:
        codice_materiale, lotto, giacenza_lotto, box, prima_vista,
        giorni, giorni_disabled, is_past_due, release_status, released_at,
        withdrawn_at, withdrawal_reason, uwagi
    """
    today = date.today()
    rows = MatlotTracking.query.all()
    result = []
    for t in rows:
        prima_vista = t.prima_vista
        is_surowce = t.codice_materiale.lower().startswith('t')

        # Giorni calculation — only meaningful for surowce ('t' prefix) rows.
        # S status: count days the batch waited until approval (released_at − prima_vista).
        # N status: count days since first seen (today − prima_vista).
        # Non-surowce rows get 0 — the column is hidden in the UI for inserty.
        if is_surowce:
            if t.release_status == 'S' and t.released_at:
                giorni = (t.released_at.date() - prima_vista).days
            else:
                giorni = (today - prima_vista).days
        else:
            giorni = 0

        released_at_str = (
            t.released_at.strftime('%d.%m.%Y')
            if t.released_at else ''
        )
        withdrawn_at_str = (
            t.withdrawn_at.strftime('%d.%m.%Y %H:%M')
            if t.withdrawn_at else ''
        )
        # Disable GIORNI counter for withdrawn surowce rows (S→N reversal).
        giorni_disabled = (
            is_surowce
            and t.release_status == 'N'
            and t.withdrawn_at is not None
        )
        is_withdrawn = (t.withdrawn_at is not None) or bool(t.withdrawal_reason)
        result.append({
            'codice_materiale': t.codice_materiale,
            'nome_commerciale': _material_names.get(t.codice_materiale, ''),
            'is_insert':        t.codice_materiale in _insert_codes,
            'lotto':            t.lotto,
            'giacenza_lotto':   t.giacenza_lotto or 0,
            'box':              t.box or '-',
            'prima_vista':      prima_vista.strftime('%d.%m.%Y'),
            'giorni':           giorni,
            'giorni_disabled':  giorni_disabled,
            'is_past_due':      is_surowce and t.release_status == 'N' and giorni > 2 and not giorni_disabled,
            'is_withdrawn':     is_withdrawn,
            'release_status':   t.release_status,
            'released_at':      released_at_str,
            'withdrawn_at':     withdrawn_at_str,
            'withdrawal_reason': t.withdrawal_reason or '',
            'uwagi':            t.uwagi or '',
        })
    return result



# ── page route ────────────────────────────────────────────────────────────────

@matlot_bp.route('/matlot-status')
def matlot_status():
    """Raw material incoming inspection — MATLOT certificate monitoring view."""
    return render_template('matlot/matlot_status.html')


# ── API: refresh (MOSYS sync) ─────────────────────────────────────────────────

_MOSYS_SYNC_TIMEOUT = 15  # seconds before giving up on MOSYS connection

@matlot_bp.route('/api/matlot-refresh', methods=['POST'])
def api_matlot_refresh():
    """Sync MOSYS → matlot_tracking. Called on page load and Refresh button.

    Runs _sync_from_mosys in a thread with a hard timeout so the HTTP request
    always returns even when the Pervasive ODBC driver hangs on TCP connect.
    The background thread may keep running until the OS TCP timeout fires,
    but the user sees a fast error response instead of an infinite spinner.

    Flask's app context is thread-local, so we unwrap the real app object and
    push a fresh context inside the worker thread for SQLAlchemy access.
    """
    app = current_app._get_current_object()

    def _sync_with_context():
        with app.app_context():
            return _sync_from_mosys()

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(_sync_with_context)
        try:
            synced, error = future.result(timeout=_MOSYS_SYNC_TIMEOUT)
        except FuturesTimeoutError:
            current_app.logger.warning(
                f"MATLOT sync timed out after {_MOSYS_SYNC_TIMEOUT}s — MOSYS unreachable?"
            )
            return jsonify({
                'success':   False,
                'error':     f'MOSYS niedostępny (timeout {_MOSYS_SYNC_TIMEOUT}s)',
                'synced':    0,
                'timestamp': datetime.now().strftime('%H:%M:%S'),
            }), 504

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
    """Return matlot_tracking rows (SQLite only — no MOSYS call)."""
    sort_field    = request.args.get('sort', 'CODICE_MATERIALE')
    sort_dir      = request.args.get('dir', 'asc')
    limit         = request.args.get('limit', 100, type=int)
    offset        = request.args.get('offset', 0, type=int)
    priority_first = request.args.get('priority_first', 'false').lower() == 'true'

    category = request.args.get('category', '').strip().lower()
    status   = request.args.get('status', 'N').strip().upper()

    search = {
        'CODICE_MATERIALE': request.args.get('search_CODICE_MATERIALE', '').lower(),
        'LOTTO':            request.args.get('search_LOTTO', '').lower(),
        'BOX':              request.args.get('search_BOX', '').lower(),
    }

    try:
        rows = _get_tracking_rows()
        today_str = date.today().strftime('%d.%m.%Y')

        # Category filter
        if category == 'surowce':
            rows = [r for r in rows if r['codice_materiale'].lower().startswith('t')]
        elif category == 'inserty':
            rows = [r for r in rows if r.get('is_insert')]

        # Search filter
        for col, val in search.items():
            if val:
                key = VALID_SORT_FIELDS.get(col, col.lower())
                rows = [r for r in rows if val in str(r.get(key) or '').lower()]

        # Badge counts — always from pending items in current category,
        # independent of which status tab is active.
        pending_rows    = [r for r in rows if r['release_status'] == 'N']
        past_due_count  = sum(1 for r in pending_rows if r['is_past_due'])
        new_count       = sum(1 for r in pending_rows if r['prima_vista'] == today_str)
        urgent_count    = sum(1 for r in pending_rows if r['is_past_due'] or r['prima_vista'] == today_str)
        withdrawn_count = sum(1 for r in rows if r.get('is_withdrawn'))

        # Status / Pilne / badge-filter
        if status in ('N', 'S'):
            rows = [r for r in rows if r['release_status'] == status]
        elif status == 'PILNE':
            rows = [r for r in rows if r['release_status'] == 'N'
                    and (r['is_past_due'] or r['prima_vista'] == today_str)]
        elif status == 'PAST_DUE':
            rows = [r for r in rows if r['is_past_due']]
        elif status == 'NEW_TODAY':
            rows = [r for r in rows if r['prima_vista'] == today_str
                    and r['release_status'] == 'N']
        elif status == 'WITHDRAWN':
            rows = [r for r in rows if r.get('is_withdrawn')]

        total_count = len(rows)

        sort_key     = VALID_SORT_FIELDS.get(sort_field, 'codice_materiale')
        reverse      = sort_dir == 'desc'
        numeric_keys = {'giacenza_lotto', 'giorni'}
        date_keys    = {'prima_vista', 'released_at'}

        def _dmy_key(s):
            """Convert dd.mm.yyyy → (yyyy, mm, dd) tuple for correct date ordering."""
            try:
                d, m, y = str(s).split('.')
                return (int(y), int(m), int(d))
            except (ValueError, AttributeError):
                return (0, 0, 0)

        if sort_key in numeric_keys:
            rows.sort(key=lambda r: r.get(sort_key) or 0, reverse=reverse)
        elif sort_key in date_keys:
            rows.sort(key=lambda r: _dmy_key(r.get(sort_key) or ''), reverse=reverse)
        else:
            rows.sort(key=lambda r: str(r.get(sort_key) or '').lower(), reverse=reverse)

        # Priority-first: stable secondary sort puts red/yellow/grey rows on top.
        # Within each priority group the user's chosen sort order is preserved
        # because Python's sort() is stable.
        if priority_first:
            def _row_priority(r):
                if r['is_past_due']:
                    return 0   # red
                if r.get('is_withdrawn'):
                    return 1   # grey
                if r['release_status'] == 'N' and r.get('giorni', 0) >= 2 and not r.get('giorni_disabled'):
                    return 2   # yellow
                return 3       # normal
            rows.sort(key=_row_priority)

        page = rows[offset: offset + limit]

        return jsonify({
            'success':         True,
            'rows':            page,
            'past_due_count':  past_due_count,
            'new_count':       new_count,
            'urgent_count':    urgent_count,
            'withdrawn_count': withdrawn_count,
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


# ── API: release (N → S) ──────────────────────────────────────────────────────

@matlot_bp.route('/api/matlot-status/release', methods=['POST'])
def api_matlot_release():
    """Release a batch: update SQLite (primary) then MOSYS (best-effort).

    Requires: codice_materiale, lotto, box (TASK3: box is part of unique key).
    """
    data   = request.get_json(silent=True) or {}
    codice = str(data.get('codice_materiale') or '').strip()
    lotto  = str(data.get('lotto') or '').strip()
    box    = str(data.get('box') or '').strip() or '-'
    uwagi  = str(data.get('uwagi') or '').strip()

    if not codice or not lotto:
        return jsonify({'success': False, 'error': 'codice_materiale and lotto required'}), 400

    try:
        tracking = MatlotTracking.query.filter_by(
            codice_materiale=codice, lotto=lotto, box=box
        ).first()

        if not tracking:
            return jsonify({'success': False, 'error': 'Batch not found in tracking'}), 404

        if tracking.release_status == 'S':
            return jsonify({'success': False, 'error': 'Batch already released'}), 409

        tracking.release_status = 'S'
        tracking.released_at    = datetime.now()
        if uwagi:
            tracking.uwagi = uwagi
        db.session.commit()

    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"api_matlot_release SQLite error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

    # Best-effort MOSYS parallel write
    try:
        from MOSYS_data_functions import update_matlot_lotto_status
        ok = update_matlot_lotto_status(codice, lotto, 'S')
        if not ok:
            current_app.logger.warning(
                f"MOSYS write failed for release {codice}/{lotto}@{box}"
            )
    except Exception as mosys_err:
        current_app.logger.warning(
            f"MOSYS write exception for release {codice}/{lotto}@{box}: {mosys_err}"
        )

    return jsonify({'success': True})


# ── API: withdraw (S → N) — TASK0 ─────────────────────────────────────────────

@matlot_bp.route('/api/matlot-status/withdraw', methods=['POST'])
def api_matlot_withdraw():
    """Withdraw a released batch: revert release_status S → N.

    Records withdrawn_at and withdrawal_reason. For batches whose
    CODICE_MATERIALE starts with 't', giorni_disabled will be True after
    withdrawal (derived in _get_tracking_rows — no extra column needed).

    Requires: codice_materiale, lotto, box.
    Optional: withdrawal_reason.
    """
    data             = request.get_json(silent=True) or {}
    codice           = str(data.get('codice_materiale') or '').strip()
    lotto            = str(data.get('lotto') or '').strip()
    box              = str(data.get('box') or '').strip() or '-'
    withdrawal_reason = str(data.get('withdrawal_reason') or '').strip()

    if not codice or not lotto:
        return jsonify({'success': False, 'error': 'codice_materiale and lotto required'}), 400

    try:
        tracking = MatlotTracking.query.filter_by(
            codice_materiale=codice, lotto=lotto, box=box
        ).first()

        if not tracking:
            return jsonify({'success': False, 'error': 'Batch not found in tracking'}), 404

        if tracking.release_status == 'N':
            return jsonify({'success': False, 'error': 'Batch is not released'}), 409

        tracking.release_status   = 'N'
        tracking.withdrawn_at     = datetime.now()
        tracking.withdrawal_reason = withdrawal_reason or None
        db.session.commit()

    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"api_matlot_withdraw SQLite error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

    # Best-effort MOSYS parallel write
    try:
        from MOSYS_data_functions import update_matlot_lotto_status
        ok = update_matlot_lotto_status(codice, lotto, 'N')
        if not ok:
            current_app.logger.warning(
                f"MOSYS write failed for withdrawal {codice}/{lotto}@{box}"
            )
    except Exception as mosys_err:
        current_app.logger.warning(
            f"MOSYS write exception for withdrawal {codice}/{lotto}@{box}: {mosys_err}"
        )

    return jsonify({'success': True})


# ── API: edit uwagi — TASK4 ───────────────────────────────────────────────────

@matlot_bp.route('/api/matlot-status/uwagi', methods=['POST'])
def api_matlot_uwagi():
    """Update editable fields for a tracking row.

    Requires: codice_materiale, lotto, box.
    Optional:
        uwagi              — notes (empty string clears)
        release_status     — 'N' or 'S'; switching to 'N' always clears released_at
        prima_vista        — YYYY-MM-DD
        released_at        — YYYY-MM-DD (ignored when release_status='N')
        withdrawn_at       — YYYY-MM-DD (empty string clears / sets to None)
        withdrawal_reason  — free text  (empty string clears / sets to None)

    When release_status changes, MOSYS is updated best-effort after commit.
    """
    data              = request.get_json(silent=True) or {}
    codice            = str(data.get('codice_materiale') or '').strip()
    lotto             = str(data.get('lotto') or '').strip()
    box               = str(data.get('box') or '').strip() or '-'
    uwagi             = str(data.get('uwagi') or '').strip()
    new_status        = str(data.get('release_status') or '').strip().upper() or None
    prima_vista_str   = str(data.get('prima_vista') or '').strip() or None
    released_at_str   = str(data.get('released_at') or '').strip() or None
    # Withdrawal fields — key presence signals intent; empty string = clear
    _wa_raw           = data.get('withdrawn_at')
    _wr_raw           = data.get('withdrawal_reason')
    withdrawn_at_str  = str(_wa_raw).strip() if _wa_raw is not None else None
    withdrawal_reason = str(_wr_raw).strip() if _wr_raw is not None else None

    if not codice or not lotto:
        return jsonify({'success': False, 'error': 'codice_materiale and lotto required'}), 400

    if new_status and new_status not in ('N', 'S'):
        return jsonify({'success': False, 'error': 'release_status must be N or S'}), 400

    def _parse_date(s):
        y, m, d = s.split('-')
        return date(int(y), int(m), int(d))

    def _parse_datetime(s):
        y, m, d = s.split('-')
        return datetime(int(y), int(m), int(d))

    try:
        tracking = MatlotTracking.query.filter_by(
            codice_materiale=codice, lotto=lotto, box=box
        ).first()

        if not tracking:
            return jsonify({'success': False, 'error': 'Batch not found in tracking'}), 404

        old_status = tracking.release_status
        tracking.uwagi = uwagi or None

        if prima_vista_str:
            try:
                tracking.prima_vista = _parse_date(prima_vista_str)
            except (ValueError, AttributeError):
                return jsonify({'success': False, 'error': f'Invalid prima_vista: {prima_vista_str}'}), 400

        if new_status:
            tracking.release_status = new_status
            if new_status == 'N':
                tracking.released_at = None          # always clear when reverting to pending
            elif new_status == 'S' and released_at_str:
                try:
                    tracking.released_at = _parse_datetime(released_at_str)
                except (ValueError, AttributeError):
                    return jsonify({'success': False, 'error': f'Invalid released_at: {released_at_str}'}), 400
        elif released_at_str:
            # Status unchanged — date edited directly
            try:
                tracking.released_at = _parse_datetime(released_at_str)
            except (ValueError, AttributeError):
                return jsonify({'success': False, 'error': f'Invalid released_at: {released_at_str}'}), 400

        # Withdrawal fields — only touched when key was present in the payload
        if withdrawn_at_str is not None:
            if withdrawn_at_str == '':
                tracking.withdrawn_at = None          # clear
            else:
                try:
                    tracking.withdrawn_at = _parse_datetime(withdrawn_at_str)
                except (ValueError, AttributeError):
                    return jsonify({'success': False, 'error': f'Invalid withdrawn_at: {withdrawn_at_str}'}), 400

        if withdrawal_reason is not None:
            tracking.withdrawal_reason = withdrawal_reason or None   # '' → None

        db.session.commit()

    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"api_matlot_uwagi SQLite error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

    # Best-effort MOSYS sync when release_status changed
    if new_status and new_status != old_status:
        try:
            from MOSYS_data_functions import update_matlot_lotto_status
            ok = update_matlot_lotto_status(codice, lotto, new_status)
            if not ok:
                current_app.logger.warning(
                    f"MOSYS write failed for status edit {codice}/{lotto}@{box} → {new_status}"
                )
        except Exception as mosys_err:
            current_app.logger.warning(
                f"MOSYS write exception for status edit {codice}/{lotto}@{box}: {mosys_err}"
            )

    return jsonify({'success': True})


# ── API: delete row ───────────────────────────────────────────────────────────

@matlot_bp.route('/api/matlot-status/delete', methods=['POST'])
def api_matlot_delete():
    """Delete a tracking row from SQLite. No MOSYS update.

    Requires: codice_materiale, lotto, box.
    """
    data   = request.get_json(silent=True) or {}
    codice = str(data.get('codice_materiale') or '').strip()
    lotto  = str(data.get('lotto') or '').strip()
    box    = str(data.get('box') or '').strip() or '-'

    if not codice or not lotto:
        return jsonify({'success': False, 'error': 'codice_materiale and lotto required'}), 400

    try:
        tracking = MatlotTracking.query.filter_by(
            codice_materiale=codice, lotto=lotto, box=box
        ).first()

        if not tracking:
            return jsonify({'success': False, 'error': 'Batch not found in tracking'}), 404

        db.session.delete(tracking)
        db.session.commit()

    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"api_matlot_delete SQLite error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

    return jsonify({'success': True})


# ── API: bulk release ─────────────────────────────────────────────────────────

@matlot_bp.route('/api/matlot-status/bulk-release', methods=['POST'])
def api_matlot_bulk_release():
    """Release all pending rows that match the client's current search filters.

    The client sends the same filters it uses for display so the server applies
    them identically — this covers rows not yet loaded by the infinite scroll.
    Only rows with release_status='N' are affected (pending rows).

    Body JSON:
        uwagi    — shared note applied to every released row (optional)
        category — 'surowce' | 'inserty' | '' (same as list endpoint)
        search   — dict of { 'CODICE_MATERIALE': '...', 'LOTTO': '...', 'BOX': '...' }

    Returns:
        { success, released_count }
    """
    data     = request.get_json(silent=True) or {}
    uwagi    = str(data.get('uwagi') or '').strip()
    category = str(data.get('category') or '').strip().lower()
    search   = data.get('search') or {}

    try:
        rows = _get_tracking_rows()

        # Only target pending rows
        rows = [r for r in rows if r['release_status'] == 'N']

        # Same category filter as the list endpoint
        if category == 'surowce':
            rows = [r for r in rows if r['codice_materiale'].lower().startswith('t')]
        elif category == 'inserty':
            rows = [r for r in rows if r.get('is_insert')]

        # Same column search filter as the list endpoint
        for col, val in search.items():
            val = str(val or '').strip().lower()
            if val:
                key = VALID_SORT_FIELDS.get(col, col.lower())
                rows = [r for r in rows if val in str(r.get(key) or '').lower()]

        if not rows:
            return jsonify({'success': False, 'error': 'Brak pasujących rekordów do zatwierdzenia'}), 404

        now = datetime.now()
        released_count = 0

        for row in rows:
            tracking = MatlotTracking.query.filter_by(
                codice_materiale=row['codice_materiale'],
                lotto=row['lotto'],
                box=row['box'],
            ).first()
            if tracking and tracking.release_status == 'N':
                tracking.release_status = 'S'
                tracking.released_at    = now
                if uwagi:
                    tracking.uwagi = uwagi
                released_count += 1

        db.session.commit()

    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"api_matlot_bulk_release SQLite error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

    # Best-effort MOSYS writes — one per row, failures logged and ignored
    try:
        from MOSYS_data_functions import update_matlot_lotto_status
        for row in rows:
            try:
                update_matlot_lotto_status(row['codice_materiale'], row['lotto'], 'S')
            except Exception as mosys_err:
                current_app.logger.warning(
                    f"MOSYS bulk write failed for {row['codice_materiale']}/{row['lotto']}: {mosys_err}"
                )
    except Exception:
        pass

    return jsonify({'success': True, 'released_count': released_count})


# ── API: bulk status change ───────────────────────────────────────────────────

@matlot_bp.route('/api/matlot-status/bulk-status', methods=['POST'])
def api_matlot_bulk_status():
    """Change release_status for all filtered rows whose current status matches original_status.

    Rows that already have the new status (or a different status) are skipped.
    When N→S: sets released_at = now.  When S→N: clears released_at.

    Body JSON:
        original_status — 'N' or 'S': only rows at this status are updated
        new_status      — 'N' or 'S': target status
        category        — 'surowce' | 'inserty' | ''
        search          — dict of { CODICE_MATERIALE, LOTTO, BOX } search filters

    Returns:
        { success, updated_count }
    """
    data            = request.get_json(silent=True) or {}
    original_status = str(data.get('original_status') or '').strip().upper()
    new_status      = str(data.get('new_status')      or '').strip().upper()
    category        = str(data.get('category')        or '').strip().lower()
    search          = data.get('search') or {}

    if original_status not in ('N', 'S') or new_status not in ('N', 'S'):
        return jsonify({'success': False, 'error': 'original_status and new_status must be N or S'}), 400

    if original_status == new_status:
        return jsonify({'success': True, 'updated_count': 0})

    try:
        rows = _get_tracking_rows()

        if category == 'surowce':
            rows = [r for r in rows if r['codice_materiale'].lower().startswith('t')]
        elif category == 'inserty':
            rows = [r for r in rows if r.get('is_insert')]

        for col, val in search.items():
            val = str(val or '').strip().lower()
            if val:
                key = VALID_SORT_FIELDS.get(col, col.lower())
                rows = [r for r in rows if val in str(r.get(key) or '').lower()]

        # Only target rows that still have original_status
        rows = [r for r in rows if r['release_status'] == original_status]

        if not rows:
            return jsonify({'success': True, 'updated_count': 0})

        now = datetime.now()
        updated_count = 0

        for row in rows:
            tracking = MatlotTracking.query.filter_by(
                codice_materiale=row['codice_materiale'],
                lotto=row['lotto'],
                box=row['box'],
            ).first()
            if tracking and tracking.release_status == original_status:
                tracking.release_status = new_status
                if new_status == 'S':
                    tracking.released_at = now
                else:
                    tracking.released_at = None
                updated_count += 1

        db.session.commit()

    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"api_matlot_bulk_status SQLite error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

    # Best-effort MOSYS writes — one per row, failures logged and ignored
    try:
        from MOSYS_data_functions import update_matlot_lotto_status
        for row in rows:
            try:
                update_matlot_lotto_status(row['codice_materiale'], row['lotto'], new_status)
            except Exception as mosys_err:
                current_app.logger.warning(
                    f"MOSYS bulk-status write failed for "
                    f"{row['codice_materiale']}/{row['lotto']}: {mosys_err}"
                )
    except Exception:
        pass

    return jsonify({'success': True, 'updated_count': updated_count})


# ── API: bulk uwagi (all fields) ──────────────────────────────────────────────

_MISSING = object()  # sentinel — distinguishes absent JSON key from null/empty value


@matlot_bp.route('/api/matlot-status/bulk-uwagi', methods=['POST'])
def api_matlot_bulk_uwagi():
    """Apply modal fields to all filtered rows except the already-saved exclude_key row.

    Only fields explicitly present in the 'fields' dict are applied.
    release_status change only applies to rows whose current status matches original_status.

    Body JSON:
        category        — 'surowce' | 'inserty' | ''
        search          — dict of column filters
        exclude_key     — {codice_materiale, lotto, box} row already saved via /uwagi
        original_status — 'N' | 'S' | '' — status filter for release_status changes
        fields:
            uwagi             — str (apply to all; empty string clears)
            prima_vista       — YYYY-MM-DD or absent (skip)
            released_at       — YYYY-MM-DD or absent (skip)
            release_status    — 'N' | 'S' or absent (skip)
            withdrawn_at      — YYYY-MM-DD or '' (clear) or absent (skip)
            withdrawal_reason — str or '' (clear) or absent (skip)

    Returns:
        { success, updated_count }
    """
    data            = request.get_json(silent=True) or {}
    category        = str(data.get('category')        or '').strip().lower()
    search          = data.get('search') or {}
    exclude_key     = data.get('exclude_key') or {}
    original_status = str(data.get('original_status') or '').strip().upper()
    fields          = data.get('fields') or {}

    # Extract each field — use _MISSING so absent key ≠ empty string
    f_uwagi             = fields.get('uwagi',             _MISSING)
    f_prima_vista       = fields.get('prima_vista',       _MISSING)
    f_released_at       = fields.get('released_at',       _MISSING)
    f_release_status    = fields.get('release_status',    _MISSING)
    f_withdrawn_at      = fields.get('withdrawn_at',      _MISSING)
    f_withdrawal_reason = fields.get('withdrawal_reason', _MISSING)

    exclude_codice = str(exclude_key.get('codice_materiale') or '').strip()
    exclude_lotto  = str(exclude_key.get('lotto')            or '').strip()
    exclude_box    = str(exclude_key.get('box')              or '').strip() or '-'

    def _parse_date(s):
        y, m, d = str(s).split('-')
        return date(int(y), int(m), int(d))

    def _parse_dt(s):
        y, m, d = str(s).split('-')
        return datetime(int(y), int(m), int(d))

    try:
        rows = _get_tracking_rows()

        if category == 'surowce':
            rows = [r for r in rows if r['codice_materiale'].lower().startswith('t')]
        elif category == 'inserty':
            rows = [r for r in rows if r.get('is_insert')]

        for col, val in search.items():
            val = str(val or '').strip().lower()
            if val:
                key = VALID_SORT_FIELDS.get(col, col.lower())
                rows = [r for r in rows if val in str(r.get(key) or '').lower()]

        # Exclude the single row already saved via /uwagi
        if exclude_codice and exclude_lotto:
            rows = [r for r in rows if not (
                r['codice_materiale'] == exclude_codice
                and r['lotto'] == exclude_lotto
                and r['box'] == exclude_box
            )]

        if not rows:
            return jsonify({'success': True, 'updated_count': 0})

        now = datetime.now()
        updated_count = 0
        status_changed_rows = []  # for MOSYS best-effort sync

        for row in rows:
            tracking = MatlotTracking.query.filter_by(
                codice_materiale=row['codice_materiale'],
                lotto=row['lotto'],
                box=row['box'],
            ).first()
            if not tracking:
                continue

            if f_uwagi is not _MISSING:
                tracking.uwagi = str(f_uwagi).strip() or None

            if f_prima_vista is not _MISSING and f_prima_vista:
                try:
                    tracking.prima_vista = _parse_date(f_prima_vista)
                except (ValueError, AttributeError):
                    pass  # skip malformed date

            if f_release_status is not _MISSING and f_release_status in ('N', 'S'):
                # Only flip rows that currently have original_status (or if no filter given)
                if not original_status or tracking.release_status == original_status:
                    if tracking.release_status != f_release_status:
                        old_st = tracking.release_status
                        tracking.release_status = f_release_status
                        if f_release_status == 'S':
                            tracking.released_at = now
                        else:
                            tracking.released_at = None
                        status_changed_rows.append((row['codice_materiale'], row['lotto'], f_release_status))

            if f_released_at is not _MISSING and f_released_at:
                try:
                    tracking.released_at = _parse_dt(f_released_at)
                except (ValueError, AttributeError):
                    pass

            if f_withdrawn_at is not _MISSING:
                wa = str(f_withdrawn_at).strip() if f_withdrawn_at is not None else ''
                tracking.withdrawn_at = None if wa == '' else _parse_dt(wa) if wa else None

            if f_withdrawal_reason is not _MISSING:
                wr = str(f_withdrawal_reason).strip() if f_withdrawal_reason is not None else ''
                tracking.withdrawal_reason = wr or None

            updated_count += 1

        db.session.commit()

    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"api_matlot_bulk_uwagi SQLite error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

    # Best-effort MOSYS sync for status changes
    if status_changed_rows:
        try:
            from MOSYS_data_functions import update_matlot_lotto_status
            for codice, lotto, new_st in status_changed_rows:
                try:
                    update_matlot_lotto_status(codice, lotto, new_st)
                except Exception as mosys_err:
                    current_app.logger.warning(
                        f"MOSYS bulk-uwagi status write failed for {codice}/{lotto}: {mosys_err}"
                    )
        except Exception:
            pass

    return jsonify({'success': True, 'updated_count': updated_count})


# ── API: bulk delete ──────────────────────────────────────────────────────────

@matlot_bp.route('/api/matlot-status/bulk-delete', methods=['POST'])
def api_matlot_bulk_delete():
    """Delete all rows that match the current category + search filters.

    Body JSON:
        category — 'surowce' | 'inserty' | ''
        search   — dict of { CODICE_MATERIALE, LOTTO, BOX } search filters

    Returns:
        { success, deleted_count }
    """
    data     = request.get_json(silent=True) or {}
    category = str(data.get('category') or '').strip().lower()
    search   = data.get('search') or {}

    try:
        rows = _get_tracking_rows()

        if category == 'surowce':
            rows = [r for r in rows if r['codice_materiale'].lower().startswith('t')]
        elif category == 'inserty':
            rows = [r for r in rows if r.get('is_insert')]

        for col, val in search.items():
            val = str(val or '').strip().lower()
            if val:
                key = VALID_SORT_FIELDS.get(col, col.lower())
                rows = [r for r in rows if val in str(r.get(key) or '').lower()]

        if not rows:
            return jsonify({'success': False, 'error': 'Brak pasujących rekordów do usunięcia'}), 404

        deleted_count = 0
        for row in rows:
            tracking = MatlotTracking.query.filter_by(
                codice_materiale=row['codice_materiale'],
                lotto=row['lotto'],
                box=row['box'],
            ).first()
            if tracking:
                db.session.delete(tracking)
                deleted_count += 1

        db.session.commit()

    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"api_matlot_bulk_delete SQLite error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

    return jsonify({'success': True, 'deleted_count': deleted_count})
