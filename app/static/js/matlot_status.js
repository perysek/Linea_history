/**
 * matlot_status.js — MATLOT incoming raw material inspection
 * Follows the same infinite-scroll / sort / search pattern as linea.js
 */

let currentSort     = { field: 'CODICE_MATERIALE', direction: 'asc' };
let sortExplicit    = false;   // true once user explicitly clicks a sort column
let searchFilters   = {};
let currentCategory = 'surowce';   // 'surowce' or 'inserty'
let currentStatus   = 'N';  // 'N' (pending), 'S' (released), or 'ALL'
let allRecords      = [];
let isLoading       = false;
let currentAbortController = null;

// Pagination state
let currentOffset = 0;
let totalRecords  = 0;
const RECORDS_PER_PAGE = 100;
let hasMoreRecords = false;

// Enhanced edit mode — toggled via the sidebar version button; persisted in localStorage
let enhancedEdit = localStorage.getItem('matlot-enhanced-edit') === '1';

// Today's date in dd.mm.yyyy — used to detect new rows added on current day
const _todayDmy = (() => {
    const d = new Date();
    return `${String(d.getDate()).padStart(2,'0')}.${String(d.getMonth()+1).padStart(2,'0')}.${d.getFullYear()}`;
})();

// Release modal state
let _pendingCodice          = '';
let _pendingLotto           = '';
let _pendingBox             = '';
let _pendingBtn             = null;
let _pendingReleaseStatus   = 'N';
let _pendingOriginalStatus  = 'N';  // status when modal was opened (bulk mode baseline)

/** True when enhanced-edit is on AND the bulk-release button is enabled. */
function isBulkMode() {
    const btn = document.getElementById('btn-bulk-release');
    return enhancedEdit && btn && !btn.disabled;
}

// Initialize on page load — sync from MOSYS first, then display data
document.addEventListener('DOMContentLoaded', () => {
    syncFromMosys().then(() => fetchRecords());

    // Wire enhanced-edit toggle button (sidebar footer, next to v1.0.0)
    const toggleBtn = document.getElementById('toggle-enhanced-edit');
    if (toggleBtn) {
        toggleBtn.style.opacity = enhancedEdit ? '0.55' : '0.15';
        toggleBtn.addEventListener('click', () => {
            enhancedEdit = !enhancedEdit;
            localStorage.setItem('matlot-enhanced-edit', enhancedEdit ? '1' : '0');
            toggleBtn.style.opacity = enhancedEdit ? '0.55' : '0.15';
        });
    }

    // Infinite scroll on table body
    const tbodyScroll = document.querySelector('.tbody-scroll');
    if (tbodyScroll) {
        tbodyScroll.addEventListener('scroll', () => {
            if (isLoading || !hasMoreRecords) return;
            const { scrollTop, scrollHeight, clientHeight } = tbodyScroll;
            if (scrollTop + clientHeight >= scrollHeight - 100) {
                currentOffset += RECORDS_PER_PAGE;
                fetchRecords(false);
            }
        });
    }

    // Setup column search inputs with server-side filtering
    document.querySelectorAll('.column-search').forEach(input => {
        input.addEventListener('input', debounce(() => {
            searchFilters[input.dataset.column] = input.value.trim();
            applyFilters();
            updateClearFiltersButton();
        }, 300));
    });

    // Close modals on Escape
    document.addEventListener('keydown', e => {
        if (e.key === 'Escape') {
            closeReleaseModal();
            closeWithdrawModal();
            closeUwagiModal();
            closeBulkReleaseModal();
        }
    });

    // Initial button state (no filters active on load)
    updateBulkReleaseButton();
});

/**
 * Set active material category (Surowce / Inserty) and refetch.
 * TASK2: toggling to inserty suppresses the GIORNI column header and alert system.
 */
function setCategory(cat) {
    currentCategory = cat;
    sortExplicit    = false;   // re-enable priority-first on view switch
    document.getElementById('cat-surowce').classList.toggle('active', cat === 'surowce');
    document.getElementById('cat-inserty').classList.toggle('active', cat === 'inserty');

    // TASK2: suppress GIORNI column header for inserty (cells handled in buildRowHtml)
    const thGiorni = document.getElementById('th-giorni');
    if (thGiorni) {
        thGiorni.style.opacity       = cat === 'inserty' ? '0' : '';
        thGiorni.style.pointerEvents = cat === 'inserty' ? 'none' : '';
    }

    fetchRecords(true);
}

/**
 * Set active status view (Oczekujące / Zatwierdzone / Wszystkie / Pilne / badge filters)
 * and refetch. Badge-filter stats: 'PAST_DUE', 'NEW_TODAY', 'WITHDRAWN'.
 */
function setStatus(stat) {
    currentStatus = stat;
    sortExplicit  = false;   // re-enable priority-first on view switch
    const badgeStats = new Set(['PAST_DUE', 'NEW_TODAY', 'WITHDRAWN']);
    // Regular toggle buttons: active only when a non-badge stat is selected
    document.getElementById('stat-pending').classList.toggle('active',  stat === 'N');
    document.getElementById('stat-released').classList.toggle('active', stat === 'S');
    document.getElementById('stat-all').classList.toggle('active',      stat === 'ALL');
    document.getElementById('stat-pilne').classList.toggle('active',    stat === 'PILNE');
    // Badge pills: active when their specific filter is selected
    const pastDuePill  = document.getElementById('past-due-pill');
    const newRowsPill  = document.getElementById('new-rows-pill');
    const withdrawnPill = document.getElementById('withdrawn-pill');
    if (pastDuePill)   pastDuePill.classList.toggle('pill-active',   stat === 'PAST_DUE');
    if (newRowsPill)   newRowsPill.classList.toggle('pill-active',   stat === 'NEW_TODAY');
    if (withdrawnPill) withdrawnPill.classList.toggle('pill-active', stat === 'WITHDRAWN');
    updateBulkReleaseButton();
    fetchRecords(true);
}

/**
 * Toggle a badge-based filter. Clicking an active badge deactivates it (back to 'N').
 */
function toggleBadgeFilter(badgeStat) {
    setStatus(currentStatus === badgeStat ? 'N' : badgeStat);
}

/**
 * POST /api/matlot-refresh — sync MOSYS → SQLite, update timestamp label.
 * Always resolves (never rejects) so callers can safely chain .then().
 */
async function syncFromMosys() {
    const btn  = document.getElementById('btn-refresh-mosys');
    const icon = document.getElementById('refresh-spin-icon');
    const ts   = document.getElementById('sync-timestamp');

    if (btn)  btn.disabled = true;
    if (icon) icon.style.animation = 'spin 0.8s linear infinite';

    const tbody = document.getElementById('matlot-tbody');
    if (tbody && allRecords.length === 0) {
        tbody.innerHTML = '<tr><td colspan="9" style="text-align: center; padding: 2rem; color: var(--color-ink-muted);">Synchronizacja z MOSYS...</td></tr>';
    }

    try {
        const response = await fetch('/api/matlot-refresh', { method: 'POST' });
        const data = await response.json();
        if (ts) {
            ts.textContent = data.success
                ? `Zsynchronizowano: ${data.timestamp} (${data.synced} partii)`
                : `Błąd synchronizacji: ${data.error || 'nieznany błąd'}`;
        }
    } catch (err) {
        console.error('MOSYS refresh error:', err);
        if (ts) ts.textContent = 'Błąd połączenia z MOSYS';
    } finally {
        if (icon) icon.style.animation = '';
        if (btn)  btn.disabled = false;
    }
}

/**
 * Fetch records via AJAX with pagination
 */
async function fetchRecords(resetOffset = true) {
    if (currentAbortController) {
        currentAbortController.abort();
    }

    if (resetOffset) {
        currentOffset = 0;
        allRecords = [];
    }

    currentAbortController = new AbortController();
    isLoading = true;

    const loadingIndicator = document.getElementById('scroll-loading-indicator');
    if (loadingIndicator && !resetOffset) {
        loadingIndicator.style.display = 'inline-flex';
    }

    const tbody = document.getElementById('matlot-tbody');
    const params = new URLSearchParams();

    params.append('sort', currentSort.field);
    params.append('dir', currentSort.direction);
    params.append('status', currentStatus);
    params.append('priority_first', sortExplicit ? 'false' : 'true');
    if (currentCategory) params.append('category', currentCategory);

    Object.entries(searchFilters).forEach(([key, value]) => {
        if (value) params.append(`search_${key}`, value);
    });

    params.append('limit', RECORDS_PER_PAGE);
    params.append('offset', currentOffset);

    if (currentOffset === 0) {
        tbody.innerHTML = '<tr><td colspan="9" style="text-align: center; padding: 2rem; color: var(--color-ink-muted);">Ładowanie...</td></tr>';
    }

    try {
        const response = await fetch(`/api/matlot-status?${params}`, {
            signal: currentAbortController.signal
        });
        const data = await response.json();

        if (data.success) {
            totalRecords  = data.pagination.total;
            currentOffset = data.pagination.offset;

            if (resetOffset) {
                allRecords = data.rows;
                renderRecordsDirect(allRecords);
            } else {
                appendRecordsDirect(data.rows);
                allRecords = allRecords.concat(data.rows);
            }

            updateCount(allRecords.length, totalRecords);
            updateLoadMoreButton(data.pagination);
            updatePastDuePill(data.past_due_count);
            updateNewRowsPill(data.new_count || 0);
            updatePilneButton(data.urgent_count || 0);
            updateWithdrawnPill(data.withdrawn_count || 0);
        } else {
            tbody.innerHTML = '<tr><td colspan="9" style="text-align: center; padding: 2rem; color: var(--color-error);">Błąd ładowania danych z MOSYS</td></tr>';
        }
    } catch (error) {
        if (error.name === 'AbortError') {
            // cancelled — new request in progress
        } else {
            console.error('Error fetching MATLOT records:', error);
            tbody.innerHTML = `
                <tr>
                    <td colspan="9" style="text-align: center; padding: 3rem; color: var(--color-ink-muted);">
                        <p style="font-size: 1rem; font-weight: 500; color: #c33; margin-bottom: 0.5rem;">Błąd połączenia z MOSYS</p>
                        <p style="font-size: 0.875rem;">Nie udało się pobrać danych o partiach materiałowych.</p>
                    </td>
                </tr>`;
        }
    } finally {
        isLoading = false;
        currentAbortController = null;
        if (loadingIndicator) loadingIndicator.style.display = 'none';
    }
}

/**
 * Build HTML string for a single MATLOT record row (9 columns).
 *
 * TASK2: when currentCategory === 'inserty', no alert row classes are applied
 *        and the GIORNI cell is rendered empty.
 * TASK0: released badge becomes a withdrawal button; giorni_disabled suppresses
 *        the waiting counter for any withdrawn row (regardless of codice prefix).
 * TASK4: every row gets an edit-uwagi icon button.
 * Uwagi column: withdrawal_reason+withdrawn_at / uwagi (S) / empty (N pending)
 */
function buildRowHtml(record) {
    const isReleased   = record.release_status === 'S';
    const isInserty    = currentCategory === 'inserty';
    const giorniHide   = isInserty || record.giorni_disabled;

    // TASK2: no alert row classes for inserty category
    const isNew      = record.prima_vista === _todayDmy && record.release_status === 'N';
    const isWithdrawn = record.is_withdrawn;
    const rowClass = (isInserty
        ? 'stagger-row'
        : isReleased
            ? 'stagger-row row-released'
            : isWithdrawn
                ? 'stagger-row row-withdrawn'
                : record.is_past_due
                    ? 'stagger-row row-past-due'
                    : record.giorni >= 2
                        ? 'stagger-row row-warning'
                        : 'stagger-row')
        + (isNew ? ' row-new' : '');

    const giorniLabel = record.giorni === 0
        ? 'dziś'
        : record.giorni === 1
            ? '1 dzień'
            : `${record.giorni} dni`;

    const fmt = n => Number(n).toLocaleString('pl-PL').replace(/,/g, ' ');

    // Escape values for inline onclick attributes (XSS-safe)
    const co  = escapeAttr(record.codice_materiale);
    const lo  = escapeAttr(record.lotto);
    const bo  = escapeAttr(record.box);
    const uwa = escapeAttr(record.uwagi || '');

    // Uwagi column — 3-way branch based on row state
    let uwagiCellContent = '';
    if (isWithdrawn) {
        const reason = record.withdrawal_reason
            ? escapeHtml(record.withdrawal_reason)
            : '<em style="color:var(--color-ink-muted);">brak powodu</em>';
        uwagiCellContent = `
            <span style="color:#991b1b; line-height:1.3;">${reason}</span>
            <span style="display:block; font-size:0.625rem; color:var(--color-ink-muted); margin-top:0.1rem;">
                cofnięto ${escapeHtml(record.withdrawn_at)}
            </span>`;
    } else if (record.uwagi) {
        uwagiCellContent = escapeHtml(record.uwagi);
    }

    // TASK0: released badge → clickable withdrawal button
    const primaryAction = isReleased
        ? `<button class="badge-released"
                   title="Kliknij, aby cofnąć zatwierdzenie"
                   onclick="openWithdrawModal(this, '${co}', '${lo}', '${bo}')">
               <svg width="11" height="11" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                   <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M5 13l4 4L19 7" />
               </svg>
               OK
           </button>`
        : `<button class="btn-release"
                   onclick="openReleaseModal(this, '${co}', '${lo}', '${bo}')">
               <svg width="11" height="11" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                   <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M5 13l4 4L19 7" />
               </svg>
               Zatwierdź
           </button>`;

    // TASK4: uwagi edit button — always shown for all rows
    const rs  = escapeAttr(record.release_status    || 'N');
    const pv  = escapeAttr(record.prima_vista       || '');
    const rat = escapeAttr(record.released_at       || '');
    const wat = escapeAttr(record.withdrawn_at      || '');
    const wre = escapeAttr(record.withdrawal_reason || '');
    const editUwagiBtn = `<button class="btn-edit-uwagi" title="Edytuj partię"
                onclick="openUwagiModal(this, '${co}', '${lo}', '${bo}', '${uwa}', '${rs}', '${pv}', '${rat}', '${wat}', '${wre}')">
                <svg width="13" height="13" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
                          d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z" />
                </svg>
            </button>`;

    return `
        <tr class="${rowClass}"
            data-codice="${escapeAttr(record.codice_materiale)}"
            data-lotto="${escapeAttr(record.lotto)}"
            data-box="${escapeAttr(record.box)}">
            <td style="font-weight: 500;">${escapeHtml(record.codice_materiale)}</td>
            <td style="color: var(--color-ink-muted);">${escapeHtml(record.nome_commerciale || '—')}</td>
            <td>${escapeHtml(record.lotto)}</td>
            <td style="text-align: right; padding-right: 1.5rem; font-weight: 500;">${fmt(record.giacenza_lotto)}</td>
            <td style="white-space: nowrap;">${escapeHtml(record.box)}</td>
            <td style="white-space: nowrap;">${escapeHtml(record.prima_vista)}</td>
            <td style="white-space: nowrap; color: ${isReleased ? '#15803d' : 'var(--color-ink-muted)'};">
                ${escapeHtml(record.released_at || '—')}
            </td>
            <td style="max-width: 0; overflow: hidden; padding-right: 0.5rem;">
                ${uwagiCellContent}
            </td>
            <td style="text-align: center;">
                ${giorniHide ? '' : `<span class="badge-giorni">${escapeHtml(giorniLabel)}</span>`}
            </td>
            <td style="text-align: center;">
                <div style="display: inline-flex; align-items: left; gap: 0.15rem;">
                    ${primaryAction}
                    ${editUwagiBtn}
                </div>
            </td>
        </tr>`;
}

/**
 * Render records directly — replaces entire tbody (initial load / sort / filter)
 */
function renderRecordsDirect(records) {
    const tbody = document.getElementById('matlot-tbody');

    if (records.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="9" style="text-align: center; padding: 3rem; color: var(--color-ink-muted);">
                    <p style="font-size: 1.125rem; font-weight: 500;">Brak partii spełniających kryteria</p>
                    <p style="font-size: 0.875rem; margin-top: 0.25rem;">Zmień filtr lub wybierz inny widok.</p>
                </td>
            </tr>`;
        return;
    }

    tbody.innerHTML = records.map(r => buildRowHtml(r)).join('');
}

/**
 * Append new records to tbody — avoids re-rendering existing rows on scroll
 */
function appendRecordsDirect(records) {
    const tbody = document.getElementById('matlot-tbody');
    tbody.insertAdjacentHTML('beforeend', records.map(r => buildRowHtml(r)).join(''));
}

/**
 * Apply filters — refetch from server with new filters
 */
function applyFilters() {
    fetchRecords(true);
}

/**
 * Sort table by column
 */
function sortTable(field) {
    if (currentSort.field === field) {
        currentSort.direction = currentSort.direction === 'asc' ? 'desc' : 'asc';
    } else {
        currentSort.field = field;
        currentSort.direction = 'asc';
    }
    sortExplicit = true;
    fetchRecords(true);
}

/**
 * Update record count display
 */
function updateCount(visible, total) {
    document.getElementById('visible-count').textContent = visible;
    document.getElementById('total-count').textContent   = total;
    document.getElementById('row-count').textContent     = `${total} pozycji`;
}

/**
 * Update infinite scroll state
 */
function updateLoadMoreButton(pagination) {
    hasMoreRecords = pagination.has_more;
}

/**
 * Show/hide past-due pill.
 * TASK2: suppressed when viewing inserty category.
 */
function updatePastDuePill(pastDueCount) {
    const pill = document.getElementById('past-due-pill');
    if (!pill) return;
    // TASK2: no past-due alerting for inserty
    if (pastDueCount > 0 && currentCategory !== 'inserty') {
        pill.textContent   = `${pastDueCount} po terminie`;
        pill.style.display = '';
    } else {
        pill.style.display = 'none';
    }
}

function updateNewRowsPill(newCount) {
    const pill = document.getElementById('new-rows-pill');
    if (!pill) return;
    if (newCount > 0) {
        pill.textContent   = `${newCount} ${newCount === 1 ? 'nowa' : 'nowych'}`;
        pill.style.display = '';
    } else {
        pill.style.display = 'none';
    }
}

function updatePilneButton(urgentCount) {
    const btn = document.getElementById('stat-pilne');
    if (!btn) return;
    btn.style.display = urgentCount > 0 ? '' : 'none';
    // If currently in PILNE view and nothing urgent remains, drop back to N
    if (urgentCount === 0 && currentStatus === 'PILNE') {
        setStatus('N');
    }
}

function updateWithdrawnPill(withdrawnCount) {
    const pill = document.getElementById('withdrawn-pill');
    if (!pill) return;
    if (withdrawnCount > 0) {
        pill.textContent   = `${withdrawnCount} ${withdrawnCount === 1 ? 'cofnięta' : 'cofniętych'}`;
        pill.style.display = '';
    } else {
        pill.style.display = 'none';
    }
}

// ── Release modal (N → S) ─────────────────────────────────────────────────────

/**
 * Open the release confirmation modal for a given batch.
 * box parameter added for TASK3 (unique key now includes box).
 */
function openReleaseModal(btn, codice, lotto, box) {
    _pendingCodice = codice;
    _pendingLotto  = lotto;
    _pendingBox    = box;
    _pendingBtn    = btn;

    document.getElementById('release-modal-codice').textContent = codice;
    document.getElementById('release-modal-lotto').textContent  = lotto;
    document.getElementById('release-modal-label').textContent  = `${codice} / ${lotto}`;
    document.getElementById('release-uwagi').value = '';

    const confirmBtn = document.getElementById('release-confirm-btn');
    confirmBtn.disabled    = false;
    confirmBtn.textContent = 'Zatwierdź dostawę';

    document.getElementById('release-modal').classList.add('active');
    setTimeout(() => document.getElementById('release-uwagi').focus(), 100);
}

function closeReleaseModal() {
    document.getElementById('release-modal').classList.remove('active');
    _pendingCodice = '';
    _pendingLotto  = '';
    _pendingBox    = '';
    _pendingBtn    = null;
}

function closeReleaseModalOnBackdrop(event) {
    if (event.target.id === 'release-modal') closeReleaseModal();
}

async function submitRelease() {
    const codice = _pendingCodice;
    const lotto  = _pendingLotto;
    const box    = _pendingBox;
    const uwagi  = document.getElementById('release-uwagi').value.trim();

    if (!codice || !lotto) return;

    const confirmBtn = document.getElementById('release-confirm-btn');
    confirmBtn.disabled    = true;
    confirmBtn.textContent = 'Zapisywanie...';

    try {
        const response = await fetch('/api/matlot-status/release', {
            method:  'POST',
            headers: { 'Content-Type': 'application/json' },
            body:    JSON.stringify({ codice_materiale: codice, lotto, box, uwagi }),
        });
        const data = await response.json();

        if (data.success) {
            closeReleaseModal();
            fetchRecords(true);
        } else {
            alert(`Błąd: ${data.error || 'Nie udało się zaktualizować statusu.'}`);
            confirmBtn.disabled    = false;
            confirmBtn.textContent = 'Zatwierdź dostawę';
        }
    } catch (err) {
        console.error('Error releasing MATLOT row:', err);
        alert('Błąd połączenia — nie udało się zaktualizować statusu.');
        confirmBtn.disabled    = false;
        confirmBtn.textContent = 'Zatwierdź dostawę';
    }
}

// ── Withdrawal modal (S → N) — TASK0 ─────────────────────────────────────────

/**
 * Open the withdrawal confirmation modal (reverting S → N).
 * The "Zatwierdzony" badge is the trigger button.
 */
function openWithdrawModal(btn, codice, lotto, box) {
    _pendingCodice = codice;
    _pendingLotto  = lotto;
    _pendingBox    = box;
    _pendingBtn    = btn;

    document.getElementById('withdraw-modal-codice').textContent = codice;
    document.getElementById('withdraw-modal-lotto').textContent  = lotto;
    document.getElementById('withdraw-modal-label').textContent  = `${codice} / ${lotto}`;
    document.getElementById('withdraw-reason').value = '';

    const confirmBtn = document.getElementById('withdraw-confirm-btn');
    confirmBtn.disabled    = false;
    confirmBtn.textContent = 'Cofnij zatwierdzenie';

    document.getElementById('withdraw-modal').classList.add('active');
    setTimeout(() => document.getElementById('withdraw-reason').focus(), 100);
}

function closeWithdrawModal() {
    document.getElementById('withdraw-modal').classList.remove('active');
    _pendingCodice = '';
    _pendingLotto  = '';
    _pendingBox    = '';
    _pendingBtn    = null;
}

function closeWithdrawModalOnBackdrop(event) {
    if (event.target.id === 'withdraw-modal') closeWithdrawModal();
}

async function submitWithdraw() {
    const codice           = _pendingCodice;
    const lotto            = _pendingLotto;
    const box              = _pendingBox;
    const withdrawal_reason = document.getElementById('withdraw-reason').value.trim();

    if (!codice || !lotto) return;

    const confirmBtn = document.getElementById('withdraw-confirm-btn');
    confirmBtn.disabled    = true;
    confirmBtn.textContent = 'Zapisywanie...';

    try {
        const response = await fetch('/api/matlot-status/withdraw', {
            method:  'POST',
            headers: { 'Content-Type': 'application/json' },
            body:    JSON.stringify({ codice_materiale: codice, lotto, box, withdrawal_reason }),
        });
        const data = await response.json();

        if (data.success) {
            closeWithdrawModal();
            fetchRecords(true);
        } else {
            alert(`Błąd: ${data.error || 'Nie udało się cofnąć zatwierdzenia.'}`);
            confirmBtn.disabled    = false;
            confirmBtn.textContent = 'Cofnij zatwierdzenie';
        }
    } catch (err) {
        console.error('Error withdrawing MATLOT row:', err);
        alert('Błąd połączenia — nie udało się cofnąć zatwierdzenia.');
        confirmBtn.disabled    = false;
        confirmBtn.textContent = 'Cofnij zatwierdzenie';
    }
}

// ── Uwagi edit modal — TASK4 ──────────────────────────────────────────────────

/** Convert dd.mm.yyyy → yyyy-mm-dd for <input type="date"> value. */
function _dmyToIso(dmy) {
    if (!dmy) return '';
    const p = dmy.split('.');
    return p.length === 3 ? `${p[2]}-${p[1].padStart(2,'0')}-${p[0].padStart(2,'0')}` : '';
}

/** Convert dd.mm.yyyy HH:MM → yyyy-mm-dd (strips time component first). */
function _dmyHmToIsoDate(val) {
    if (!val) return '';
    return _dmyToIso(val.split(' ')[0]);
}

/** Toggle status buttons and show/hide released_at row. */
function setUwagiStatus(status) {
    _pendingReleaseStatus = status;
    document.getElementById('uwagi-status-N').classList.toggle('active', status === 'N');
    document.getElementById('uwagi-status-S').classList.toggle('active', status === 'S');
    // released_at only visible when enhanced mode is on AND status is S
    document.getElementById('uwagi-released-at-row').style.display =
        (enhancedEdit && status === 'S') ? '' : 'none';
    if (status === 'N') document.getElementById('uwagi-released-at').value = '';
}

/**
 * Open the batch edit modal (uwagi, status, dates, withdrawal fields).
 */
function openUwagiModal(btn, codice, lotto, box, currentUwagi, releaseStatus, primaVista, releasedAt, withdrawnAt, withdrawalReason) {
    _pendingCodice = codice;
    _pendingLotto  = lotto;
    _pendingBox    = box;
    _pendingBtn    = btn;

    _pendingOriginalStatus = releaseStatus || 'N';

    document.getElementById('uwagi-modal-label').textContent    = `${codice} / ${lotto}`;
    document.getElementById('uwagi-input').value                = currentUwagi || '';
    document.getElementById('uwagi-prima-vista').value          = _dmyToIso(primaVista);
    document.getElementById('uwagi-released-at').value          = _dmyToIso(releasedAt);
    document.getElementById('uwagi-withdrawn-at').value         = _dmyHmToIsoDate(withdrawnAt);
    document.getElementById('uwagi-withdrawal-reason').value    = withdrawalReason || '';

    // Bulk mode indicator
    const bulk = isBulkMode();
    const bulkIndicator = document.getElementById('uwagi-bulk-indicator');
    if (bulkIndicator) {
        bulkIndicator.style.display = bulk ? '' : 'none';
        const countEl = document.getElementById('uwagi-bulk-count');
        if (countEl) countEl.textContent = totalRecords;
    }

    // Show/hide enhanced-only rows and delete button based on current mode
    document.getElementById('uwagi-status-row').style.display          = enhancedEdit ? '' : 'none';
    document.getElementById('uwagi-prima-vista-row').style.display     = enhancedEdit ? '' : 'none';
    document.getElementById('uwagi-withdrawn-at-row').style.display    = enhancedEdit ? '' : 'none';
    document.getElementById('uwagi-withdrawal-reason-row').style.display = enhancedEdit ? '' : 'none';
    document.getElementById('uwagi-delete-btn').style.display          = enhancedEdit ? '' : 'none';

    setUwagiStatus(releaseStatus || 'N');

    const confirmBtn = document.getElementById('uwagi-confirm-btn');
    confirmBtn.disabled    = false;
    confirmBtn.textContent = 'Zapisz';

    const deleteBtn = document.getElementById('uwagi-delete-btn');
    deleteBtn.disabled    = false;
    deleteBtn.textContent = isBulkMode() ? `Usuń wszystkie (${totalRecords})` : 'Usuń';

    document.getElementById('uwagi-modal').classList.add('active');
    setTimeout(() => document.getElementById('uwagi-input').focus(), 100);
}

function closeUwagiModal() {
    document.getElementById('uwagi-modal').classList.remove('active');
    _pendingCodice = '';
    _pendingLotto  = '';
    _pendingBox    = '';
    _pendingBtn    = null;
}

function closeUwagiModalOnBackdrop(event) {
    if (event.target.id === 'uwagi-modal') closeUwagiModal();
}

async function submitUwagi() {
    const codice     = _pendingCodice;
    const lotto      = _pendingLotto;
    const box        = _pendingBox;
    const uwagi            = document.getElementById('uwagi-input').value.trim();
    const primaVista       = document.getElementById('uwagi-prima-vista').value || null;
    const releasedAt       = document.getElementById('uwagi-released-at').value || null;
    const withdrawnAt      = document.getElementById('uwagi-withdrawn-at').value;
    const withdrawalReason = document.getElementById('uwagi-withdrawal-reason').value;
    const statusChanged    = _pendingReleaseStatus !== _pendingOriginalStatus;

    if (!codice || !lotto) return;

    const confirmBtn = document.getElementById('uwagi-confirm-btn');
    confirmBtn.disabled    = true;
    confirmBtn.textContent = 'Zapisywanie...';

    try {
        // Step 1: save the single row (all fields)
        const response = await fetch('/api/matlot-status/uwagi', {
            method:  'POST',
            headers: { 'Content-Type': 'application/json' },
            body:    JSON.stringify({
                codice_materiale:  codice,
                lotto,
                box,
                uwagi,
                release_status:    _pendingReleaseStatus,
                prima_vista:       primaVista,
                released_at:       releasedAt,
                withdrawn_at:      withdrawnAt,
                withdrawal_reason: withdrawalReason,
            }),
        });
        const data = await response.json();

        if (!data.success) {
            alert(`Błąd: ${data.error || 'Nie udało się zapisać.'}`);
            confirmBtn.disabled    = false;
            confirmBtn.textContent = 'Zapisz';
            return;
        }

        // Step 2: bulk mode + status changed → apply status to all other filtered rows
        // The single row already has the new status so it will be skipped by bulk-status.
        if (isBulkMode() && statusChanged) {
            const searchPayload = {};
            Object.entries(searchFilters).forEach(([key, val]) => {
                if (val && val.trim()) searchPayload[key] = val.trim();
            });
            await fetch('/api/matlot-status/bulk-status', {
                method:  'POST',
                headers: { 'Content-Type': 'application/json' },
                body:    JSON.stringify({
                    original_status: _pendingOriginalStatus,
                    new_status:      _pendingReleaseStatus,
                    category:        currentCategory,
                    search:          searchPayload,
                }),
            });
        }

        closeUwagiModal();
        fetchRecords(true);
    } catch (err) {
        console.error('Error updating:', err);
        alert('Błąd połączenia — nie udało się zapisać.');
        confirmBtn.disabled    = false;
        confirmBtn.textContent = 'Zapisz';
    }
}

async function submitDeleteRow() {
    const codice = _pendingCodice;
    const lotto  = _pendingLotto;
    const box    = _pendingBox;
    if (!codice || !lotto) return;

    const deleteBtn = document.getElementById('uwagi-delete-btn');
    deleteBtn.disabled    = true;
    deleteBtn.textContent = 'Usuwanie...';

    try {
        let response;
        if (isBulkMode()) {
            // Bulk delete — all rows matching current filters
            const searchPayload = {};
            Object.entries(searchFilters).forEach(([key, val]) => {
                if (val && val.trim()) searchPayload[key] = val.trim();
            });
            response = await fetch('/api/matlot-status/bulk-delete', {
                method:  'POST',
                headers: { 'Content-Type': 'application/json' },
                body:    JSON.stringify({ category: currentCategory, search: searchPayload }),
            });
        } else {
            // Single row delete
            response = await fetch('/api/matlot-status/delete', {
                method:  'POST',
                headers: { 'Content-Type': 'application/json' },
                body:    JSON.stringify({ codice_materiale: codice, lotto, box }),
            });
        }
        const data = await response.json();

        if (data.success) {
            closeUwagiModal();
            fetchRecords(true);
        } else {
            alert(`Błąd: ${data.error || 'Nie udało się usunąć.'}`);
            deleteBtn.disabled    = false;
            deleteBtn.textContent = isBulkMode() ? `Usuń wszystkie (${totalRecords})` : 'Usuń';
        }
    } catch (err) {
        console.error('Error deleting row:', err);
        alert('Błąd połączenia — nie udało się usunąć.');
        deleteBtn.disabled    = false;
        deleteBtn.textContent = isBulkMode() ? `Usuń wszystkie (${totalRecords})` : 'Usuń';
    }
}

// ── Bulk release modal ────────────────────────────────────────────────────────

/**
 * Enable/disable the "Zatwierdź wybrane" button based on two conditions:
 *   1. At least one column search filter is non-empty
 *   2. Status view is 'Oczekujące' (currentStatus === 'N')
 */
function updateBulkReleaseButton() {
    const btn = document.getElementById('btn-bulk-release');
    if (!btn) return;
    const hasFilters = Object.values(searchFilters).some(v => v && v.trim() !== '');
    btn.disabled = !(currentStatus === 'PILNE' || (hasFilters && currentStatus === 'N'));
}

/**
 * Open the bulk-release confirmation modal.
 * Populates the row count from totalRecords (covers all matching rows,
 * not just the ones already loaded via infinite scroll).
 */
function openBulkReleaseModal() {
    document.getElementById('bulk-release-count').textContent =
        totalRecords === 1 ? '1 partia' : `${totalRecords} partii`;
    document.getElementById('bulk-release-uwagi').value = '';

    const confirmBtn = document.getElementById('bulk-release-confirm-btn');
    confirmBtn.disabled    = false;
    confirmBtn.textContent = 'Zatwierdź wybrane';

    document.getElementById('bulk-release-modal').classList.add('active');
    setTimeout(() => document.getElementById('bulk-release-uwagi').focus(), 100);
}

function closeBulkReleaseModal() {
    document.getElementById('bulk-release-modal').classList.remove('active');
}

function closeBulkReleaseModalOnBackdrop(event) {
    if (event.target.id === 'bulk-release-modal') closeBulkReleaseModal();
}

/**
 * POST /api/matlot-status/bulk-release with active filters + uwagi.
 * Server applies the same filters and releases all matching pending rows.
 */
async function submitBulkRelease() {
    const uwagi = document.getElementById('bulk-release-uwagi').value.trim();

    const confirmBtn = document.getElementById('bulk-release-confirm-btn');
    confirmBtn.disabled    = true;
    confirmBtn.textContent = 'Zatwierdzanie...';

    // Mirror the active search filters to the server
    const searchPayload = {};
    Object.entries(searchFilters).forEach(([key, val]) => {
        if (val && val.trim()) searchPayload[key] = val.trim();
    });

    try {
        const response = await fetch('/api/matlot-status/bulk-release', {
            method:  'POST',
            headers: { 'Content-Type': 'application/json' },
            body:    JSON.stringify({
                uwagi,
                category: currentCategory,
                search:   searchPayload,
            }),
        });
        const data = await response.json();

        if (data.success) {
            closeBulkReleaseModal();
            fetchRecords(true);
        } else {
            alert(`Błąd: ${data.error || 'Nie udało się zatwierdzić partii.'}`);
            confirmBtn.disabled    = false;
            confirmBtn.textContent = 'Zatwierdź wybrane';
        }
    } catch (err) {
        console.error('Error bulk releasing MATLOT rows:', err);
        alert('Błąd połączenia — nie udało się zatwierdzić partii.');
        confirmBtn.disabled    = false;
        confirmBtn.textContent = 'Zatwierdź wybrane';
    }
}

// ── Filters ───────────────────────────────────────────────────────────────────

function updateClearFiltersButton() {
    const clearButton = document.getElementById('btn-clear-filters');
    const hasActiveFilters = Object.values(searchFilters).some(v => v && v.trim() !== '');
    if (clearButton) clearButton.style.display = hasActiveFilters ? 'inline-flex' : 'none';
    updateBulkReleaseButton();
}

function clearAllFilters() {
    document.querySelectorAll('.column-search').forEach(input => { input.value = ''; });
    searchFilters = {};
    applyFilters();
    updateClearFiltersButton();
}

// ── Utilities ─────────────────────────────────────────────────────────────────

function escapeHtml(text) {
    if (!text && text !== 0) return '';
    const div = document.createElement('div');
    div.textContent = String(text);
    return div.innerHTML;
}

function escapeAttr(text) {
    return String(text || '').replace(/\\/g, '\\\\').replace(/'/g, "\\'");
}

function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => { clearTimeout(timeout); func(...args); };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}
