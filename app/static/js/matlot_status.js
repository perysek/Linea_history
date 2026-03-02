/**
 * matlot_status.js — MATLOT incoming raw material inspection
 * Follows the same infinite-scroll / sort / search pattern as linea.js
 */

let currentSort     = { field: 'CODICE_MATERIALE', direction: 'asc' };
let searchFilters   = {};
let currentCategory = '';   // '', 'surowce', or 'inserty'
let currentStatus   = 'N';  // 'N' (pending), 'S' (released), or 'ALL'
let allRecords      = [];
let isLoading       = false;
let currentAbortController = null;

// Pagination state
let currentOffset = 0;
let totalRecords  = 0;
const RECORDS_PER_PAGE = 100;
let hasMoreRecords = false;

// Release modal state
let _pendingCodice = '';
let _pendingLotto  = '';
let _pendingBtn    = null;

// Initialize on page load — sync from MOSYS first, then display data
document.addEventListener('DOMContentLoaded', () => {
    syncFromMosys().then(() => fetchRecords());

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

    // Close modal on Escape
    document.addEventListener('keydown', e => {
        if (e.key === 'Escape') closeReleaseModal();
    });
});

/**
 * Set active material category (Surowce / Inserty) and refetch
 */
function setCategory(cat) {
    currentCategory = cat;
    document.getElementById('cat-all').classList.toggle('active',     cat === '');
    document.getElementById('cat-surowce').classList.toggle('active', cat === 'surowce');
    document.getElementById('cat-inserty').classList.toggle('active', cat === 'inserty');
    fetchRecords(true);
}

/**
 * Set active status view (Oczekujące / Zatwierdzone / Wszystkie) and refetch
 */
function setStatus(stat) {
    currentStatus = stat;
    document.getElementById('stat-pending').classList.toggle('active',  stat === 'N');
    document.getElementById('stat-released').classList.toggle('active', stat === 'S');
    document.getElementById('stat-all').classList.toggle('active',      stat === 'ALL');
    fetchRecords(true);
}

/**
 * POST /api/matlot-refresh — sync MOSYS → SQLite, update timestamp label.
 * Always resolves (never rejects) so callers can safely chain .then().
 */
async function syncFromMosys() {
    const btn  = document.getElementById('btn-refresh-mosys');
    const icon = document.getElementById('refresh-spin-icon');
    const ts   = document.getElementById('sync-timestamp');

    // Start spin animation
    if (btn)  btn.disabled = true;
    if (icon) icon.style.animation = 'spin 0.8s linear infinite';

    // Show syncing state in tbody only on initial load (no records yet)
    const tbody = document.getElementById('matlot-tbody');
    if (tbody && allRecords.length === 0) {
        tbody.innerHTML = '<tr><td colspan="8" style="text-align: center; padding: 2rem; color: var(--color-ink-muted);">Synchronizacja z MOSYS...</td></tr>';
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
    if (currentCategory) params.append('category', currentCategory);

    Object.entries(searchFilters).forEach(([key, value]) => {
        if (value) params.append(`search_${key}`, value);
    });

    params.append('limit', RECORDS_PER_PAGE);
    params.append('offset', currentOffset);

    if (currentOffset === 0) {
        tbody.innerHTML = '<tr><td colspan="8" style="text-align: center; padding: 2rem; color: var(--color-ink-muted);">Ładowanie...</td></tr>';
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
        } else {
            tbody.innerHTML = '<tr><td colspan="8" style="text-align: center; padding: 2rem; color: var(--color-error);">Błąd ładowania danych z MOSYS</td></tr>';
        }
    } catch (error) {
        if (error.name === 'AbortError') {
            // cancelled — new request in progress
        } else {
            console.error('Error fetching MATLOT records:', error);
            tbody.innerHTML = `
                <tr>
                    <td colspan="8" style="text-align: center; padding: 3rem; color: var(--color-ink-muted);">
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
 * Build HTML string for a single MATLOT record row (8 columns)
 */
function buildRowHtml(record) {
    const isReleased = record.release_status === 'S';

    const rowClass = isReleased
        ? 'stagger-row row-released'
        : record.is_past_due
            ? 'stagger-row row-past-due'
            : record.giorni >= 2
                ? 'stagger-row row-warning'
                : 'stagger-row';

    const giorniLabel = record.giorni === 0
        ? 'dziś'
        : record.giorni === 1
            ? '1 dzień'
            : `${record.giorni} dni`;

    const fmt = n => Number(n).toLocaleString('pl-PL').replace(/,/g, ' ');

    const actionCell = isReleased
        ? `<span class="badge-released">
               <svg width="11" height="11" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                   <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M5 13l4 4L19 7" />
               </svg>
               Zatwierdzony
           </span>`
        : `<button class="btn-release"
                   onclick="openReleaseModal(this, '${escapeAttr(record.codice_materiale)}', '${escapeAttr(record.lotto)}')">
               <svg width="11" height="11" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                   <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M5 13l4 4L19 7" />
               </svg>
               Zatwierdź certyfikat
           </button>`;

    return `
        <tr class="${rowClass}"
            data-codice="${escapeAttr(record.codice_materiale)}"
            data-lotto="${escapeAttr(record.lotto)}">
            <td style="font-weight: 500;">${escapeHtml(record.codice_materiale)}</td>
            <td>${escapeHtml(record.lotto)}</td>
            <td style="text-align: right; padding-right: 1.5rem; font-weight: 500;">${fmt(record.giacenza_lotto)}</td>
            <td style="font-size: 0.75rem; white-space: nowrap;">${escapeHtml(record.box)}</td>
            <td style="font-size: 0.75rem; white-space: nowrap;">${escapeHtml(record.prima_vista)}</td>
            <td style="font-size: 0.75rem; white-space: nowrap; color: ${isReleased ? '#15803d' : 'var(--color-ink-muted)'};">
                ${escapeHtml(record.released_at || '—')}
            </td>
            <td style="text-align: center;">
                ${isReleased ? '—' : `<span class="badge-giorni">${escapeHtml(giorniLabel)}</span>`}
            </td>
            <td style="text-align: center;">
                ${actionCell}
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
                <td colspan="8" style="text-align: center; padding: 3rem; color: var(--color-ink-muted);">
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
 * Show/hide past-due pill (only meaningful in pending view)
 */
function updatePastDuePill(pastDueCount) {
    const pill = document.getElementById('past-due-pill');
    if (!pill) return;
    if (pastDueCount > 0 && currentStatus !== 'S') {
        pill.textContent   = `${pastDueCount} po terminie`;
        pill.style.display = '';
    } else {
        pill.style.display = 'none';
    }
}

// ── Release modal ─────────────────────────────────────────────────────────────

/**
 * Open the release confirmation modal for a given batch
 */
function openReleaseModal(btn, codice, lotto) {
    _pendingCodice = codice;
    _pendingLotto  = lotto;
    _pendingBtn    = btn;

    document.getElementById('release-modal-codice').textContent = codice;
    document.getElementById('release-modal-lotto').textContent  = lotto;
    document.getElementById('release-modal-label').textContent  = `${codice} / ${lotto}`;
    document.getElementById('release-uwagi').value = '';

    const confirmBtn = document.getElementById('release-confirm-btn');
    confirmBtn.disabled    = false;
    confirmBtn.textContent = 'Zatwierdź certyfikat';

    document.getElementById('release-modal').classList.add('active');
    setTimeout(() => document.getElementById('release-uwagi').focus(), 100);
}

/**
 * Close the release modal without action
 */
function closeReleaseModal() {
    document.getElementById('release-modal').classList.remove('active');
    _pendingCodice = '';
    _pendingLotto  = '';
    _pendingBtn    = null;
}

/**
 * Close modal when backdrop is clicked
 */
function closeReleaseModalOnBackdrop(event) {
    if (event.target.id === 'release-modal') closeReleaseModal();
}

/**
 * Submit the release: POST to /api/matlot-status/release with uwagi
 */
async function submitRelease() {
    const codice = _pendingCodice;
    const lotto  = _pendingLotto;
    const uwagi  = document.getElementById('release-uwagi').value.trim();

    if (!codice || !lotto) return;

    const confirmBtn = document.getElementById('release-confirm-btn');
    confirmBtn.disabled    = true;
    confirmBtn.textContent = 'Zapisywanie...';

    try {
        const response = await fetch('/api/matlot-status/release', {
            method:  'POST',
            headers: { 'Content-Type': 'application/json' },
            body:    JSON.stringify({ codice_materiale: codice, lotto: lotto, uwagi: uwagi }),
        });
        const data = await response.json();

        if (data.success) {
            closeReleaseModal();
            fetchRecords(true);
        } else {
            alert(`Błąd: ${data.error || 'Nie udało się zaktualizować statusu.'}`);
            confirmBtn.disabled    = false;
            confirmBtn.textContent = 'Zatwierdź certyfikat';
        }
    } catch (err) {
        console.error('Error releasing MATLOT row:', err);
        alert('Błąd połączenia — nie udało się zaktualizować statusu.');
        confirmBtn.disabled    = false;
        confirmBtn.textContent = 'Zatwierdź certyfikat';
    }
}

/**
 * Update clear filters button visibility
 */
function updateClearFiltersButton() {
    const clearButton = document.getElementById('btn-clear-filters');
    const hasActiveFilters = Object.values(searchFilters).some(v => v && v.trim() !== '');
    if (clearButton) clearButton.style.display = hasActiveFilters ? 'inline-flex' : 'none';
}

/**
 * Clear all column search filters
 */
function clearAllFilters() {
    document.querySelectorAll('.column-search').forEach(input => { input.value = ''; });
    searchFilters = {};
    applyFilters();
    updateClearFiltersButton();
}

/**
 * Escape HTML to prevent XSS
 */
function escapeHtml(text) {
    if (!text && text !== 0) return '';
    const div = document.createElement('div');
    div.textContent = String(text);
    return div.innerHTML;
}

function escapeAttr(text) {
    return String(text || '').replace(/\\/g, '\\\\').replace(/'/g, "\\'");
}

/**
 * Debounce helper
 */
function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => { clearTimeout(timeout); func(...args); };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}
