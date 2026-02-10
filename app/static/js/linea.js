/**
 * LINEA - JavaScript for AJAX search, sorting, and filtering
 * Refined Minimal Design System
 */

let currentSort = { field: 'DATA', direction: 'desc' };
let searchFilters = {};
let allRecords = [];  // Store all fetched records for client-side sorting

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
    // Initial load
    fetchRecords();

    // Setup column search inputs with instant client-side filtering
    document.querySelectorAll('.column-search').forEach(input => {
        input.addEventListener('input', debounce(() => {
            searchFilters[input.dataset.column] = input.value.trim();
            applyFiltersAndSort();  // Client-side filtering
        }, 300));  // Reduced debounce for faster response
    });
});

/**
 * Fetch records via AJAX (only called when filters/date range change)
 */
async function fetchRecords() {
    const tbody = document.getElementById('linea-tbody');
    const params = new URLSearchParams();

    // Add current URL params (date range)
    const urlParams = new URLSearchParams(window.location.search);
    if (urlParams.has('days')) {
        params.append('days', urlParams.get('days'));
    }
    if (urlParams.has('date_from')) {
        params.append('date_from', urlParams.get('date_from'));
    }
    if (urlParams.has('date_to')) {
        params.append('date_to', urlParams.get('date_to'));
    }

    // Don't send search filters or sort params - all done client-side now

    // Show loading state
    tbody.innerHTML = '<tr><td colspan="9" style="text-align: center; padding: 2rem; color: var(--color-ink-muted);">Ładowanie...</td></tr>';

    try {
        const response = await fetch(`/linea/api/search?${params}`);
        const data = await response.json();

        if (data.success) {
            allRecords = data.records;  // Store all records
            applyFiltersAndSort();  // Apply filters and sort client-side
        } else {
            tbody.innerHTML = '<tr><td colspan="9" style="text-align: center; padding: 2rem; color: var(--color-error);">Błąd ładowania danych</td></tr>';
        }
    } catch (error) {
        console.error('Error fetching records:', error);
        tbody.innerHTML = '<tr><td colspan="9" style="text-align: center; padding: 2rem; color: var(--color-error);">Błąd połączenia z serwerem</td></tr>';
    }
}

/**
 * Apply filters and sort client-side
 */
function applyFiltersAndSort() {
    // Start with all records
    let filtered = allRecords;

    // Apply search filters
    Object.entries(searchFilters).forEach(([column, searchValue]) => {
        if (searchValue) {
            const lowerSearch = searchValue.toLowerCase();
            filtered = filtered.filter(record => {
                const fieldValue = (record[column] || '').toString().toLowerCase();
                return fieldValue.includes(lowerSearch);
            });
        }
    });

    // Sort the filtered results
    const sorted = [...filtered].sort((a, b) => {
        const aVal = a[currentSort.field] || '';
        const bVal = b[currentSort.field] || '';

        let comparison = 0;
        if (aVal < bVal) comparison = -1;
        if (aVal > bVal) comparison = 1;

        return currentSort.direction === 'asc' ? comparison : -comparison;
    });

    // Render
    renderRecords(sorted);
    updateCount(sorted.length);
}

/**
 * Render records in table
 */
function renderRecords(records) {
    const tbody = document.getElementById('linea-tbody');

    if (records.length === 0) {
        tbody.innerHTML = '<tr><td colspan="9" style="text-align: center; padding: 2rem; color: var(--color-ink-muted);">Brak rekordów spełniających kryteria</td></tr>';
        return;
    }

    tbody.innerHTML = records.map((record, index) => {
        const delay = Math.min(index * 0.03, 0.3);
        const hasRiparazione = record.CODICE_RIPARAZIONE && record.CODICE_RIPARAZIONE.trim();
        const rowClass = hasRiparazione ? 'stagger-row clickable-row' : 'stagger-row';
        const dataAttr = hasRiparazione ? `data-codice-riparazione="${escapeHtml(record.CODICE_RIPARAZIONE)}"` : '';

        return `
            <tr class="${rowClass}" style="animation-delay: ${delay}s" ${dataAttr}>
                <td>${escapeHtml(record.COMM || '—')}</td>
                <td>${escapeHtml(record.DATA || '—')}</td>
                <td>${escapeHtml(record.GODZ || '—')}</td>
                <td>${escapeHtml(record.NR_NIEZG || '—')}</td>
                <td>${escapeHtml(record.TYP_UWAGI || '—')}</td>
                <td>${escapeHtml(record.UWAGA || '—')}</td>
                <td>${escapeHtml(record.MASZYNA || '—')}</td>
                <td>${escapeHtml(record.KOD_DETALU || '—')}</td>
                <td>${escapeHtml(record.NR_FORMY || '—')}</td>
            </tr>
        `;
    }).join('');

    // Add click handlers to clickable rows
    document.querySelectorAll('.clickable-row').forEach(row => {
        row.addEventListener('click', handleRowClick);
    });
}

/**
 * Sort table by column
 */
function sortTable(field) {
    if (currentSort.field === field) {
        // Toggle direction if same field
        currentSort.direction = currentSort.direction === 'asc' ? 'desc' : 'asc';
    } else {
        // New field, default to ascending
        currentSort.field = field;
        currentSort.direction = 'asc';
    }

    // Update sort icon visual state
    document.querySelectorAll('.refined-table th').forEach(th => {
        th.classList.remove('sorted');
    });
    event.target.closest('th').classList.add('sorted');

    // Re-apply filters and sort
    applyFiltersAndSort();
}

/**
 * Update record count display
 */
function updateCount(total) {
    document.getElementById('visible-count').textContent = total;
    document.getElementById('total-count').textContent = total;
}

/**
 * Escape HTML to prevent XSS
 */
function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

/**
 * Handle clickable row click - open repair details modal
 */
function handleRowClick(event) {
    const row = event.currentTarget;
    const codiceRiparazione = row.dataset.codiceRiparazione;

    if (codiceRiparazione) {
        openRiparazModal(codiceRiparazione);
    }
}

/**
 * Open repair details modal
 */
async function openRiparazModal(codiceRiparazione) {
    const modal = document.getElementById('riparaz-modal');
    const modalCodice = document.getElementById('modal-codice');
    const modalBody = document.getElementById('modal-body');

    // Show modal with loading state
    modalCodice.textContent = codiceRiparazione;
    modalBody.innerHTML = '<div class="modal-loading">Ładowanie...</div>';
    modal.classList.add('active');

    // Disable body scroll
    document.body.style.overflow = 'hidden';

    try {
        const response = await fetch(`/linea/api/riparaz/${encodeURIComponent(codiceRiparazione)}`);
        const data = await response.json();

        if (data.success && data.records.length > 0) {
            renderRiparazTable(data.records);
        } else {
            modalBody.innerHTML = '<div class="modal-loading">Brak danych dla tego kodu riparazione.</div>';
        }
    } catch (error) {
        console.error('Error fetching riparaz:', error);
        modalBody.innerHTML = '<div class="modal-loading" style="color: var(--color-error);">Błąd ładowania danych</div>';
    }
}

/**
 * Render repair details table in modal
 */
function renderRiparazTable(records) {
    const modalBody = document.getElementById('modal-body');

    const tableHTML = `
        <table class="modal-table">
            <thead>
                <tr>
                    <th>Codice Stampo</th>
                    <th>Commessa</th>
                    <th>Data Inizio</th>
                    <th>Ora</th>
                    <th>Operatore</th>
                    <th>Stato</th>
                    <th>Data Fine</th>
                    <th>Data Collaudo</th>
                    <th>Uwaga</th>
                </tr>
            </thead>
            <tbody>
                ${records.map(record => `
                    <tr>
                        <td>${escapeHtml(record.CODICE_STAMPO || '—')}</td>
                        <td>${escapeHtml(record.COMMESSA || '—')}</td>
                        <td>${escapeHtml(record.DATA_INIZIO || '—')}</td>
                        <td>${escapeHtml(record.ORA_INIZIO || '—')}</td>
                        <td>${escapeHtml(record.OPER_INIZIO || '—')}</td>
                        <td>${escapeHtml(record.STATO_RIPARAZIONE || '—')}</td>
                        <td>${escapeHtml(record.DATA_FINE || '—')}</td>
                        <td>${escapeHtml(record.DATA_COLLAUDO || '—')}</td>
                        <td style="max-width: 300px; white-space: normal; word-wrap: break-word;">${escapeHtml(record.UWAGA || '—')}</td>
                    </tr>
                `).join('')}
            </tbody>
        </table>
    `;

    modalBody.innerHTML = tableHTML;
}

/**
 * Close repair details modal
 */
function closeRiparazModal() {
    const modal = document.getElementById('riparaz-modal');
    modal.classList.remove('active');

    // Re-enable body scroll
    document.body.style.overflow = '';
}

/**
 * Close modal when clicking on backdrop
 */
function closeModalOnBackdrop(event) {
    if (event.target.id === 'riparaz-modal') {
        closeRiparazModal();
    }
}

/**
 * Debounce helper - delays execution until after wait period
 * @param {Function} func - Function to debounce
 * @param {number} wait - Milliseconds to wait
 */
function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

// Close modal on Escape key
document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') {
        closeRiparazModal();
    }
});
