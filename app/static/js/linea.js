/**
 * LINEA - JavaScript for AJAX search, sorting, and filtering
 * Refined Minimal Design System
 */

let currentSort = { field: 'DATA', direction: 'desc' };  // Default: newest to oldest
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
            updateClearFiltersButton();  // Update button visibility
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
        let aVal, bVal;

        // Special handling for DATA column - combine DATA_RAW + ORA_RAW for timestamp sorting
        if (currentSort.field === 'DATA') {
            aVal = createTimestamp(a.DATA_RAW, a.ORA_RAW);
            bVal = createTimestamp(b.DATA_RAW, b.ORA_RAW);
        } else {
            aVal = a[currentSort.field] || '';
            bVal = b[currentSort.field] || '';
        }

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
 * Create timestamp from DATE and TIME for proper sorting
 * Combines NOTCOJAN.DATA & NOTCOJAN.ORA into 14-digit integer: YYYYMMDDHHMMSS
 * @param {string} dataRaw - Raw date from NOTCOJAN.DATA in format "YYYYMMDD" (e.g., "20240210")
 * @param {string} oraRaw - Raw time from NOTCOJAN.ORA in format "HHMMSS" (e.g., "143025")
 * @returns {number} - 14-digit integer timestamp for numerical sorting
 */
function createTimestamp(dataRaw, oraRaw) {
    // Handle empty or null values
    if (!dataRaw) return 0;

    // Convert to strings and remove any non-digit characters
    const dataStr = (dataRaw || '').toString().replace(/\D/g, '');
    const oraStr = (oraRaw || '').toString().replace(/\D/g, '');

    // Ensure exactly 8 digits for date, 6 for time
    const dateDigits = dataStr.padStart(8, '0').substring(0, 8);
    const timeDigits = oraStr.padStart(6, '0').substring(0, 6);

    // Concatenate: YYYYMMDD + HHMMSS = YYYYMMDDHHMMSS (14 digits)
    const timestampString = dateDigits + timeDigits;

    // Convert to integer for numerical sorting
    const timestampInt = parseInt(timestampString, 10);

    // Return 0 for invalid timestamps
    return isNaN(timestampInt) ? 0 : timestampInt;
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
        const hasNC = record.NR_NIEZG && record.NR_NIEZG.trim();

        // Row is clickable if it has either riparazione code or NC number
        const isClickable = hasRiparazione || hasNC;
        const rowClass = isClickable ? 'stagger-row clickable-row' : 'stagger-row';

        // Store both codice_riparazione and nr_niezg as data attributes
        let dataAttrs = '';
        if (hasRiparazione) {
            dataAttrs += `data-codice-riparazione="${escapeHtml(record.CODICE_RIPARAZIONE)}" `;
        }
        if (hasNC) {
            dataAttrs += `data-nr-niezg="${escapeHtml(record.NR_NIEZG)}" `;
        }

        // Map TYP_UWAGI to Polish labels
        const typLabel = mapTypUwagi(record.TYP_UWAGI || '—');

        // Add yellow circle indicator for closed NC without AC records
        const nrNiezgDisplay = record.MISSING_AC
            ? `${escapeHtml(record.NR_NIEZG || '—')}<span style="color: #ffc107; margin-left: 0.25rem;">●</span>`
            : escapeHtml(record.NR_NIEZG || '—');

        return `
            <tr class="${rowClass}" style="animation-delay: ${delay}s" ${dataAttrs}>
                <td>${escapeHtml(record.COMM || '—')}</td>
                <td>${escapeHtml(record.DATA || '—')}</td>
                <td>${escapeHtml(record.GODZ || '—')}</td>
                <td>${nrNiezgDisplay}</td>
                <td>${typLabel}</td>
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
 * Map TYP_UWAGI codes to Polish labels
 */
function mapTypUwagi(typ) {
    const mapping = {
        'NC': 'Otwarcie',
        'AC': 'Działania',
        'AV': 'Uwaga',
        'OK': 'Zamknięcie'
    };
    return mapping[typ] || typ;
}

/**
 * Update clear filters button visibility
 */
function updateClearFiltersButton() {
    const clearButton = document.getElementById('btn-clear-filters');
    const hasActiveFilters = Object.values(searchFilters).some(value => value && value.trim() !== '');

    if (hasActiveFilters) {
        clearButton.style.display = 'inline-flex';
    } else {
        clearButton.style.display = 'none';
    }
}

/**
 * Clear all column search filters
 */
function clearAllFilters() {
    // Clear all search input fields
    document.querySelectorAll('.column-search').forEach(input => {
        input.value = '';
    });

    // Reset search filters object
    searchFilters = {};

    // Re-apply filters and sort (this will show all records)
    applyFiltersAndSort();

    // Hide the clear button
    updateClearFiltersButton();
}

/**
 * Handle clickable row click - open modal with repair details and/or blocked parts info
 */
async function handleRowClick(event) {
    const row = event.currentTarget;
    const codiceRiparazione = row.dataset.codiceRiparazione;
    const nrNiezg = row.dataset.nrNiezg;

    if (codiceRiparazione || nrNiezg) {
        await openDetailsModal(codiceRiparazione, nrNiezg);
    }
}

/**
 * Open details modal - shows repair details and/or blocked parts info
 */
async function openDetailsModal(codiceRiparazione, nrNiezg) {
    const modal = document.getElementById('riparaz-modal');
    const modalCodice = document.getElementById('modal-codice');
    const modalBody = document.getElementById('modal-body');

    // Set modal title
    let title = '';
    if (codiceRiparazione && nrNiezg) {
        title = `${nrNiezg} / Nr naprawy: ${codiceRiparazione}`;
    } else if (codiceRiparazione) {
        title = codiceRiparazione;
    } else if (nrNiezg) {
        title = nrNiezg;
    }

    // Show modal with loading state
    modalCodice.textContent = title;
    modalBody.innerHTML = '<div class="modal-loading">Ładowanie...</div>';
    modal.classList.add('active');

    // Disable body scroll
    document.body.style.overflow = 'hidden';

    try {
        let contentHTML = '';

        // Fetch blocked parts quantity if NC number exists
        if (nrNiezg) {
            try {
                const blockedResponse = await fetch(`/linea/api/blocked-parts/${encodeURIComponent(nrNiezg)}`);
                const blockedData = await blockedResponse.json();

                if (blockedData.success) {
                    // Build NC list HTML as table
                    let ncListHTML = '';
                    if (blockedData.related_ncs && blockedData.related_ncs.length > 0) {
                        ncListHTML = `
                            <h4 style="margin: 1rem 0 0.5rem 0; font-size: 0.875rem; font-weight: 500; color: var(--color-ink-muted);">
                                Wszystkie detale zablokowane w tym zamówieniu produkcyjnym:
                            </h4>
                            <div class="nc-list">
                                <table class="nc-table">
                                    <thead>
                                        <tr>
                                            <th>Nr NC</th>
                                            <th>Data</th>
                                            <th>Opis niezgodności</th>
                                            <th style="text-align: right;">Ilość</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                        `;

                        blockedData.related_ncs.forEach(nc => {
                            const isCurrent = nc.nr_niezg === nrNiezg;
                            const currentClass = isCurrent ? ' current' : '';
                            ncListHTML += `
                                <tr class="${currentClass}">
                                    <td class="nc-number">${escapeHtml(nc.nr_niezg)}</td>
                                    <td class="nc-data">${escapeHtml(nc.data || '—')}</td>
                                    <td class="nc-uwaga" title="${escapeHtml(nc.uwaga || '')}">${escapeHtml(nc.uwaga || '—')}</td>
                                    <td class="nc-qty">${nc.blocked_qty} szt.</td>
                                </tr>
                            `;
                        });

                        ncListHTML += `
                                    </tbody>
                                </table>
                            </div>
                        `;

                        // Add total
                        ncListHTML += `
                            <div class="nc-total">
                                <span class="nc-total-label">Razem zablokowanych:</span>
                                <span class="nc-total-value">${blockedData.total_qty} szt.</span>
                            </div>
                        `;
                    }

                    contentHTML += `
                        <div style="padding: 1rem; background: #fff3cd; border: 1px solid #ffc107; border-radius: 4px; margin-bottom: 1.5rem;">
                            <h3 style="margin: 0 0 0.5rem 0; font-size: 1rem; color: #856404;">
                                🔒 Ilość zablokowanych części
                            </h3>
                            <p style="margin: 0 0 0.5rem 0; font-size: 1.5rem; font-weight: 600; color: #856404;">
                                ${blockedData.blocked_qty} szt.
                            </p>
                            ${ncListHTML}
                        </div>
                    `;
                }
            } catch (error) {
                console.error('Error fetching blocked parts:', error);
            }
        }

        // Fetch repair details if riparazione code exists
        if (codiceRiparazione) {
            try {
                const riparazResponse = await fetch(`/linea/api/riparaz/${encodeURIComponent(codiceRiparazione)}`);
                const riparazData = await riparazResponse.json();

                if (riparazData.success && riparazData.records.length > 0) {
                    contentHTML += '<h3 style="margin: 0 0 1rem 0; font-size: 1rem; color: var(--color-ink);">Szczegóły naprawy</h3>';
                    contentHTML += renderRiparazTableHTML(riparazData.records);
                } else if (codiceRiparazione && !nrNiezg) {
                    // Only show "no data" if there's no blocked parts info
                    contentHTML += '<div class="modal-loading">Brak danych dla tego kodu naprawy.</div>';
                }
            } catch (error) {
                console.error('Error fetching riparaz:', error);
                if (!nrNiezg) {
                    contentHTML += '<div class="modal-loading" style="color: var(--color-error);">Błąd ładowania danych naprawy</div>';
                }
            }
        }

        // Update modal body
        if (contentHTML) {
            modalBody.innerHTML = contentHTML;
        } else {
            modalBody.innerHTML = '<div class="modal-loading">Brak danych do wyświetlenia.</div>';
        }
    } catch (error) {
        console.error('Error in modal:', error);
        modalBody.innerHTML = '<div class="modal-loading" style="color: var(--color-error);">Błąd ładowania danych</div>';
    }
}

/**
 * Calculate repair time duration
 * MOSYS format: date = "YYYYMMDD", time = "HHMM" (4 digits, seconds assumed as 00)
 * @param {string} dataInizio - Start date in YYYYMMDD format (e.g., "20240110")
 * @param {string} oraInizio - Start time in HHMM format (e.g., "0800")
 * @param {string} dataFine - End date in YYYYMMDD format (e.g., "20240111")
 * @param {string} oraFine - End time in HHMM format (e.g., "1430")
 * @returns {string} - Formatted duration "Xh Ym" or empty string
 */
function calculateRepairTime(dataInizio, oraInizio, dataFine, oraFine) {
    console.log('📅 calculateRepairTime() called with:', { dataInizio, oraInizio, dataFine, oraFine });

    if (!dataFine || !dataInizio) {
        console.log('⚠️ Missing date data, returning empty string');
        return '';
    }

    try {
        // Parse YYYYMMDD format
        const startYear = parseInt(dataInizio.substring(0, 4), 10);
        const startMonth = parseInt(dataInizio.substring(4, 6), 10) - 1; // Month is 0-indexed
        const startDay = parseInt(dataInizio.substring(6, 8), 10);

        const endYear = parseInt(dataFine.substring(0, 4), 10);
        const endMonth = parseInt(dataFine.substring(4, 6), 10) - 1;
        const endDay = parseInt(dataFine.substring(6, 8), 10);

        console.log('📅 Parsed dates:', {
            start: { year: startYear, month: startMonth, day: startDay },
            end: { year: endYear, month: endMonth, day: endDay }
        });

        // Parse time format - supports both HHMM (4 digits) and HHMMSS (6 digits)
        const startTimeStr = (oraInizio || '0000').toString().padStart(4, '0');
        const startHour = parseInt(startTimeStr.substring(0, 2), 10);
        const startMin = parseInt(startTimeStr.substring(2, 4), 10);
        const startSec = startTimeStr.length >= 6 ? parseInt(startTimeStr.substring(4, 6), 10) : 0;

        const endTimeStr = (oraFine || '0000').toString().padStart(4, '0');
        const endHour = parseInt(endTimeStr.substring(0, 2), 10);
        const endMin = parseInt(endTimeStr.substring(2, 4), 10);
        const endSec = endTimeStr.length >= 6 ? parseInt(endTimeStr.substring(4, 6), 10) : 0;

        console.log('⏰ Parsed times:', {
            start: { hour: startHour, min: startMin, sec: startSec },
            end: { hour: endHour, min: endMin, sec: endSec }
        });

        // Create Date objects
        const startDate = new Date(startYear, startMonth, startDay, startHour, startMin, startSec);
        const endDate = new Date(endYear, endMonth, endDay, endHour, endMin, endSec);

        console.log('📆 Date objects created:', {
            startDate: startDate.toISOString(),
            endDate: endDate.toISOString(),
            startValid: !isNaN(startDate.getTime()),
            endValid: !isNaN(endDate.getTime())
        });

        // Validate dates
        if (isNaN(startDate.getTime()) || isNaN(endDate.getTime())) {
            console.log('❌ Invalid date objects, returning empty string');
            return '';
        }

        // Calculate difference in milliseconds
        const diffMs = endDate - startDate;

        console.log('⏱️ Time difference:', {
            diffMs: diffMs,
            isNegative: diffMs < 0
        });

        if (diffMs < 0) {
            console.log('❌ Negative time difference, returning empty string');
            return '';
        }

        // Convert to hours and minutes
        const totalMinutes = Math.floor(diffMs / 60000);
        const hours = Math.floor(totalMinutes / 60);
        const minutes = totalMinutes % 60;

        const result = `${hours}h ${minutes}m`;
        console.log('✅ Repair time calculated:', result);

        return result;
    } catch (error) {
        console.error('❌ Error calculating repair time:', error);
        return '';
    }
}

/**
 * Render repair details as list HTML (returns string instead of updating DOM)
 * MOSYS repair table has only one record per repair linked to NC
 */
function renderRiparazTableHTML(records) {
    if (!records || records.length === 0) {
        return '<div class="modal-loading">Brak danych riparazione.</div>';
    }

    // Take the first record (should be only one)
    const record = records[0];

    // DEBUG: Log the record to see what we're getting
    console.log('🔍 Riparazione record:', record);
    console.log('🔍 RAW fields:', {
        DATA_INIZIO_RAW: record.DATA_INIZIO_RAW,
        ORA_INIZIO_RAW: record.ORA_INIZIO_RAW,
        DATA_FINE_RAW: record.DATA_FINE_RAW,
        ORA_FINE_RAW: record.ORA_FINE_RAW
    });

    // Calculate repair time using RAW MOSYS format values
    const repairTime = calculateRepairTime(
        record.DATA_INIZIO_RAW,
        record.ORA_INIZIO_RAW,
        record.DATA_FINE_RAW,
        record.ORA_FINE_RAW
    );

    console.log('🔍 Calculated repair time:', repairTime);

    // Determine status badge
    let statusBadge = '';
    if (record.DATA_COLLAUDO && record.DATA_COLLAUDO.trim()) {
        statusBadge = '<span class="status-badge completed">Zakończona</span>';
    } else {
        statusBadge = '<span class="status-badge in-progress">W trakcie</span>';
    }

    return `
        <div class="detail-list">
            <div class="detail-item">
                <div class="detail-label">Nr formy:</div>
                <div class="detail-value">${escapeHtml(record.CODICE_STAMPO || '—')}</div>
            </div>
            <div class="detail-item">
                <div class="detail-label">Zamówienie:</div>
                <div class="detail-value">${escapeHtml(record.COMMESSA || '—')}</div>
            </div>
            <div class="detail-item">
                <div class="detail-label">Status:</div>
                <div class="detail-value">${statusBadge}</div>
            </div>
            <div class="detail-item">
                <div class="detail-label">Data rozpoczęcia:</div>
                <div class="detail-value">${escapeHtml(record.DATA_INIZIO || '—')}</div>
            </div>
            <div class="detail-item">
                <div class="detail-label">Godzina:</div>
                <div class="detail-value">${escapeHtml(record.ORA_INIZIO || '—')}</div>
            </div>
            <div class="detail-item">
                <div class="detail-label">Data Zakończenia:</div>
                <div class="detail-value">${escapeHtml(record.DATA_FINE || '—')}</div>
            </div>
            <div class="detail-item">
                <div class="detail-label">Godzina:</div>
                <div class="detail-value">${escapeHtml(record.ORA_FINE || '—')}</div>
            </div>
            ${repairTime ? `
            <div class="detail-item">
                <div class="detail-label">Czas naprawy:</div>
                <div class="detail-value" style="font-weight: 600; color: var(--color-accent);">${repairTime}</div>
            </div>
            ` : ''}
            <div class="detail-item">
                <div class="detail-label">Nr operatora:</div>
                <div class="detail-value">${escapeHtml(record.OPER_INIZIO || '—')}</div>
            </div>
            <div class="detail-item">
                <div class="detail-label">Data Kontroli naprawy:</div>
                <div class="detail-value">${escapeHtml(record.DATA_COLLAUDO || '—')}</div>
            </div>
            <div class="detail-item">
                <div class="detail-label">Uwagi:</div>
                <div class="detail-value">${escapeHtml(record.UWAGA || '—')}</div>
            </div>
        </div>
    `;
}

/**
 * Close details modal
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
