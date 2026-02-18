/**
 * Wykaz zablokowanych - AJAX infinite scroll
 * Matches linea.js pagination pattern.
 */

let currentSort = { field: 'KOD_DETALU', direction: 'asc' };
let searchFilters = {};
let allParts = [];
let isLoading = false;
let currentAbortController = null;

// Pagination state
let currentOffset = 0;
let totalRecords = 0;
const PARTS_PER_PAGE = 100;
let hasMoreRecords = false;

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
    fetchParts();

    // Infinite scroll on table body
    const tbodyScroll = document.querySelector('.tbody-scroll');
    if (tbodyScroll) {
        tbodyScroll.addEventListener('scroll', () => {
            if (isLoading || !hasMoreRecords) return;
            const { scrollTop, scrollHeight, clientHeight } = tbodyScroll;
            if (scrollTop + clientHeight >= scrollHeight - 100) {
                currentOffset += PARTS_PER_PAGE;
                fetchParts(false);
            }
        });
    }

    // Column search inputs with debounce
    document.querySelectorAll('.column-search').forEach(input => {
        input.addEventListener('input', debounce(() => {
            searchFilters[input.dataset.column] = input.value.trim();
            applyFilters();
            updateClearFiltersButton();
        }, 300));
    });
});

/**
 * Fetch blocked parts from API with current sort, filters and offset.
 */
async function fetchParts(resetOffset = true) {
    if (currentAbortController) {
        currentAbortController.abort();
    }

    if (resetOffset) {
        currentOffset = 0;
        allParts = [];
    }

    currentAbortController = new AbortController();
    isLoading = true;

    // Show loading indicator only for pagination loads (not initial/reset)
    const loadingIndicator = document.getElementById('scroll-loading-indicator');
    if (loadingIndicator && !resetOffset) {
        loadingIndicator.style.display = 'inline-flex';
    }

    const tbody = document.getElementById('blocked-tbody');
    if (resetOffset) {
        tbody.innerHTML = '<tr><td colspan="7" style="text-align: center; padding: 2rem; color: var(--color-ink-muted);">Ładowanie...</td></tr>';
    }

    const params = new URLSearchParams();
    params.append('sort', currentSort.field);
    params.append('dir', currentSort.direction);
    params.append('limit', PARTS_PER_PAGE);
    params.append('offset', currentOffset);

    Object.entries(searchFilters).forEach(([key, value]) => {
        if (value) params.append(`search_${key}`, value);
    });

    try {
        const response = await fetch(`/api/wykaz-zablokowanych?${params}`, {
            signal: currentAbortController.signal
        });
        const data = await response.json();

        if (data.success) {
            totalRecords = data.pagination.total;
            currentOffset = data.pagination.offset;
            hasMoreRecords = data.pagination.has_more;

            if (resetOffset) {
                allParts = data.parts;
            } else {
                allParts = allParts.concat(data.parts);
            }

            renderParts(allParts);
            updateCount(allParts.length, totalRecords, data.total_blocked);
            updateFilteredSum(data.total_blocked);
        } else {
            tbody.innerHTML = '<tr><td colspan="7" style="text-align: center; padding: 2rem; color: var(--color-error);">Błąd ładowania danych z MOSYS</td></tr>';
        }
    } catch (error) {
        if (error.name === 'AbortError') {
            console.log('Request cancelled - new request in progress');
        } else {
            console.error('Error fetching blocked parts:', error);
            tbody.innerHTML = `
                <tr>
                    <td colspan="7" style="text-align: center; padding: 3rem; color: var(--color-ink-muted);">
                        <p style="font-size: 1rem; font-weight: 500; color: #c33; margin-bottom: 0.5rem;">Błąd połączenia z MOSYS</p>
                        <p style="font-size: 0.875rem;">Nie udało się pobrać danych o zablokowanych detalach.</p>
                    </td>
                </tr>`;
        }
    } finally {
        isLoading = false;
        currentAbortController = null;
        if (loadingIndicator) {
            loadingIndicator.style.display = 'none';
        }
    }
}

/**
 * Render accumulated parts into tbody.
 */
function renderParts(parts) {
    const tbody = document.getElementById('blocked-tbody');

    if (parts.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="7" style="text-align: center; padding: 3rem; color: var(--color-ink-muted);">
                    <p style="font-size: 1.125rem; font-weight: 500;">Brak zablokowanych detali</p>
                    <p style="font-size: 0.875rem;">Aktualnie żadne detale nie oczekują na sortowanie.</p>
                </td>
            </tr>`;
        return;
    }

    const formatNumber = (num) => num.toLocaleString('pl-PL').replace(/,/g, ' ');

    tbody.innerHTML = parts.map(p => `
        <tr class="clickable-row" onclick="openBoxesModal('${escapeHtml(p.nc)}')">
            <td style="font-weight: 500;">${escapeHtml(p.kod) || '-'}</td>
            <td>${escapeHtml(p.nc) || '-'}</td>
            <td style="white-space: nowrap;">${escapeHtml(p.data) || '-'}</td>
            <td style="max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
                title="${escapeHtml(p.opis)}">${escapeHtml(p.opis) || '-'}</td>
            <td style="white-space: nowrap; font-size: 0.75rem;">${escapeHtml(p.produced)}</td>
            <td style="text-align: right; font-weight: 500; padding-right: 1.5rem;">${p.opakowan}</td>
            <td style="text-align: right; font-weight: 500; color: #991b1b; padding-right: 1.5rem;">${formatNumber(p.ilosc)}</td>
        </tr>
    `).join('');
}

/**
 * Apply column filters — refetch from first page.
 */
function applyFilters() {
    fetchParts(true);
}

/**
 * Sort table by column — refetch from first page.
 */
function sortTable(field) {
    if (currentSort.field === field) {
        currentSort.direction = currentSort.direction === 'asc' ? 'desc' : 'asc';
    } else {
        currentSort.field = field;
        currentSort.direction = 'asc';
    }
    updateSortIcons(field);
    fetchParts(true);
}

/**
 * Update pagination info and row-count pill.
 */
function updateCount(visible, total, totalBlocked) {
    document.getElementById('visible-count').textContent = visible;
    document.getElementById('total-count').textContent = total;
    document.getElementById('row-count').textContent = `${total} pozycji`;

    const blockedPill = document.getElementById('total-blocked-pill');
    if (blockedPill) {
        const fmt = (n) => n.toLocaleString('pl-PL').replace(/,/g, ' ');
        blockedPill.textContent = `${fmt(totalBlocked)} szt. zablokowanych`;
    }
}

/**
 * Show/hide the filtered-sum-box in the table header.
 */
function updateFilteredSum(totalBlocked) {
    const hasActiveFilters = Object.values(searchFilters).some(v => v !== '');
    const filteredSumBox = document.getElementById('filtered-sum-box');
    const totalQtyEl = document.getElementById('total-blocked-qty');

    if (!filteredSumBox) return;

    if (hasActiveFilters) {
        filteredSumBox.style.display = 'block';
        if (totalQtyEl) {
            totalQtyEl.textContent = totalBlocked.toLocaleString('pl-PL').replace(/,/g, ' ');
        }
    } else {
        filteredSumBox.style.display = 'none';
    }
}

/**
 * Update sort icon states in column headers.
 */
function updateSortIcons(activeField) {
    document.querySelectorAll('.sortable').forEach(th => {
        const sortIcon = th.querySelector('.sort-icon');
        if (!sortIcon) return;
        const thHeader = th.querySelector('.th-header');
        if (!thHeader) return;
        const onclickAttr = thHeader.getAttribute('onclick');
        if (!onclickAttr) return;
        const match = onclickAttr.match(/sortTable\('(.+?)'\)/);
        const field = match ? match[1] : null;

        if (field === activeField) {
            th.classList.add('sorted');
            sortIcon.style.opacity = '1';
            sortIcon.style.transform = currentSort.direction === 'desc' ? 'rotate(180deg)' : 'rotate(0deg)';
        } else {
            th.classList.remove('sorted');
            sortIcon.style.opacity = '0';
            sortIcon.style.transform = 'rotate(0deg)';
        }
    });
}

/**
 * Clear all column search filters.
 */
function clearAllFilters() {
    document.querySelectorAll('.column-search').forEach(input => { input.value = ''; });
    searchFilters = {};
    applyFilters();
    updateClearFiltersButton();
}

/**
 * Show/hide the clear filters button.
 */
function updateClearFiltersButton() {
    const clearBtn = document.getElementById('btn-clear-filters');
    const hasActive = Object.values(searchFilters).some(v => v !== '');
    if (clearBtn) clearBtn.style.display = hasActive ? 'inline-flex' : 'none';
}

/**
 * Escape HTML to prevent XSS.
 */
function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

/**
 * Debounce helper.
 */
function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => { clearTimeout(timeout); func(...args); };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

/**
 * Open boxes modal and fetch box details.
 */
async function openBoxesModal(ncNumber) {
    const modal = document.getElementById('boxes-modal');
    const modalBody = document.getElementById('modal-body');
    const modalNcNumber = document.getElementById('modal-nc-number');

    modalNcNumber.textContent = ncNumber;
    modal.classList.add('active');
    modalBody.innerHTML = '<div class="modal-loading">Ładowanie...</div>';

    try {
        const response = await fetch(`/wykaz-zablokowanych/boxes/${ncNumber}`);
        const data = await response.json();

        if (data.success && data.boxes.length > 0) {
            let tableHtml = `
                <table class="modal-table" style="table-layout: fixed; width: 100%;">
                    <colgroup>
                        <col style="width: 30%;">
                        <col style="width: 18%;">
                        <col style="width: 14%;">
                        <col style="width: 12%;">
                        <col style="width: 26%;">
                    </colgroup>
                    <thead>
                        <tr>
                            <th style="white-space: nowrap;">Nr opakowania</th>
                            <th style="white-space: normal;">Data produkcji</th>
                            <th>Operator</th>
                            <th style="text-align: right;">Ilość</th>
                            <th>Lokalizacja</th>
                        </tr>
                    </thead>
                    <tbody>
            `;

            data.boxes.forEach(box => {
                tableHtml += `
                    <tr>
                        <td style="overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
                            title="${box.numero_confezione}">${box.numero_confezione}</td>
                        <td style="white-space: nowrap; font-size: 0.75rem;">${box.data_carico}</td>
                        <td style="text-align: center;">${box.oper_carico}</td>
                        <td style="text-align: right; font-weight: 500;">${box.qt_blocked}</td>
                        <td style="overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
                            title="${box.location}">${box.location}</td>
                    </tr>
                `;
            });

            tableHtml += '</tbody></table>';
            modalBody.innerHTML = tableHtml;
        } else {
            modalBody.innerHTML = '<div class="modal-loading">Brak danych o opakowaniach</div>';
        }
    } catch (error) {
        console.error('Error fetching box details:', error);
        modalBody.innerHTML = '<div class="modal-loading" style="color: #c33;">Błąd podczas ładowania danych</div>';
    }
}

/**
 * Close boxes modal.
 */
function closeBoxesModal() {
    document.getElementById('boxes-modal').classList.remove('active');
}

/**
 * Close modal when clicking on backdrop.
 */
function closeModalOnBackdrop(event) {
    if (event.target.id === 'boxes-modal') {
        closeBoxesModal();
    }
}
