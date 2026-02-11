/**
 * Wykaz zablokowanych - JavaScript for sorting and filtering
 * Refined Minimal Design System
 */

let currentSort = { field: 'KOD_DETALU', direction: 'asc' };
let searchFilters = {};

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
    // Setup column search inputs with debounced filtering
    document.querySelectorAll('.column-search').forEach(input => {
        input.addEventListener('input', debounce(() => {
            searchFilters[input.dataset.column] = input.value.trim();
            applyFilters();
            updateClearFiltersButton();
        }, 300));
    });

    // Apply initial sort by KOD_DETALU ascending
    sortTable('KOD_DETALU');
});

/**
 * Debounce function to limit how often a function is called
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

/**
 * Apply search filters to table rows
 */
function applyFilters() {
    const tbody = document.getElementById('blocked-tbody');
    const rows = tbody.querySelectorAll('tr[data-kod]');
    let visibleCount = 0;
    let totalBlocked = 0;

    rows.forEach(row => {
        let visible = true;

        // Check each active filter
        for (const [column, searchValue] of Object.entries(searchFilters)) {
            if (searchValue) {
                // Map column names to data attributes
                const attrMap = {
                    'KOD_DETALU': 'kod',
                    'NR_NIEZG': 'nc',
                    'DATA_NIEZG': 'data',
                    'OPIS_NIEZG': 'opis'
                };
                const attr = attrMap[column];
                const cellValue = (row.dataset[attr] || '').toLowerCase();
                const search = searchValue.toLowerCase();

                if (!cellValue.includes(search)) {
                    visible = false;
                    break;
                }
            }
        }

        row.style.display = visible ? '' : 'none';
        if (visible) {
            visibleCount++;
            totalBlocked += parseInt(row.dataset.ilosc) || 0;
        }
    });

    // Update row count
    document.getElementById('visible-count').textContent = visibleCount;
    document.getElementById('row-count').textContent = `${visibleCount} pozycji`;

    // Check if any filters are active
    const hasActiveFilters = Object.values(searchFilters).some(val => val !== '');

    // Update total blocked quantity with space as thousands separator
    const totalQtyElement = document.getElementById('total-blocked-qty');
    const filteredSumBox = document.getElementById('filtered-sum-box');

    if (hasActiveFilters && totalQtyElement && filteredSumBox) {
        // Show filtered sum when filters are active
        filteredSumBox.style.display = 'block';
        totalQtyElement.textContent = totalBlocked.toLocaleString('pl-PL').replace(/,/g, ' ');
    } else if (filteredSumBox) {
        // Hide filtered sum when no filters are active
        filteredSumBox.style.display = 'none';
    }
}

/**
 * Sort table by column
 */
function sortTable(field) {
    const tbody = document.getElementById('blocked-tbody');
    const rows = Array.from(tbody.querySelectorAll('tr[data-kod]'));

    // Toggle sort direction if same field, otherwise default to ascending
    if (currentSort.field === field) {
        currentSort.direction = currentSort.direction === 'asc' ? 'desc' : 'asc';
    } else {
        currentSort.field = field;
        currentSort.direction = 'asc';
    }

    // Update sort icons
    updateSortIcons(field);

    // Sort rows
    rows.sort((a, b) => {
        let aVal, bVal;

        // Map field names to data attributes
        const attrMap = {
            'KOD_DETALU': 'kod',
            'NR_NIEZG': 'nc',
            'DATA_NIEZG': 'data',
            'OPIS_NIEZG': 'opis',
            'PRODUCED': 'produced',
            'ILOSC_OPAKOWAN': 'opakowan',
            'ILOSC_ZABL': 'ilosc'
        };
        const attr = attrMap[field];

        // Get values based on field type
        if (field === 'ILOSC_ZABL' || field === 'ILOSC_OPAKOWAN') {
            // Numeric sorting
            aVal = parseInt(a.dataset[attr]) || 0;
            bVal = parseInt(b.dataset[attr]) || 0;

            return currentSort.direction === 'asc' ? aVal - bVal : bVal - aVal;
        } else {
            // String sorting
            aVal = (a.dataset[attr] || '').toLowerCase();
            bVal = (b.dataset[attr] || '').toLowerCase();

            if (aVal < bVal) return currentSort.direction === 'asc' ? -1 : 1;
            if (aVal > bVal) return currentSort.direction === 'asc' ? 1 : -1;
            return 0;
        }
    });

    // Reorder rows in DOM
    rows.forEach(row => tbody.appendChild(row));
}

/**
 * Update sort icon states
 */
function updateSortIcons(activeField) {
    document.querySelectorAll('.sortable').forEach(th => {
        const sortIcon = th.querySelector('.sort-icon');
        if (!sortIcon) return;

        // Extract field name from onclick attribute on th-header div
        const thHeader = th.querySelector('.th-header');
        if (!thHeader) return;

        const onclickAttr = thHeader.getAttribute('onclick');
        if (!onclickAttr) return;

        const fieldMatch = onclickAttr.match(/sortTable\('(.+?)'\)/);
        const field = fieldMatch ? fieldMatch[1] : null;

        if (field === activeField) {
            // Active sort column
            th.classList.add('sorted');
            sortIcon.style.opacity = '1';
            sortIcon.style.transform = currentSort.direction === 'desc' ? 'rotate(180deg)' : 'rotate(0deg)';
        } else {
            // Inactive columns
            th.classList.remove('sorted');
            sortIcon.style.opacity = '0';
            sortIcon.style.transform = 'rotate(0deg)';
        }
    });
}

/**
 * Clear all search filters
 */
function clearAllFilters() {
    // Clear all search inputs
    document.querySelectorAll('.column-search').forEach(input => {
        input.value = '';
    });

    // Clear filters object
    searchFilters = {};

    // Reapply filters (will show all rows)
    applyFilters();

    // Hide clear button
    updateClearFiltersButton();
}

/**
 * Show/hide the clear filters button based on active filters
 */
function updateClearFiltersButton() {
    const clearBtn = document.getElementById('btn-clear-filters');
    const hasActiveFilters = Object.values(searchFilters).some(val => val !== '');

    if (clearBtn) {
        clearBtn.style.display = hasActiveFilters ? 'inline-flex' : 'none';
    }
}

/**
 * Open boxes modal and fetch box details
 */
async function openBoxesModal(ncNumber) {
    const modal = document.getElementById('boxes-modal');
    const modalBody = document.getElementById('modal-body');
    const modalNcNumber = document.getElementById('modal-nc-number');

    // Set NC number in modal header
    modalNcNumber.textContent = ncNumber;

    // Show modal
    modal.classList.add('active');

    // Show loading state
    modalBody.innerHTML = '<div class="modal-loading">Ładowanie...</div>';

    try {
        // Fetch box details
        const response = await fetch(`/wykaz-zablokowanych/boxes/${ncNumber}`);
        const data = await response.json();

        if (data.success && data.boxes.length > 0) {
            // Build table HTML with optimized column widths
            let tableHtml = `
                <table class="modal-table" style="table-layout: fixed; width: 100%;">
                    <colgroup>
                        <col style="width: 30%;">  <!-- Nr opakowania -->
                        <col style="width: 18%;">  <!-- Data produkcji -->
                        <col style="width: 14%;">  <!-- Operator -->
                        <col style="width: 12%;">  <!-- Ilość -->
                        <col style="width: 26%;">  <!-- Lokalizacja -->
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
                    <tr style="padding: 2py;">
                        <td style="overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${box.numero_confezione}">${box.numero_confezione}</td>
                        <td style="white-space: nowrap; font-size: 0.75rem;">${box.data_carico}</td>
                        <td style="text-align: center;">${box.oper_carico}</td>
                        <td style="text-align: right; font-weight: 500;">${box.qt_blocked}</td>
                        <td style="overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${box.location}">${box.location}</td>
                    </tr>
                `;
            });

            tableHtml += `
                    </tbody>
                </table>
            `;

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
 * Close boxes modal
 */
function closeBoxesModal() {
    const modal = document.getElementById('boxes-modal');
    modal.classList.remove('active');
}

/**
 * Close modal when clicking on backdrop
 */
function closeModalOnBackdrop(event) {
    if (event.target.id === 'boxes-modal') {
        closeBoxesModal();
    }
}
