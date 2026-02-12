/**
 * Wykaz zablokowanych - JavaScript for sorting and filtering with virtual scrolling
 * Refined Minimal Design System
 */

let currentSort = { field: 'KOD_DETALU', direction: 'asc' };
let searchFilters = {};
let allData = [];          // All data from the table
let filteredData = [];     // Filtered subset of data
let virtualScroll = null;  // Virtual scroll manager instance

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
    // Extract data from existing DOM
    loadDataFromDOM();

    // Initialize virtual scrolling
    initVirtualScroll();

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
 * Load data from existing DOM rows into memory
 */
function loadDataFromDOM() {
    const tbody = document.getElementById('blocked-tbody');
    const rows = tbody.querySelectorAll('tr[data-kod]');

    allData = Array.from(rows).map(row => ({
        kod: row.dataset.kod || '',
        nc: row.dataset.nc || '',
        data: row.dataset.data || '',
        opis: row.dataset.opis || '',
        produced: row.dataset.produced || '',
        opakowan: parseInt(row.dataset.opakowan) || 0,
        ilosc: parseInt(row.dataset.ilosc) || 0
    }));

    filteredData = [...allData];
}

/**
 * Initialize virtual scrolling
 */
function initVirtualScroll() {
    const container = document.querySelector('.tbody-scroll');
    const tbody = document.getElementById('blocked-tbody');

    if (!container || !tbody) return;

    virtualScroll = new VirtualScrollManager({
        container: container,
        tbody: tbody,
        data: filteredData,
        rowHeight: 45,          // Approximate row height in pixels
        bufferSize: 5,          // Render 5 extra rows above/below viewport
        renderRow: renderRow
    });
}

/**
 * Render a single table row
 */
function renderRow(data, index) {
    const row = document.createElement('tr');
    row.className = 'clickable-row';
    row.setAttribute('data-kod', data.kod);
    row.setAttribute('data-nc', data.nc);
    row.setAttribute('data-data', data.data);
    row.setAttribute('data-opis', data.opis);
    row.setAttribute('data-produced', data.produced);
    row.setAttribute('data-opakowan', data.opakowan);
    row.setAttribute('data-ilosc', data.ilosc);
    row.onclick = () => openBoxesModal(data.nc);

    // Format number with space separator
    const formatNumber = (num) => num.toLocaleString('pl-PL').replace(/,/g, ' ');

    row.innerHTML = `
        <td style="font-weight: 500;">${data.kod || '-'}</td>
        <td>${data.nc}</td>
        <td style="white-space: nowrap;">${data.data || '-'}</td>
        <td style="max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${data.opis}">${data.opis || '-'}</td>
        <td style="white-space: nowrap; font-size: 0.75rem;">${data.produced}</td>
        <td style="text-align: right; font-weight: 500; padding-right: 1.5rem;">${data.opakowan}</td>
        <td style="text-align: right; font-weight: 500; color: #991b1b; padding-right: 1.5rem;">${formatNumber(data.ilosc)}</td>
    `;

    return row;
}

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
 * Apply search filters to data array
 */
function applyFilters() {
    // Filter data array
    filteredData = allData.filter(item => {
        // Check each active filter
        for (const [column, searchValue] of Object.entries(searchFilters)) {
            if (searchValue) {
                // Map column names to data properties
                const attrMap = {
                    'KOD_DETALU': 'kod',
                    'NR_NIEZG': 'nc',
                    'DATA_NIEZG': 'data',
                    'OPIS_NIEZG': 'opis'
                };
                const attr = attrMap[column];
                const cellValue = (item[attr] || '').toLowerCase();
                const search = searchValue.toLowerCase();

                if (!cellValue.includes(search)) {
                    return false;
                }
            }
        }
        return true;
    });

    // Apply current sort to filtered data
    sortFilteredData();

    // Update virtual scroll with filtered data
    if (virtualScroll) {
        virtualScroll.updateData(filteredData);
    }

    // Calculate totals
    const visibleCount = filteredData.length;
    const totalBlocked = filteredData.reduce((sum, item) => sum + item.ilosc, 0);

    // Update row count
    document.getElementById('visible-count').textContent = visibleCount;
    document.getElementById('total-count').textContent = allData.length;
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
    // Toggle sort direction if same field, otherwise default to ascending
    if (currentSort.field === field) {
        currentSort.direction = currentSort.direction === 'asc' ? 'desc' : 'asc';
    } else {
        currentSort.field = field;
        currentSort.direction = 'asc';
    }

    // Update sort icons
    updateSortIcons(field);

    // Sort filtered data and update virtual scroll
    sortFilteredData();

    if (virtualScroll) {
        virtualScroll.updateData(filteredData);
    }
}

/**
 * Sort the filtered data array based on current sort settings
 */
function sortFilteredData() {
    const field = currentSort.field;
    const direction = currentSort.direction;

    // Map field names to data properties
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

    filteredData.sort((a, b) => {
        let aVal, bVal;

        // Get values based on field type
        if (field === 'ILOSC_ZABL' || field === 'ILOSC_OPAKOWAN') {
            // Numeric sorting
            aVal = a[attr] || 0;
            bVal = b[attr] || 0;

            return direction === 'asc' ? aVal - bVal : bVal - aVal;
        } else {
            // String sorting
            aVal = (a[attr] || '').toLowerCase();
            bVal = (b[attr] || '').toLowerCase();

            if (aVal < bVal) return direction === 'asc' ? -1 : 1;
            if (aVal > bVal) return direction === 'asc' ? 1 : -1;
            return 0;
        }
    });
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

    // Reset filtered data to all data
    filteredData = [...allData];

    // Reapply sorting and update virtual scroll
    sortFilteredData();

    if (virtualScroll) {
        virtualScroll.updateData(filteredData);
    }

    // Update counts
    document.getElementById('visible-count').textContent = filteredData.length;
    document.getElementById('total-count').textContent = allData.length;
    document.getElementById('row-count').textContent = `${filteredData.length} pozycji`;

    // Hide filtered sum box
    const filteredSumBox = document.getElementById('filtered-sum-box');
    if (filteredSumBox) {
        filteredSumBox.style.display = 'none';
    }

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
