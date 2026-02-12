/**
 * Virtual Scroll Manager
 * Renders only visible table rows for improved performance with large datasets
 */

class VirtualScrollManager {
    constructor(options) {
        this.container = options.container;           // Scroll container element
        this.tbody = options.tbody;                   // Table body element
        this.data = options.data || [];               // All row data
        this.rowHeight = options.rowHeight || 45;     // Estimated row height in pixels
        this.bufferSize = options.bufferSize || 5;    // Extra rows to render above/below viewport
        this.renderRow = options.renderRow;           // Function to render a single row
        this.onScroll = options.onScroll;             // Optional scroll callback

        this.visibleStart = 0;
        this.visibleEnd = 0;
        this.rafId = null;
        this.scrollHeight = 0;
        this.spacerTop = null;
        this.spacerBottom = null;

        this.init();
    }

    init() {
        this.createSpacers();

        // Bind scroll handler
        this.container.addEventListener('scroll', () => this.handleScroll(), { passive: true });

        // Initial render
        this.render();
    }

    createSpacers() {
        // Create spacer elements for virtual scrolling
        this.spacerTop = document.createElement('tr');
        this.spacerTop.className = 'virtual-scroll-spacer-top';
        this.spacerTop.style.height = '0px';

        this.spacerBottom = document.createElement('tr');
        this.spacerBottom.className = 'virtual-scroll-spacer-bottom';
        this.spacerBottom.style.height = '0px';

        // Add spacers to tbody
        this.tbody.insertBefore(this.spacerTop, this.tbody.firstChild);
        this.tbody.appendChild(this.spacerBottom);
    }

    reinitializeSpacers() {
        // Remove old spacers if they exist
        if (this.spacerTop && this.spacerTop.parentNode) {
            this.spacerTop.remove();
        }
        if (this.spacerBottom && this.spacerBottom.parentNode) {
            this.spacerBottom.remove();
        }

        // Create new spacers
        this.createSpacers();
    }

    handleScroll() {
        // Use requestAnimationFrame for smooth scrolling
        if (this.rafId) {
            cancelAnimationFrame(this.rafId);
        }

        this.rafId = requestAnimationFrame(() => {
            this.render();
            if (this.onScroll) {
                this.onScroll();
            }
        });
    }

    updateData(newData) {
        console.log(`VirtualScroll: updating data (${newData.length} rows)`);
        this.data = newData;
        // Reset scroll to top when data changes significantly
        if (this.container.scrollTop > 0) {
            this.container.scrollTop = 0;
        }
        this.render();
    }

    render() {
        const startTime = performance.now();
        const dataLength = this.data.length;

        if (dataLength === 0) {
            this.clearRows();
            console.log('VirtualScroll: no data to render');
            return;
        }

        // Verify spacers still exist in DOM (they might have been removed)
        if (!this.spacerBottom.parentNode || !this.spacerTop.parentNode) {
            console.warn('VirtualScroll: spacers lost from DOM, reinitializing...');
            this.reinitializeSpacers();
        }

        // Calculate visible range
        const scrollTop = this.container.scrollTop;
        const containerHeight = this.container.clientHeight;

        const startIndex = Math.floor(scrollTop / this.rowHeight);
        const endIndex = Math.ceil((scrollTop + containerHeight) / this.rowHeight);

        // Add buffer
        this.visibleStart = Math.max(0, startIndex - this.bufferSize);
        this.visibleEnd = Math.min(dataLength, endIndex + this.bufferSize);

        // Update spacer heights
        const topSpacerHeight = this.visibleStart * this.rowHeight;
        const bottomSpacerHeight = (dataLength - this.visibleEnd) * this.rowHeight;

        this.spacerTop.style.height = `${topSpacerHeight}px`;
        this.spacerBottom.style.height = `${bottomSpacerHeight}px`;

        // Clear existing rows (except spacers)
        this.clearRows();

        // Render visible rows
        const fragment = document.createDocumentFragment();
        const rowsToRender = this.visibleEnd - this.visibleStart;

        for (let i = this.visibleStart; i < this.visibleEnd; i++) {
            const row = this.renderRow(this.data[i], i);
            fragment.appendChild(row);
        }

        // Insert rendered rows between spacers
        try {
            this.tbody.insertBefore(fragment, this.spacerBottom);
        } catch (error) {
            console.error('VirtualScroll: insertBefore failed, reinitializing spacers', error);
            this.reinitializeSpacers();
            this.tbody.insertBefore(fragment, this.spacerBottom);
        }

        const renderTime = performance.now() - startTime;
        console.log(`VirtualScroll: rendered ${rowsToRender} rows (${this.visibleStart}-${this.visibleEnd} of ${dataLength}) in ${renderTime.toFixed(2)}ms`);
    }

    clearRows() {
        // Remove all rows except spacers
        const rows = Array.from(this.tbody.children);
        rows.forEach(row => {
            if (row !== this.spacerTop && row !== this.spacerBottom) {
                row.remove();
            }
        });
    }

    getVisibleRange() {
        return {
            start: this.visibleStart,
            end: this.visibleEnd,
            total: this.data.length
        };
    }

    scrollToIndex(index) {
        const scrollTop = index * this.rowHeight;
        this.container.scrollTop = scrollTop;
    }

    destroy() {
        if (this.rafId) {
            cancelAnimationFrame(this.rafId);
        }
        this.container.removeEventListener('scroll', this.handleScroll);
        this.clearRows();
        if (this.spacerTop && this.spacerTop.parentNode) {
            this.spacerTop.remove();
        }
        if (this.spacerBottom && this.spacerBottom.parentNode) {
            this.spacerBottom.remove();
        }
    }
}
