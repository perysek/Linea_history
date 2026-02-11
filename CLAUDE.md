# LINEA_60 Project

## Overview
LINEA Flask web application for manufacturing quality control and sorting area data management. The application integrates with MOSYS (external production database via pyodbc) and maintains its own SQLite database for sorting/selection data.

## Architecture

### Application Factory Pattern
**File**: `app/__init__.py`

The application uses Flask's application factory pattern with environment-based configuration:
- `create_app(config_name)` - Factory function supporting 'development', 'production', or 'default' configs
- Extensions: SQLAlchemy (db), Flask-Migrate (migrate)
- Blueprints: `linea_bp`, `placeholder_bp`
- Root route redirects to `linea.index`

### Configuration
**File**: `config.py`

Three configuration classes:
- `Config` - Base configuration with SQLite database
- `DevelopmentConfig` - Debug mode enabled, SQL echoing
- `ProductionConfig` - Production settings

Database configuration:
- Local SQLite: `linea.db` for sorting area data
- MOSYS: Separate pyodbc connection (not via SQLAlchemy)
- Connection pooling with pre-ping and 1-hour recycle

## Database Models

### Sorting Area Models
**File**: `app/models/sorting_area.py`

Four SQLAlchemy models for quality control workflow:

1. **KategoriaZrodlaDanych** (dzialy table)
   - Department/category master data
   - Fields: id, opis_kategorii, koszt_pracy

2. **Operator** (operatorzy table)
   - Operator information
   - Fields: id, nr_operatora, imie_nazwisko, dzial_id
   - Relationship: Many-to-one with KategoriaZrodlaDanych

3. **DaneRaportu** (dane_z_raportow table)
   - Main report data for sorting/control results
   - Fields: nr_raportu, operator_id, nr_niezgodnosci, data_niezgodnosci, nr_zamowienia, kod_detalu, nr_instrukcji, selekcja_na_biezaco, ilosc_detali_sprawdzonych, czas_pracy, zalecana_wydajnosc, uwagi, uwagi_do_wydajnosci, data_selekcji
   - MOSYS cached columns: data_niezgodnosci, nr_zamowienia, kod_detalu
   - Relationship: One-to-many with BrakiDefektyRaportu (cascade delete)
   - Computed properties:
     - `total_defects` - Sum of all defects
     - `rzeczywista_wydajnosc` - Parts per hour
     - `efektywnosc` - Efficiency percentage vs target

4. **BrakiDefektyRaportu** (braki_defekty_raportow table)
   - Defect details linked to reports
   - Fields: id, raport_id, defekt, ilosc

**File**: `app/models/__init__.py`
- Exports all sorting_area models
- MOSYS models intentionally not imported (use separate connection)

## Routes

### Placeholder Blueprint
**File**: `app/routes/placeholder.py`

Key routes for quality control features:

1. **GET /wykaz-zablokowanych**
   - Lists blocked parts from MOSYS
   - Formats production date ranges
   - Handles MOSYS connection errors gracefully

2. **GET /wykaz-zablokowanych/boxes/<nr_niezgodnosci>**
   - AJAX endpoint for box details modal
   - Returns JSON with box data for specific NC number

3. **GET /dane-selekcji**
   - Dashboard with sorting reports table
   - Features:
     - Date range filtering with presets (last week, month, quarter, year)
     - Text filters on 7 columns (operator, nr_raportu, nr_niezgodnosci, data_nc, commessa, kod_detalu, nr_instrukcji)
     - Server-side sorting on 10 valid columns
     - Eager loading (joinedload) to prevent N+1 queries
     - Pre-computed stats using SQL aggregations
     - Lazy loading of missing MOSYS data (batch fetch via `get_batch_niezgodnosc_details`)
   - Query optimization:
     - `joinedload` for operator and department relationships
     - SQL-level aggregations for stats (count, sum)
     - Filtered stats matching table filters
     - Batch MOSYS queries for reports with missing cached data
     - Single `db.session.commit()` after batch update
   - Stats computed: report count, parts checked, total defects, average scrap rate, average productivity, hours worked

## Frontend

### Base Template
**File**: `app/templates/base.html`

Main layout template with:
- Sidebar navigation (256px fixed width)
- Dark gradient sidebar (#1e293b to #0f172a)
- Inter font family
- Responsive main content area with scrolling
- CSS custom properties for consistent theming

### Templates
**File**: `app/templates/placeholder/dane_selekcji.html`

Full implementation of selection data dashboard (replaced placeholder in commit ebcb8c8):
- Extends `base.html` (not `base_placeholder.html`)
- Six stat cards displaying aggregated metrics (Raportów, Detali, Braków, Brakowość, Wydajność, Godz. pracy)
- Date filter pills with preset buttons (last week, month, quarter, year, all)
- Responsive data table with 15 columns:
  - Sortable columns with ▼/▲ indicators (data_selekcji, nr_raportu, nr_niezgodnosci, data_niezgodnosci)
  - Column search inputs on RAPORT and NR NC columns
  - Fixed column widths using colgroup
  - Color-coded indicators:
    - Scrap rate: red (≥5%), orange (≥2%), green (>0%)
    - Online status: green badge (Tak), gray badge (Nie)
    - Defects: orange text when >0
  - Number formatting: space-separated thousands (`{:,.0f}".format(value).replace(',', ' ')`)
  - Max height with scrolling via table-scroll-wrapper

Key data displayed:
- Selection date, department, report number, NC number, NC date
- Order number, part code, instruction number
- Online/offline sorting indicator (selekcja_na_biezaco)
- Parts checked, defects found (total_defects property), defect types list
- Scrap percentage (calculated: total_defects / ilosc_detali_sprawdzonych * 100)
- Work time (czas_pracy), productivity (rzeczywista_wydajnosc property)

**File**: `app/templates/placeholder/wykaz_zablokowanych.html`

Blocked parts table with MOSYS integration:
- Modal box for viewing production box details
- AJAX endpoint for dynamic box loading

## Integration Points

### MOSYS Integration
The application integrates with an external MOSYS database:
- Connection: Direct pyodbc (not via SQLAlchemy)
- Functions in `MOSYS_data_functions.py`:
  - `get_all_blocked_parts()` - Blocked parts list with production date ranges
  - `get_blocked_boxes_details(nr_niezgodnosci)` - Box details for specific NC number
  - `get_batch_niezgodnosc_details(nr_list)` - Batch fetch for multiple NC numbers (returns dict mapping NC to details)
- Data caching: NC dates, order numbers, part codes cached in DaneRaportu table
- Lazy loading pattern: Reports with missing cached MOSYS data are batch-fetched and committed once

## Conventions

### Template Patterns
- **Base template selection**: Production templates extend `base.html`; placeholder/TODO templates extend `base_placeholder.html`
- **Number formatting**: Use space-separated thousands for Polish locale: `{:,.0f}".format(value).replace(',', ' ')`
- **Date formatting**: Use `strftime('%d.%m.%y')` for compact date display (DD.MM.YY)
- **Color-coded metrics**:
  - Scrap rate thresholds: red ≥5%, orange ≥2%, green >0%, gray for 0
  - Status badges: green for positive/active, gray for negative/inactive
  - Warning text: orange (#f59e0b) for attention items
- **Table styling**:
  - Use `colgroup` for fixed column widths
  - Inline styles for component-specific adjustments
  - Font size 0.6875rem (11px) for table data, 0.625rem (10px) for dense content
- **CSS organization**: Main styles in `static/css/linea.css`, component-specific styles inline in templates

### Query Optimization
- **Eager loading**: Always use `joinedload()` for relationships accessed in templates
- **Filtered aggregations**: Apply identical filters to stats queries and main data query
- **Batch external calls**: Collect missing data IDs, fetch in single batch, commit once

## Development Notes

### Database Migration Pattern
- Use Flask-Migrate for schema changes to local SQLite
- MOSYS database is read-only, no migrations needed there

### Performance Considerations
- Eager loading with joinedload prevents N+1 query issues
- Stats computed at SQL level for efficiency (func.count, func.sum, func.coalesce)
- Date range defaults to last month to limit initial data load
- Max table height prevents excessive DOM rendering
- Batch MOSYS queries instead of individual calls (lazy loading pattern)
- Single commit after batch updates

### Query Optimization Patterns
- **Eager loading**: Use `joinedload()` for relationships accessed in templates to prevent N+1 queries
- **SQL aggregations**: Use `func.count()`, `func.sum()`, `func.coalesce()` instead of Python loops
- **Filtered aggregations**: Apply same filters to stats queries as main query for accuracy
- **Batch external API calls**: Collect IDs needing external data, make single batch request, commit once

### Placeholder Routes Status
Implemented features (fully functional):
- `/wykaz-zablokowanych` - Blocked parts list with MOSYS integration
- `/dane-selekcji` - Selection data dashboard with advanced filtering, stats, and date range presets

Features under development (TODO placeholders):
- `/kontrola-jakosci` - Quality control workflow
- `/utrzymanie-form` - Mold maintenance tracking
- `/dane-zamowien` - Order data management
- `/analiza-danych` - Data analysis and reporting

## File Structure Summary
```
app/
  __init__.py           - Application factory with blueprint registration
  models/
    __init__.py         - Model exports
    sorting_area.py     - Quality control data models
  routes/
    placeholder.py      - Feature routes (blocked parts, selection data)
  templates/
    base.html           - Main layout with sidebar navigation
    placeholder/
      base_placeholder.html  - TODO list layout for features under development
      dane_selekcji.html     - Selection data dashboard (implemented)
      wykaz_zablokowanych.html - Blocked parts table (implemented)
      kontrola_jakosci.html    - Quality control (placeholder)
      utrzymanie_form.html     - Mold maintenance (placeholder)
      dane_zamowien.html       - Order data (placeholder)
      analiza_danych.html      - Data analysis (placeholder)
  static/
    css/
      linea.css         - Main stylesheet with design system variables
config.py               - Environment-based configuration
MOSYS_data_functions.py - MOSYS database integration functions
```
