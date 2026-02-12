# LINEA_60 Project

## Overview
LINEA Flask web application for manufacturing quality control and sorting area data management. The application integrates with MOSYS (external production database via pyodbc) and maintains its own SQLite database for sorting/selection data.

**Recent changes**:
- Complete sorting area database integration (staged for commit)
  - Added comprehensive database models with SQLAlchemy (dzialy, operatorzy, dane_z_raportow, braki_defekty_raportow)
  - Created production-ready Excel migration tool with batch MOSYS enrichment, duplicate detection, and incremental import support
  - Set up Flask-Migrate with initial migration (1648cf2d4935) for schema version control
  - Enhanced /dane-selekcji dashboard with 9 text filters and 11 sortable columns
  - Implemented filter state persistence and advanced query optimization (joins, eager loading, batch API calls)
  - Migrated from Pervasive/STAAMP to dual-database architecture (SQLite for sorting, MOSYS via pyodbc read-only)

## Architecture

### Application Factory Pattern
**File**: `app/__init__.py`

The application uses Flask's application factory pattern with environment-based configuration:
- `create_app(config_name)` - Factory function supporting 'development', 'production', or 'default' configs
- Extensions initialized:
  - `db = SQLAlchemy()` - Database ORM
  - `migrate = Migrate()` - Database migration management
- Extension initialization pattern:
  1. Extensions created at module level
  2. `init_app()` called in factory function
  3. Models imported within app context after db initialization
- Blueprints: `linea_bp`, `placeholder_bp`
- Root route redirects to `linea.index`

**Key change** (commit e9b03ae): Added SQLAlchemy and Flask-Migrate integration. Models are now imported within app context to ensure db is properly initialized before model definitions are processed.

### Configuration
**File**: `config.py`

Three configuration classes:
- `Config` - Base configuration with SQLite database
- `DevelopmentConfig` - Debug mode enabled, SQL echoing
- `ProductionConfig` - Production settings

Database configuration:
- Local SQLite: `linea.db` for sorting area data (via SQLAlchemy)
  - Path: `sqlite:///<project_root>/linea.db`
  - Connection pooling: pre-ping enabled, 1-hour recycle
- MOSYS: Separate pyodbc connection (not via SQLAlchemy)
  - Used only for read operations via `MOSYS_data_functions.py`

**Note**: Previous configuration used Pervasive/STAAMP database via ODBC DSN. This was replaced with SQLite for sorting area data management (commit e9b03ae).

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
- Exports all sorting_area models for easy import
- MOSYS models intentionally not included (MOSYS uses separate pyodbc connection, not SQLAlchemy)
- Pattern: `from app.models.sorting_area import *` makes models available via `from app.models import DaneRaportu`

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
     - Text filters on 9 columns (data_selekcji, operator, nr_raportu, nr_niezgodnosci, data_nc, commessa, kod_detalu, nr_instrukcji, defekt)
     - Server-side sorting on 11 valid columns (including selekcja_na_biezaco)
     - Eager loading (joinedload) to prevent N+1 queries
     - Pre-computed stats using SQL aggregations
     - Lazy loading of missing MOSYS data (batch fetch via `get_batch_niezgodnosc_details`)
   - Query optimization:
     - `joinedload` for operator and department relationships
     - SQL-level aggregations for stats (count, sum)
     - Filtered stats matching table filters
     - Batch MOSYS queries for reports with missing cached data
     - Single `db.session.commit()` after batch update
     - Date field filtering using `.cast(db.String)` for proper string comparison
     - Join with BrakiDefektyRaportu for defect-based filtering
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
  - Sortable columns with ▼/▲ indicators (11 columns: data_selekcji, operator, nr_raportu, nr_niezgodnosci, data_niezgodnosci, nr_zamowienia, kod_detalu, nr_instrukcji, selekcja_na_biezaco, ilosc_detali_sprawdzonych, defekt)
  - Column search inputs on 9 columns (DATA, DZIAŁ, RAPORT, NR NC, DATA NC, ZAM, KOD DETALU, INSTR, WADY)
  - Fixed column widths using colgroup
  - Color-coded indicators:
    - Scrap rate: red (≥5%), orange (≥2%), green (>0%)
    - Online status: green badge (Tak), gray badge (Nie)
    - Defects: orange text when >0
  - Number formatting: space-separated thousands (`{:,.0f}".format(value).replace(',', ' ')`)
  - Max height with scrolling via table-scroll-wrapper
  - All search inputs preserve filter values on page reload

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

## Data Migration

### Excel Import Tool
**File**: `migrate_excel_data.py`

Production data migration script for importing historical sorting data from Excel into SQLite database:

**Features**:
- Reads from PPM_wew.xlsm (macro-enabled Excel with formulas)
- Automatic duplicate detection based on (data_niezgodnosci, nr_raportu) tuple
- Batch MOSYS data enrichment for performance
- Defect parsing from Uwagi column using regex pattern matching
- Date-based filtering to skip old records
- Supports both pandas (fast) and openpyxl (fallback) for reading Excel
- Verbose row-by-row status display

**Command-line options**:
```bash
python migrate_excel_data.py                         # Normal import (verbose)
python migrate_excel_data.py --dry-run               # Preview without committing
python migrate_excel_data.py --quiet                 # Disable verbose output
python migrate_excel_data.py --start-row=10291       # Custom starting row
python migrate_excel_data.py --from-date=2026-01-01  # Date-based filtering
```

**Excel file structure**:
- Location: `G:\DOCUMENT\qualita\System Zarządzania Jakością\Cele jakościowe\PPM wewnętrzny koszty złej jakości (2023).xlsm`
- Sheet: 'dane'
- First row: header
- Data starts: row 2

**Processing logic**:
1. Load Excel data (pandas preferred, openpyxl fallback)
2. Parse each row starting from start_row (default: 2)
3. Check for duplicates using (data_niezgodnosci, nr_raportu) combination
4. Extract defects from Uwagi column (pattern: "defect_name x count")
5. Batch fetch MOSYS data for enrichment (nr_zamowienia, kod_detalu, data_niezgodnosci)
6. Create DaneRaportu records with related BrakiDefektyRaportu records
7. Commit in batches (default: 100 records per batch)

**Defect parsing**:
- Regex pattern: `([a-zA-ZąćęłńóśźżĄĆĘŁŃÓŚŹŻ\s]+)\s*x\s*(\d+)`
- Example: "pęcherze x52, nadpalenia x0" → [("pęcherze", 52)]
- Only creates records for defects with count > 0

**Fixed values**:
- operator_id: 2 (default operator)
- nr_instrukcji: 'wg raportu' (per report instruction)

**Helper utilities**:
- `check_db.py` - Database inspection tool
- `seed_database.py` - Test data seeding
- `verify_import.py` - Import verification tool

These files are in .gitignore as temporary helper scripts.

### Database Migrations
**Directory**: `migrations/`

Flask-Migrate (Alembic) setup for schema version control:

**Files**:
- `alembic.ini` - Alembic configuration
- `env.py` - Migration environment setup
- `script.py.mako` - Migration script template
- `README` - Migration documentation

**Initial Migration**:
- File: `versions/1648cf2d4935_initial_migration_sorting_area_tables.py`
- Revision ID: 1648cf2d4935
- Base revision: None (initial)
- Generated: 2026-02-12 00:20:12
- Creates four tables with relationships:
  1. `dzialy` (departments) - id, opis_kategorii (unique), koszt_pracy
  2. `operatorzy` (operators) - id, nr_operatora (unique), imie_nazwisko, dzial_id (FK to dzialy)
  3. `dane_z_raportow` (reports) - id, nr_raportu, operator_id (FK), nr_niezgodnosci, MOSYS cached fields, performance metrics, dates
  4. `braki_defekty_raportow` (defects) - id, raport_id (FK to dane_z_raportow), defekt, ilosc
- Includes foreign keys, unique constraints, and proper indexing
- Downgrade available: drops all tables in reverse order

**Migration commands**:
```bash
flask db migrate -m "description"  # Generate migration
flask db upgrade                    # Apply migrations
flask db downgrade                  # Rollback migrations
```

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
- Used by: migrate_excel_data.py for historical data enrichment, placeholder.py routes for live data

## Data Import Workflow

### Historical Data Migration
The application includes a complete workflow for importing historical sorting data:

**Step 1: Database Setup**
```bash
flask db upgrade  # Apply migrations to create tables
```

**Step 2: Data Import from Excel**
```bash
python migrate_excel_data.py --from-date=2026-01-01
```
- Reads PPM_wew.xlsm (macro-enabled Excel with formulas)
- Automatically detects and skips duplicates
- Parses defects from Uwagi column using regex
- Batch fetches MOSYS data for enrichment
- Commits in batches for performance

**Step 3: Verification**
```bash
python verify_import.py  # Check imported data
python check_db.py       # Inspect database state
```

### Data Flow Architecture
```
Excel (PPM_wew.xlsm)
    |
    v
migrate_excel_data.py
    |
    +---> Parse row data
    |     (operators, reports, defects)
    |
    +---> Batch fetch MOSYS enrichment
    |     (NC dates, order numbers, part codes)
    |
    v
SQLite (linea.db)
    |
    v
Flask Routes (dane_selekcji)
    |
    +---> Lazy load missing MOSYS data
    |     (batch fetch for performance)
    |
    v
Web Dashboard (Jinja2 templates)
```

### Duplicate Prevention
- Composite key: (data_niezgodnosci, nr_raportu)
- Check performed before insert
- Skip with warning message (not an error)
- Allows incremental imports from same source file

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

### Version Control
The `.gitignore` file excludes:
- Standard Python artifacts: `__pycache__/`, `*.pyc`, `*.pyo`, virtual environments
- Environment files: `.env`, `.env.production`, `.env.local`
- IDE files: `.vscode/`, `.idea/`, `.DS_Store`
- Data files: `*.xlsx`, `*.xls`, `*.db` (database is generated, not committed)
- Temporary helper scripts: `check_db.py`, `seed_database.py`, `verify_import.py`, `Tasks_*.txt`
- Legacy Flet app files: `collaudo10_1.py`, `crud.py`, `main.py`, etc.
- Claude/development artifacts: `.claude/`, `.playwright-mcp/`, `examples/`, `docs/`

**Important**: SQLite database (`linea.db`) is excluded from version control. Use migrations to recreate schema, then import data using `migrate_excel_data.py`.

### Database Migration Pattern
- Use Flask-Migrate for schema changes to local SQLite
- MOSYS database is read-only, no migrations needed there
- Migration workflow:
  1. Modify models in `app/models/sorting_area.py`
  2. Generate migration: `flask db migrate -m "description"`
  3. Review generated migration in `migrations/versions/`
  4. Apply migration: `flask db upgrade`
  5. Commit migration file to version control

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
- **Date field filtering**: Use `.cast(db.String)` when applying LIKE operations to Date columns (e.g., `DaneRaportu.data_selekcji.cast(db.String).ilike(f"%{filter_value}%")`)
- **Join-based filtering**: When filtering by related table data (e.g., defects), use explicit joins and apply filters on joined table

### Placeholder Routes Status
Implemented features (fully functional):
- `/wykaz-zablokowanych` - Blocked parts list with MOSYS integration (commit 5e42001)
- `/dane-selekcji` - Selection data dashboard with advanced filtering, stats, and date range presets (commit e9b03ae)

Features under development (TODO placeholders):
- `/kontrola-jakosci` - Quality control workflow
- `/utrzymanie-form` - Mold maintenance tracking
- `/dane-zamowien` - Order data management
- `/analiza-danych` - Data analysis and reporting

### Git History
Key commits:
- `e9b03ae` (2026-02-12) - Add sorting area data management with database integration
  - Created SQLAlchemy models for sorting area (dzialy, operatorzy, dane_z_raportow, braki_defekty_raportow)
  - Added Flask-Migrate for schema version control
  - Created Excel import tool (migrate_excel_data.py) with batch MOSYS enrichment
  - Implemented /dane-selekcji dashboard with filtering and stats
  - Migrated from Pervasive/STAAMP to SQLite for sorting data
- `5e42001` (prior) - Add blocked parts table with MOSYS integration and modal box details
- `ebcb8c8` (prior) - Add sidebar navigation and placeholder pages for future features
- `3f30a12` (prior) - Add production deployment configuration for Windows local network
- `50c9bea` (initial) - Initial commit: LINEA Flask web application

## File Structure Summary
```
app/
  __init__.py           - Application factory with SQLAlchemy/Flask-Migrate integration
  models/
    __init__.py         - Model exports (sorting_area models only)
    sorting_area.py     - Quality control data models (4 SQLAlchemy models)
  routes/
    linea.py            - Main LINEA routes
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

migrations/             - Flask-Migrate (Alembic) migrations for SQLite schema
  versions/
    1648cf2d4935_*.py   - Initial migration (sorting area tables) - Created 2026-02-12
  alembic.ini           - Alembic configuration
  env.py                - Migration environment setup
  script.py.mako        - Migration template
  README                - Migration documentation

config.py               - Environment-based configuration (SQLite for sorting, MOSYS separate)
MOSYS_data_functions.py - MOSYS database integration functions (pyodbc, read-only)
migrate_excel_data.py   - Excel to SQLite data migration tool (PPM_wew.xlsm import)
check_db.py             - Database inspection utility (temporary, in .gitignore)
seed_database.py        - Test data seeding utility (temporary, in .gitignore)
verify_import.py        - Import verification utility (temporary, in .gitignore)
linea.db                - SQLite database (generated, in .gitignore)

.gitignore              - Excludes: venv, __pycache__, .env, .claude/, *.db, *.xlsx, helper scripts, Tasks_*.txt
```

## Recent Changes (Staged for Commit)

**Complete Sorting Area Database Integration**

Major architectural migration to dual-database system with comprehensive data management tools:

**Database Architecture**:
- Created 4 SQLAlchemy models for sorting workflow (KategoriaZrodlaDanych, Operator, DaneRaportu, BrakiDefektyRaportu)
- Set up Flask-Migrate with initial migration `1648cf2d4935` (created 2026-02-12 00:20:12)
- Migrated from single Pervasive/STAAMP database to dual-database pattern:
  - Local SQLite (linea.db) for sorting area data with full CRUD via SQLAlchemy
  - MOSYS via pyodbc for read-only production data access
- Added data caching layer: MOSYS data (NC dates, order numbers, part codes) cached in DaneRaportu table

**Data Migration Tool** (`migrate_excel_data.py`):
- Production-ready Excel import supporting macro-enabled .xlsm files with formulas
- Automatic duplicate detection via (data_niezgodnosci, nr_raportu) composite key
- Intelligent defect parsing from Uwagi column using regex pattern matching
- Batch MOSYS enrichment for performance (100 records per batch)
- Command-line options: --dry-run, --quiet, --start-row, --from-date for flexible imports
- Fallback support: pandas (fast) → openpyxl (compatible) for Excel reading
- Incremental import capability: safely re-run on same source file

**Dashboard Enhancement** (`/dane-selekcji`):
- Extended filtering: 9 text filters including date, department, defect type
- Sortable columns: 11 total (added operator, nr_zamowienia, kod_detalu, nr_instrukcji, selekcja_na_biezaco, ilosc_detali_sprawdzonych)
- Filter state persistence across page reloads and sort operations
- Advanced query optimization:
  - `.cast(db.String)` pattern for Date field filtering
  - Explicit joins for defect and department filtering
  - Identical filter logic applied to data, stats, and defects queries
- Column-level search inputs on 9 columns with preserved filter values

**File Updates**:
- `app/__init__.py` - Added SQLAlchemy/Flask-Migrate initialization within app context
- `app/models/sorting_area.py` - Complete model definitions with relationships and computed properties
- `app/models/__init__.py` - Export configuration for sorting_area models
- `app/routes/placeholder.py` - Implemented dane_selekcji route with filtering, batch MOSYS lazy loading
- `app/templates/placeholder/dane_selekcji.html` - Full dashboard with sortable columns, search inputs, stat cards
- `config.py` - Switched from Pervasive ODBC to SQLite with connection pooling
- `.gitignore` - Added exclusions for *.db, helper scripts (check_db.py, seed_database.py, verify_import.py)
- `migrations/` - Complete Alembic setup with initial migration creating all 4 tables

**Technical Patterns**:
- Batch MOSYS queries instead of N+1 calls (collect IDs → single batch fetch → commit once)
- Eager loading with `joinedload()` for operator/department relationships
- SQL-level aggregations for dashboard stats (func.count, func.sum, func.coalesce)
- Cascade deletes on BrakiDefektyRaportu when parent DaneRaportu is deleted

## Implementation Notes

### Database Architecture Decision
The dual-database pattern separates concerns:
- **SQLite (linea.db)**: Owns sorting area workflow data, supports full CRUD operations
- **MOSYS (pyodbc)**: Read-only access to production database, data cached locally for performance

Benefits of this separation:
- Faster queries: Local SQLite access vs remote database calls
- Offline capability: Sorting data accessible without MOSYS connection
- Schema control: SQLAlchemy ORM with version-controlled migrations
- Data integrity: Foreign keys, constraints, and relationship management via ORM
- Performance: Batch caching of MOSYS data minimizes external calls

### Migration Strategy
Historical data import follows a safe, incremental pattern:
1. Duplicate detection prevents re-importing existing records
2. Batch processing (100 records) for memory efficiency
3. Dry-run mode for validation before commit
4. Date-based filtering to limit scope (`--from-date`)
5. Custom start row (`--start-row`) for resuming interrupted imports

### Query Optimization Patterns
Key patterns used throughout the application:
- **Eager loading**: `joinedload()` prevents N+1 queries on relationships
- **SQL aggregations**: Use database for math (func.count, func.sum) instead of Python loops
- **Filter consistency**: Apply identical filters to main query, stats query, and related queries
- **Batch external calls**: Collect missing data IDs → single batch MOSYS call → single commit
- **Type casting**: `.cast(db.String)` enables LIKE operations on Date columns in SQLite
