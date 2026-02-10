# LINEA Flask Web Application

A Flask web application for displaying LINEA production records from the MOSYS Pervasive database with real-time search, sorting, and filtering.

## Features

- ✅ **Date Range Filtering**: Preset buttons (7/30/60/90 days) + custom date picker
- ✅ **Column Search**: Real-time AJAX search on all text columns (500ms debounce)
- ✅ **Sortable Columns**: Click headers to sort ascending/descending
- ✅ **Responsive Design**: Columns hide on smaller screens
- ✅ **Refined Minimal UI**: Elegant design with Inter font and smooth animations
- ✅ **SQLAlchemy ORM**: Direct database queries (no pandas dependency)

## Project Structure

```
LINEA_60/
├── app/
│   ├── __init__.py              # Flask app factory
│   ├── models/
│   │   ├── notcojan.py          # NOTCOJAN table model
│   │   └── collaudo.py          # COLLAUDO table model
│   ├── routes/
│   │   └── linea.py             # LINEA routes + AJAX API
│   ├── templates/
│   │   ├── base.html            # Base template
│   │   └── linea/
│   │       └── index.html       # Main table view
│   └── static/
│       ├── css/
│       │   └── linea.css        # Refined Minimal styles
│       └── js/
│           └── linea.js         # AJAX functionality
├── config.py                    # Configuration
├── run.py                       # Application entry point
├── requirements.txt             # Dependencies
└── docs/
    └── plans/
        └── 2026-02-10-linea-flask-app-design.md
```

## Setup

### Prerequisites

- Python 3.8+
- ODBC DSN configured: `STAAMP_DB`
- Pervasive database access

### Installation

1. **Clone or navigate to the project directory:**
   ```bash
   cd C:\Users\piotrperesiak\PycharmProjects\LINEA_60
   ```

2. **Create virtual environment:**
   ```bash
   python -m venv .venv
   .venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment (optional):**
   ```bash
   copy .env.example .env
   # Edit .env with your settings
   ```

5. **Verify ODBC DSN connection:**
   - Ensure `STAAMP_DB` DSN is configured in Windows ODBC Data Source Administrator
   - Test connection to Pervasive database

## Running the Application

### Development Mode

```bash
python run.py
```

The application will be available at: `http://localhost:5000/linea`

### Production Mode

```bash
set FLASK_CONFIG=production
set SECRET_KEY=your-secure-secret-key
python run.py
```

## Usage

### Main Table View

Navigate to `/linea` to see the production records table.

**Default behavior:**
- Shows last 30 days of records
- Records sorted by date (descending)

### Date Filtering

**Preset buttons:**
- Click "7 dni", "30 dni", "60 dni", or "90 dni" for quick date ranges

**Custom range:**
- Select "Od" (from) date
- Select "Do" (to) date
- Click "Zastosuj" to apply

### Column Search

Type in the search box below any column header to filter records:
- **COMM** (Commessa): Order number
- **NR_NIEZG** (Nr NC): Non-conformity number
- **TYP_UWAGI** (Typ): Note type
- **UWAGA**: Combined notes (searches NOTE_01 through NOTE_10)
- **MASZYNA**: Machine/Press
- **KOD_DETALU**: Article code
- **NR_FORMY**: Mold number (STAMPO_I + STAMPO_P)

Search is **case-insensitive** with **500ms debounce** for smooth UX.

### Sorting

Click on any column header with a sort icon to toggle:
- First click: Sort ascending
- Second click: Sort descending

Sortable columns:
- COMM, DATA, GODZ, NR_NIEZG, TYP_UWAGI, MASZYNA, KOD_DETALU, NR_FORMY

## Database Models

### NOTCOJAN (Production Notes)

Primary keys: `COMMESSA`, `DATA`, `ORA`

**Fields:**
- `NOTE_01` through `NOTE_10`: Combined into `UWAGA`
- `NUMERO_NC`: Non-conformity number
- `TIPO_NOTA`: Note type

**Properties:**
- `formatted_date`: Converts YYYYMMDD → YYYY/MM/DD
- `formatted_time`: Converts HHMM → HH:MM
- `combined_notes`: Joins all NOTE fields

### COLLAUDO (Quality Control)

Primary key: `COMMESSA`

**Fields:**
- `PRESSA`: Machine/Press
- `ARTICOLO`: Article code
- `STAMPO_I`, `STAMPO_P`: Mold parts (combined into NR_FORMY)

## API Endpoints

### GET /linea
Main view with table

**Query Parameters:**
- `days`: Preset range (7, 30, 60, 90)
- `date_from`: Custom start date (YYYY-MM-DD)
- `date_to`: Custom end date (YYYY-MM-DD)

### GET /linea/api/search
AJAX endpoint for filtered records

**Query Parameters:**
- Date range: `days`, `date_from`, `date_to`
- Search: `search_COMM`, `search_NR_NIEZG`, `search_TYP_UWAGI`, `search_UWAGA`, `search_MASZYNA`, `search_KOD_DETALU`, `search_NR_FORMY`
- Sort: `sort` (field name), `dir` (asc/desc)

**Response:**
```json
{
    "success": true,
    "records": [...],
    "total": 42
}
```

## Responsive Design

### Desktop (>1024px)
All 9 columns visible

### Tablet (768-1024px)
Hidden columns:
- GODZ (Time)
- NR_NIEZG (NC Number)

### Mobile (<768px)
Hidden columns:
- GODZ, NR_NIEZG, TYP_UWAGI, KOD_DETALU

## Technology Stack

- **Backend**: Flask 3.x, SQLAlchemy 2.x
- **Database**: Pervasive SQL (via pyodbc)
- **Frontend**: Custom CSS (Refined Minimal), Vanilla JavaScript
- **Font**: Inter (Google Fonts)

## Development

### Debug Mode

Enable SQL query logging in `config.py`:

```python
SQLALCHEMY_ENGINE_OPTIONS = {
    'echo': True  # Shows SQL queries in console
}
```

### Adding New Features

1. **Models**: Add to `app/models/`
2. **Routes**: Add to `app/routes/`
3. **Templates**: Add to `app/templates/`
4. **Static files**: Add to `app/static/`

## Troubleshooting

### Database Connection Issues

**Error:** "Can't open connection"
- Verify ODBC DSN `STAAMP_DB` is configured
- Check Pervasive database is running
- Test connection with ODBC Data Source Administrator

### No Records Displayed

- Check date range (default is last 30 days)
- Verify NOTCOJAN table has recent data
- Check browser console for JavaScript errors
- Enable SQL logging to see queries

### Search Not Working

- Check JavaScript console for errors
- Verify `/linea/api/search` endpoint is accessible
- Clear browser cache and reload

## License

Internal use only - Staamp Production System

## Support

For issues or questions, contact the development team.
