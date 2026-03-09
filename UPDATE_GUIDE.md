# LINEA — Update & Migration Guide

How to pull code updates from GitHub, apply database schema migrations, and import data into `linea.db` on the production server (`STMP-POL-QMS` / `10.52.10.101`).

---

## Overview of the process

```
GitHub (main branch)
       |
       | git pull
       v
C:\linea-production\   ← code files
       |
       | flask db upgrade
       v
linea.db               ← schema updated
       |
       | python migrate_excel_data.py  (optional)
       v
linea.db               ← data imported
       |
       | Restart-Service LINEA-App
       v
App running with new code + schema
```

---

## Step 1 — RDP into the server

Connect to `STMP-POL-QMS` (10.52.10.101) via Remote Desktop.

Open **PowerShell as Administrator** for all commands below.

---

## Step 2 — Stop the service

Always stop the app before updating to avoid file locks.

```powershell
Stop-Service LINEA-App

# Confirm it stopped:
Get-Service LINEA-App
# Status should show: Stopped
```

---

## Step 3 — Back up the database

**Do this every time before touching the database.**

```powershell
# Quick manual backup (date-stamped copy):
$date = Get-Date -Format "yyyyMMdd_HHmm"
Copy-Item "C:\linea-production\linea.db" "C:\linea-backups\linea_$date.db"

# Verify the backup exists:
dir C:\linea-backups\ | Sort-Object LastWriteTime -Descending | Select-Object -First 3
```

---

## Step 4 — Pull the latest code

```powershell
cd C:\linea-production

git pull origin main
```

Expected output:
```
remote: Enumerating objects: ...
Updating abc1234..def5678
Fast-forward
 app/routes/auth.py   | 10 ++++
 requirements.txt     |  1 +
 ...
```

If git is not installed on the server, use Option B below.

### Option B — Manual file transfer (no git)

1. On your dev machine: create a ZIP of the project
   (exclude: `.venv/`, `__pycache__/`, `linea.db`, `*.pyc`)
2. Copy the ZIP to the server via RDP drag-drop or a shared network path
3. Extract over `C:\linea-production\` (overwrite existing files)
4. Do **not** overwrite `linea.db` — that is your live data

---

## Step 5 — Update Python dependencies

Only needed when `requirements.txt` changed (check `git diff HEAD~1 requirements.txt`).

```powershell
cd C:\linea-production
.\venv\Scripts\Activate.ps1

pip install -r requirements.txt

# Spot-check key packages:
python -c "import flask_login; print('Flask-Login OK')"
python -c "import flask_migrate; print('Flask-Migrate OK')"
```

If pip is slow on the server, add `--no-deps` only when you are certain no new transitive dependencies were added.

---

## Step 6 — Apply database schema migrations

Run this **every time** after a `git pull` that includes new migration files
(look for new files under `migrations/versions/` in the pull output).

```powershell
cd C:\linea-production
.\venv\Scripts\Activate.ps1

$env:FLASK_APP    = "run.py"
$env:FLASK_CONFIG = "production"

# Show pending migrations:
flask db current
flask db history --indicate-current

# Apply all pending migrations:
flask db upgrade
```

### What the output means

| Output | Meaning |
|--------|---------|
| `Running upgrade ... -> abc123` | Migration applied — schema changed |
| `INFO [alembic] No upgrade required` | Database is already up to date — nothing to do |
| `OperationalError: table already exists` | Migration out of sync — see Troubleshooting |

### How to confirm the migration worked

```powershell
python -c "
from app import create_app, db
from sqlalchemy import inspect
app = create_app('production')
with app.app_context():
    tables = inspect(db.engine).get_table_names()
    print('Tables:', tables)
"
```

For the RBAC migration specifically, you should see `roles` and `users` in the list.

---

## Step 7 — Verify the admin user was seeded

The app auto-creates a default `admin` user on first run when the `users` table is empty.

```powershell
python -c "
from app import create_app
app = create_app('production')
with app.app_context():
    from app.models.auth import User
    u = User.query.filter_by(username='admin').first()
    if u:
        print(f'Admin user OK: {u.username} / role: {u.role.name}')
    else:
        print('WARNING: No admin user found!')
"
```

If you see `WARNING: No admin user found!`, the table was already populated from a previous run (seed is no-op when any user exists). In that case the admin user needs to be created manually — see the Troubleshooting section.

**Change the default password immediately after first login** via the admin panel at `/auth/admin/users`.

---

## Step 8 — (Optional) Import data from Excel

Only run this when you need to import new sorting records from `PPM_wew.xlsm`.
The script detects and skips duplicates — it is safe to re-run.

```powershell
cd C:\linea-production
.\venv\Scripts\Activate.ps1

# Dry-run first (no changes, just shows what would be imported):
python migrate_excel_data.py --dry-run --from-date=2026-01-01

# If the preview looks correct, run for real:
python migrate_excel_data.py --from-date=2026-01-01
```

### Common options

| Flag | Purpose |
|------|---------|
| `--dry-run` | Preview without writing to db |
| `--quiet` | Suppress per-row output (faster) |
| `--from-date=YYYY-MM-DD` | Only import rows on/after this date |
| `--start-row=N` | Start from row N in the Excel file (resume interrupted import) |

### Excel file location

The app defaults to:
```
G:\DOCUMENT\qualita\Sistema Zarządzania Jakością\Cele jakościowe\PPM wewnętrzny koszty złej jakości (2023).xlsm
```

Override via environment variable if the path differs on the server:
```powershell
$env:EXCEL_FILE_PATH = "\\SERVER\share\PPM_wew.xlsm"
python migrate_excel_data.py --from-date=2026-01-01
```

---

## Step 9 — Restart the service and verify

```powershell
Start-Service LINEA-App

# Confirm it's running:
Get-Service LINEA-App
# Status: Running

# Check the startup log for errors:
Get-Content "C:\linea-production\logs\service-output.log" -Tail 30

# Quick HTTP test:
Invoke-WebRequest -Uri "http://localhost:8084/auth/login" -UseBasicParsing | Select-Object StatusCode
# Expected: 200
```

Open a browser on the server: `http://localhost:8084`
From a workstation: `http://10.52.10.101:8084`

---

## Step 10 — First login after RBAC deployment

The RBAC system was added in commit `afec4ad`. On the first startup after this update:

1. The app creates tables `roles` and `users` via migration
2. On startup it seeds: username `admin`, password `admin123`, role `Administrator` (superadmin)
3. Navigate to `http://10.52.10.101:8084` → redirected to login
4. Log in with `admin` / `admin123`
5. Immediately go to **Administracja → Użytkownicy** → edit `admin` → change password
6. Create user accounts for each team member with appropriate roles

---

## Full update checklist

```
[ ] 1. Stop-Service LINEA-App
[ ] 2. Backup database  (Copy-Item linea.db -> linea-backups\)
[ ] 3. git pull origin main
[ ] 4. pip install -r requirements.txt  (if requirements.txt changed)
[ ] 5. flask db upgrade
[ ] 6. python migrate_excel_data.py  (if new Excel data to import)
[ ] 7. Start-Service LINEA-App
[ ] 8. Verify: Get-Service LINEA-App  (Running)
[ ] 9. Verify: browser opens login page
```

---

## Quick copy-paste update script

Save this as `update.ps1` in `C:\linea-production\` for one-click updates:

```powershell
# update.ps1 — LINEA update script
# Run as Administrator from C:\linea-production

param(
    [switch]$SkipDataImport,
    [string]$FromDate = ""
)

$ErrorActionPreference = "Stop"
$AppDir = "C:\linea-production"
$BackupDir = "C:\linea-backups"

Write-Host "=== LINEA Update Script ===" -ForegroundColor Cyan

# 1. Stop service
Write-Host "`n[1/7] Stopping LINEA-App service..." -ForegroundColor Yellow
Stop-Service LINEA-App -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# 2. Backup database
Write-Host "[2/7] Backing up database..." -ForegroundColor Yellow
$date = Get-Date -Format "yyyyMMdd_HHmm"
Copy-Item "$AppDir\linea.db" "$BackupDir\linea_$date.db"
Write-Host "      Backup: $BackupDir\linea_$date.db" -ForegroundColor Green

# 3. Pull code
Write-Host "[3/7] Pulling latest code from GitHub..." -ForegroundColor Yellow
Set-Location $AppDir
git pull origin main

# 4. Install dependencies
Write-Host "[4/7] Installing dependencies..." -ForegroundColor Yellow
& "$AppDir\venv\Scripts\pip.exe" install -r "$AppDir\requirements.txt" --quiet

# 5. Run migrations
Write-Host "[5/7] Running database migrations..." -ForegroundColor Yellow
$env:FLASK_APP    = "run.py"
$env:FLASK_CONFIG = "production"
& "$AppDir\venv\Scripts\flask.exe" db upgrade

# 6. Optional data import
if (-not $SkipDataImport -and $FromDate -ne "") {
    Write-Host "[6/7] Importing Excel data from $FromDate..." -ForegroundColor Yellow
    & "$AppDir\venv\Scripts\python.exe" "$AppDir\migrate_excel_data.py" --from-date=$FromDate --quiet
} else {
    Write-Host "[6/7] Skipping Excel data import." -ForegroundColor DarkGray
}

# 7. Start service
Write-Host "[7/7] Starting LINEA-App service..." -ForegroundColor Yellow
Start-Service LINEA-App
Start-Sleep -Seconds 3
$status = (Get-Service LINEA-App).Status
Write-Host "`n=== Done. Service status: $status ===" -ForegroundColor Cyan
```

**Usage:**
```powershell
# Code + migrations only (no Excel import):
.\update.ps1 -SkipDataImport

# Code + migrations + import Excel records from 2026-01-01:
.\update.ps1 -FromDate "2026-01-01"
```

---

## Troubleshooting migrations

### "table already exists" error

```powershell
flask db current   # shows current migration head
flask db history   # shows all migrations

# If the DB has the table but alembic doesn't know it:
flask db stamp head   # mark the DB as up-to-date without running migrations
```

### Rolling back a migration

```powershell
flask db downgrade -1   # undo the last migration
# or go back to a specific revision:
flask db downgrade abc123
```

### Migration file missing

If the migration file exists in git but alembic doesn't see it:
```powershell
# List migration files:
dir migrations\versions\

# Run upgrade again (it auto-discovers .py files in versions/):
flask db upgrade
```

### Corrupted database

```powershell
Stop-Service LINEA-App

# Restore from backup:
$latest = Get-ChildItem C:\linea-backups\linea_*.db | Sort-Object LastWriteTime -Descending | Select-Object -First 1
Copy-Item $latest.FullName "C:\linea-production\linea.db"

Start-Service LINEA-App
```
