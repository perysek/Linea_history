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

## Step 8 — (Optional) Copy dev database to production server

Use this when you have been working on data locally (seeding test data, running
`migrate_excel_data.py` on the dev machine, etc.) and want the production server
to have the same records.

The dev database lives at:
```
C:\Users\piotrperesiak\PycharmProjects\LINEA_60\linea.db   (dev machine)
```

The production database lives at:
```
C:\linea-production\linea.db                                (server STMP-POL-QMS)
```

> **Note:** `linea.db` is excluded from Git (in `.gitignore`), so it is never
> pushed to GitHub. You must transfer it manually.

---

### Option A — Transfer via RDP (simplest)

1. Open an **RDP session** to `STMP-POL-QMS` (10.52.10.101)
2. In the RDP window, open the **Local Resources** tab → enable **Drives**
   so your dev machine's drives appear inside the RDP session
3. Inside the RDP session, open **File Explorer**
4. Navigate to your dev machine drive (e.g. `\\tsclient\C\`)
5. Copy the file:
   ```
   \\tsclient\C\Users\piotrperesiak\PycharmProjects\LINEA_60\linea.db
   ```
   → paste into:
   ```
   C:\linea-production\linea.db
   ```
   (confirm overwrite)

---

### Option B — PowerShell over the network (from dev machine)

Run this on your **dev machine** (PowerShell as Administrator):

```powershell
# Source: dev db
$src = "C:\Users\piotrperesiak\PycharmProjects\LINEA_60\linea.db"

# Destination: production server via UNC path
# Replace with the actual admin share path for STMP-POL-QMS:
$dst = "\\10.52.10.101\c$\linea-production\linea.db"

# Backup current production db first (on the server):
$date = Get-Date -Format "yyyyMMdd_HHmm"
Copy-Item $dst "\\10.52.10.101\c$\linea-backups\linea_before_devpush_$date.db"

# Copy dev db → production:
Copy-Item $src $dst -Force

Write-Host "Done. Production db replaced with dev db."
```

> Requires that `C$` admin share is accessible from your dev machine, and that
> you have admin credentials for `STMP-POL-QMS`.

---

### Option C — PowerShell Remoting (most reliable for automation)

Run this on your **dev machine**:

```powershell
$serverIP  = "10.52.10.101"
$localDb   = "C:\Users\piotrperesiak\PycharmProjects\LINEA_60\linea.db"
$remoteDir = "C:\linea-production"
$backupDir = "C:\linea-backups"

# Open a PS session to the server
$session = New-PSSession -ComputerName $serverIP -Credential (Get-Credential)

# 1. Backup current db on the server
$date = Get-Date -Format "yyyyMMdd_HHmm"
Invoke-Command -Session $session -ScriptBlock {
    param($src, $dst, $d)
    Copy-Item $src "$dst\linea_before_devpush_$d.db"
} -ArgumentList "$remoteDir\linea.db", $backupDir, $date

# 2. Push the dev db to the server
Copy-Item -Path $localDb -Destination "$remoteDir\linea.db" -ToSession $session -Force

# 3. Close session
Remove-PSSession $session

Write-Host "Dev db transferred to $serverIP"
```

---

### After any of the above options — run migrations on the server

The copied dev db may be behind if the server was not updated yet.
Always run migrations after replacing the db:

```powershell
# On the server (PowerShell as Admin):
cd C:\linea-production
.\venv\Scripts\Activate.ps1
$env:FLASK_APP    = "run.py"
$env:FLASK_CONFIG = "production"
flask db upgrade
```

This is safe to run even if the db is already up to date (`No upgrade required`).

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
[ ] 5. Copy dev linea.db to server  (if pushing local data — see Step 8)
[ ] 6. flask db upgrade
[ ] 7. Start-Service LINEA-App
[ ] 8. Verify: Get-Service LINEA-App  (Running)
[ ] 9. Verify: browser opens login page
```

---

## Quick copy-paste update script

Save this as `update.ps1` in `C:\linea-production\` for one-click updates.
Run it **on the server** (PowerShell as Administrator):

```powershell
# update.ps1 — LINEA update script
# Run as Administrator from C:\linea-production

param(
    # Path to dev linea.db on the dev machine (via UNC or mapped drive).
    # Leave empty to skip db copy and keep the existing production db.
    [string]$DevDb = ""
)

$ErrorActionPreference = "Stop"
$AppDir    = "C:\linea-production"
$BackupDir = "C:\linea-backups"

Write-Host "=== LINEA Update Script ===" -ForegroundColor Cyan

# 1. Stop service
Write-Host "`n[1/6] Stopping LINEA-App service..." -ForegroundColor Yellow
Stop-Service LINEA-App -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# 2. Backup current database
Write-Host "[2/6] Backing up database..." -ForegroundColor Yellow
$date = Get-Date -Format "yyyyMMdd_HHmm"
Copy-Item "$AppDir\linea.db" "$BackupDir\linea_$date.db"
Write-Host "      Backup: $BackupDir\linea_$date.db" -ForegroundColor Green

# 3. Pull latest code from GitHub
Write-Host "[3/6] Pulling latest code from GitHub..." -ForegroundColor Yellow
Set-Location $AppDir
git pull origin main

# 4. Install/update dependencies
Write-Host "[4/6] Installing dependencies..." -ForegroundColor Yellow
& "$AppDir\venv\Scripts\pip.exe" install -r "$AppDir\requirements.txt" --quiet

# 5. Copy dev db if provided
if ($DevDb -ne "") {
    if (Test-Path $DevDb) {
        Write-Host "[5/6] Copying dev database from: $DevDb" -ForegroundColor Yellow
        Copy-Item $DevDb "$AppDir\linea.db" -Force
        Write-Host "      Dev db copied." -ForegroundColor Green
    } else {
        Write-Host "[5/6] WARNING: DevDb path not found: $DevDb" -ForegroundColor Red
        Write-Host "      Keeping existing production database." -ForegroundColor DarkGray
    }
} else {
    Write-Host "[5/6] No DevDb specified — keeping existing production database." -ForegroundColor DarkGray
}

# 6. Run schema migrations (safe even if db is already up to date)
Write-Host "[6/6] Running database migrations..." -ForegroundColor Yellow
$env:FLASK_APP    = "run.py"
$env:FLASK_CONFIG = "production"
& "$AppDir\venv\Scripts\flask.exe" db upgrade

# Start service
Write-Host "`nStarting LINEA-App service..." -ForegroundColor Yellow
Start-Service LINEA-App
Start-Sleep -Seconds 3
$status = (Get-Service LINEA-App).Status
Write-Host "`n=== Done. Service status: $status ===" -ForegroundColor Cyan
```

**Usage:**

```powershell
# Code + migrations only (keep existing production data):
.\update.ps1

# Code + replace production db with dev db (via RDP drive mapping):
# (\\tsclient\C\ is your dev machine's C: drive inside the RDP session)
.\update.ps1 -DevDb "\\tsclient\C\Users\piotrperesiak\PycharmProjects\LINEA_60\linea.db"

# Code + replace production db with dev db (via network share):
.\update.ps1 -DevDb "\\10.52.10.xxx\c$\Users\piotrperesiak\PycharmProjects\LINEA_60\linea.db"
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
