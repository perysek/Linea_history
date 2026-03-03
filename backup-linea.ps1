# LINEA Database Backup Script
# Schedule via Task Scheduler: daily at 02:00 AM
#
# Usage: PowerShell.exe -File C:\linea-production\backup-linea.ps1

$AppDir    = "C:\linea-production"
$DbFile    = "$AppDir\linea.db"
$BackupDir = "C:\linea-backups"
$Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$BackupFile = "$BackupDir\linea_$Timestamp.db"
$RetainDays = 30

# Create backup directory if it doesn't exist
if (!(Test-Path $BackupDir)) {
    New-Item -ItemType Directory -Path $BackupDir -Force | Out-Null
}

# Only backup if database exists
if (!(Test-Path $DbFile)) {
    Write-Host "Database not found at $DbFile - skipping backup." -ForegroundColor Yellow
    exit 1
}

# Copy database file
try {
    Copy-Item $DbFile $BackupFile -ErrorAction Stop
    Write-Host "Backup created: $BackupFile" -ForegroundColor Green
} catch {
    Write-Host "Backup FAILED: $_" -ForegroundColor Red
    exit 1
}

# Remove backups older than $RetainDays days
$Cutoff = (Get-Date).AddDays(-$RetainDays)
Get-ChildItem $BackupDir -Filter "linea_*.db" |
    Where-Object { $_.LastWriteTime -lt $Cutoff } |
    ForEach-Object {
        Remove-Item $_.FullName -Force
        Write-Host "Removed old backup: $($_.Name)" -ForegroundColor Gray
    }

Write-Host "Backup complete. Retention: $RetainDays days." -ForegroundColor Cyan
