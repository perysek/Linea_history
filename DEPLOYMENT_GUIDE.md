# LINEA Production Deployment - Windows Local Network

## Overview

Deploy LINEA Flask web application on Windows for local network access using Python 3.11 and Gunicorn WSGI server.

**Benefits:**
- ✅ Fast deployment (30-60 minutes)
- ✅ Native Windows performance
- ✅ Easy troubleshooting
- ✅ Lower resource usage (~300 MB RAM)
- ✅ Secure local network access

**Architecture:**
```
Windows Server/PC
├── Python 3.11
├── Gunicorn WSGI Server
├── LINEA Flask Application (Windows Service)
└── Pervasive SQL Database (ODBC)
```

**Current Configuration:**
- Server IP: `10.52.20.103`
- Application Port: `8084`
- Database: Pervasive SQL via ODBC DSN (STAAMP_DB)

---

## Part 1: Prerequisites Installation

### Step 1: Install Python 3.11

1. **Download Python**
   - URL: https://www.python.org/downloads/
   - Version: Python 3.11.x (latest)
   - File: `python-3.11.x-amd64.exe`

2. **Install Python**
   - Run installer as Administrator
   - ✅ Check "Add Python to PATH"
   - ✅ Check "Install for all users"
   - Click "Install Now"

3. **Verify Installation**
   ```powershell
   python --version
   # Should show: Python 3.11.x

   pip --version
   # Should show pip version
   ```

### Step 2: Verify ODBC DSN Configuration

The LINEA application requires an ODBC DSN named `STAAMP_DB` to connect to Pervasive SQL.

1. **Open ODBC Data Source Administrator**
   ```powershell
   # For 64-bit systems
   odbcad32.exe
   ```

2. **Verify DSN Exists**
   - Go to "System DSN" tab
   - Look for: `STAAMP_DB`
   - If not found, create it according to your Pervasive SQL documentation

3. **Test Connection**
   - Select `STAAMP_DB`
   - Click "Configure"
   - Click "Test Connection"
   - Verify connection succeeds

### Step 3: Install Git (Optional but Recommended)

1. **Download Git for Windows**
   - URL: https://git-scm.com/download/win
   - Install with default settings

2. **Verify**
   ```powershell
   git --version
   ```

---

## Part 2: Application Setup

### Step 1: Navigate to Application Directory

```powershell
# Navigate to your application directory
cd C:\Users\piotrperesiak\PycharmProjects\LINEA_60
```

### Step 2: Create Virtual Environment

```powershell
# Create virtual environment
python -m venv .venv

# Activate virtual environment
.\.venv\Scripts\Activate.ps1

# Verify activation (should show (.venv) in prompt)
```

**Note:** If you encounter execution policy errors:
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### Step 3: Install Dependencies

```powershell
# Upgrade pip
python -m pip install --upgrade pip

# Install application dependencies
pip install -r requirements.txt

# Verify Gunicorn installation
gunicorn --version
```

### Step 4: Create Logs Directory

```powershell
# Create directory for logs
New-Item -ItemType Directory -Path "logs" -Force
```

---

## Part 3: Configuration

### Step 1: Review Production Environment File

The `.env.production` file has been created with the following settings:

```ini
# Flask Configuration
FLASK_CONFIG=production
FLASK_APP=run.py
SECRET_KEY=dd6ce4129e8ad5dc22752003a360503cd1c8c9ce8bb77c7d329e14e5714df3a5

# Database Configuration
# ODBC DSN: STAAMP_DB (configure via ODBC Data Source Administrator)

# Session Configuration
SESSION_COOKIE_SECURE=False
SESSION_COOKIE_HTTPONLY=True
SESSION_COOKIE_SAMESITE=Lax
PERMANENT_SESSION_LIFETIME=3600

# Gunicorn Configuration
GUNICORN_WORKERS=4
GUNICORN_LOG_LEVEL=info

# Server Configuration
SERVER_IP=10.52.20.103
SERVER_PORT=8084
```

### Step 2: Verify Server IP Address

```powershell
# Get your current IP address
ipconfig

# Look for IPv4 Address under your network adapter
# Update SERVER_IP in .env.production if different
```

### Step 3: Update Configuration (if needed)

```powershell
# Edit configuration if needed
notepad .env.production
```

**Important Settings to Review:**
- `SERVER_IP`: Must match your actual network IP
- `SERVER_PORT`: Default is 8084 (change if port is in use)
- `SECRET_KEY`: Already generated securely (do not share)

---

## Part 4: Test Manually Before Service Install

### Step 1: Test the Application

```powershell
# Make sure virtual environment is activated
.\.venv\Scripts\Activate.ps1

# Set environment to production
$env:FLASK_CONFIG="production"

# Test with Gunicorn
.\.venv\Scripts\gunicorn.exe --config gunicorn.conf.py run:app

# You should see:
# [INFO] Starting gunicorn ...
# [INFO] Listening at: http://0.0.0.0:8084
```

### Step 2: Test Access

1. **From This Computer:**
   - Open browser: `http://localhost:8084`
   - Should see LINEA application

2. **From Another Computer on Network:**
   - Open browser: `http://10.52.20.103:8084`
   - Should see LINEA application

3. **Stop Test Server:**
   - Press `Ctrl + C` in PowerShell

### Step 3: Troubleshoot Connection Issues

If you cannot access from another computer:

```powershell
# Test if port is listening
netstat -an | findstr "8084"
# Should show: 0.0.0.0:8084 LISTENING

# Test firewall (from another computer)
# telnet 10.52.20.103 8084
```

---

## Part 5: Install as Windows Service

### Step 1: Download NSSM (Non-Sucking Service Manager)

1. **Download NSSM**
   - URL: https://nssm.cc/download
   - Download latest version (e.g., nssm-2.24.zip)

2. **Extract NSSM**
   ```powershell
   # Create directory
   New-Item -ItemType Directory -Path "C:\nssm" -Force

   # Extract to C:\nssm
   Expand-Archive -Path "Downloads\nssm-2.24.zip" -DestinationPath "C:\nssm"
   ```

### Step 2: Install Service

```powershell
# Open PowerShell as Administrator

# Navigate to NSSM directory
cd C:\nssm\nssm-2.24\win64

# Install service with full paths
.\nssm.exe install LINEA-App "C:\Users\piotrperesiak\PycharmProjects\LINEA_60\.venv\Scripts\gunicorn.exe" "--config gunicorn.conf.py run:app"

# Set application directory
.\nssm.exe set LINEA-App AppDirectory "C:\Users\piotrperesiak\PycharmProjects\LINEA_60"

# Set environment variable for production
.\nssm.exe set LINEA-App AppEnvironmentExtra FLASK_CONFIG=production

# Set startup type to automatic
.\nssm.exe set LINEA-App Start SERVICE_AUTO_START

# Set description
.\nssm.exe set LINEA-App Description "LINEA Flask Web Application"

# Set output logs
.\nssm.exe set LINEA-App AppStdout "C:\Users\piotrperesiak\PycharmProjects\LINEA_60\logs\service-output.log"
.\nssm.exe set LINEA-App AppStderr "C:\Users\piotrperesiak\PycharmProjects\LINEA_60\logs\service-error.log"
```

### Step 3: Start Service

```powershell
# Start the service
Start-Service LINEA-App

# Verify it's running
Get-Service LINEA-App

# Check status should show: Running
```

### Step 4: Verify Application

```powershell
# Test from browser
# http://localhost:8084

# Check service logs
Get-Content "logs\service-output.log" -Tail 50
```

---

## Part 6: Configure Windows Firewall

### Allow Port 8084 on Local Network

```powershell
# Open PowerShell as Administrator

# Create firewall rule for local network access
New-NetFirewallRule -DisplayName "LINEA Application" `
    -Direction Inbound `
    -LocalPort 8084 `
    -Protocol TCP `
    -Action Allow `
    -Profile Private

# Verify rule
Get-NetFirewallRule -DisplayName "LINEA Application"
```

### Test Network Access

```powershell
# Test from this computer
Test-NetConnection -ComputerName localhost -Port 8084

# From another computer on the network, open browser:
# http://10.52.20.103:8084
```

---

## Part 7: Maintenance Procedures

### Service Management

```powershell
# Stop service
Stop-Service LINEA-App

# Start service
Start-Service LINEA-App

# Restart service
Restart-Service LINEA-App

# Check status
Get-Service LINEA-App

# View service configuration
C:\nssm\nssm-2.24\win64\nssm.exe edit LINEA-App
```

### Update Application

```powershell
# 1. Stop service
Stop-Service LINEA-App

# 2. Update code (if using Git)
cd C:\Users\piotrperesiak\PycharmProjects\LINEA_60
git pull origin main

# 3. Update dependencies (if requirements changed)
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt --upgrade

# 4. Start service
Start-Service LINEA-App

# 5. Verify
# Check logs and test access
Get-Content "logs\service-output.log" -Tail 50
```

### View Logs

```powershell
# Service logs
Get-Content "logs\service-output.log" -Tail 50
Get-Content "logs\service-error.log" -Tail 50

# Application logs
Get-Content "logs\error.log" -Tail 50

# Access logs
Get-Content "logs\access.log" -Tail 50

# Follow logs in real-time
Get-Content "logs\service-output.log" -Wait -Tail 50
```

### Monitor Service Performance

```powershell
# Check service status
Get-Service LINEA-App

# View process details
Get-Process -Name gunicorn | Format-List *

# Check memory usage
Get-Process -Name gunicorn | Select-Object Name, CPU, WorkingSet
```

---

## Part 8: Troubleshooting

### Service Won't Start

```powershell
# Check service status
Get-Service LINEA-App

# View service logs
Get-Content "logs\service-error.log" -Tail 100

# Try running manually to see errors
cd C:\Users\piotrperesiak\PycharmProjects\LINEA_60
.\.venv\Scripts\Activate.ps1
$env:FLASK_CONFIG="production"
.\.venv\Scripts\gunicorn.exe --config gunicorn.conf.py run:app
```

### Can't Access from Network

```powershell
# Test if service is listening
Test-NetConnection -ComputerName localhost -Port 8084

# Check firewall rules
Get-NetFirewallRule -DisplayName "LINEA Application"

# Verify IP address is correct
ipconfig

# Test from another computer
# ping 10.52.20.103
# telnet 10.52.20.103 8084
```

### Database Connection Errors

```powershell
# Verify ODBC DSN exists
odbcad32.exe
# Check System DSN tab for STAAMP_DB

# Test ODBC connection
# Use ODBC Data Source Administrator
# Select STAAMP_DB -> Configure -> Test Connection

# Check service logs for specific errors
Get-Content "logs\service-error.log" -Tail 100
```

### High Memory Usage

```powershell
# Check current memory usage
Get-Process -Name gunicorn | Select-Object Name, WorkingSet

# Reduce Gunicorn workers in .env.production
# Change: GUNICORN_WORKERS=4 → GUNICORN_WORKERS=2
notepad .env.production

# Restart service
Restart-Service LINEA-App
```

### Port Already in Use

```powershell
# Check what's using port 8084
netstat -ano | findstr "8084"

# Change port in .env.production
notepad .env.production
# Update SERVER_PORT=8085 (or another free port)

# Update firewall rule for new port
New-NetFirewallRule -DisplayName "LINEA Application" `
    -Direction Inbound `
    -LocalPort 8085 `
    -Protocol TCP `
    -Action Allow `
    -Profile Private

# Restart service
Restart-Service LINEA-App
```

---

## Part 9: Security Considerations

### Network Security

1. **Firewall Configuration**
   - Only allow access from trusted network (Private profile)
   - Never expose to Public networks without additional security

2. **HTTPS/SSL Setup**
   - For production, consider adding NGINX reverse proxy with SSL
   - See "External Access Setup" section in original guide

3. **Application Security**
   - SECRET_KEY is already generated and secured
   - Keep `.env.production` file protected
   - Never commit `.env.production` to Git

### Access Control

```powershell
# Verify .env.production is not in Git
git status

# Should be listed in .gitignore
Get-Content .gitignore | Select-String "env"
```

---

## Production Checklist

### Pre-Deployment
- [ ] Python 3.11 installed
- [ ] ODBC DSN (STAAMP_DB) configured and tested
- [ ] Virtual environment created in `.venv`
- [ ] Dependencies installed from requirements.txt
- [ ] `.env.production` reviewed and configured
- [ ] Logs directory created
- [ ] Application tested manually

### Deployment
- [ ] NSSM downloaded and extracted
- [ ] Windows Service installed with NSSM
- [ ] Service configured to start automatically
- [ ] Service started successfully
- [ ] Application accessible on `http://localhost:8084`
- [ ] Firewall rule created and active
- [ ] Accessible from other computers on network

### Post-Deployment
- [ ] Logs directory monitored (no errors)
- [ ] Team trained on basic operations
- [ ] User access tested from multiple computers
- [ ] Database connectivity verified
- [ ] Documentation updated with actual IP and port

---

## Quick Reference

### File Locations
- Application: `C:\Users\piotrperesiak\PycharmProjects\LINEA_60`
- Logs: `C:\Users\piotrperesiak\PycharmProjects\LINEA_60\logs\`
- Virtual Environment: `C:\Users\piotrperesiak\PycharmProjects\LINEA_60\.venv`
- Configuration: `C:\Users\piotrperesiak\PycharmProjects\LINEA_60\.env.production`

### Service Commands
```powershell
Start-Service LINEA-App      # Start
Stop-Service LINEA-App       # Stop
Restart-Service LINEA-App    # Restart
Get-Service LINEA-App        # Status
```

### Access URLs
- Local: `http://localhost:8084`
- Network: `http://10.52.20.103:8084`
- Other computers: `http://10.52.20.103:8084`

### Important Ports
- Application: `8084`
- Pervasive SQL: (configured via ODBC)

### Key Configuration Files
- `.env.production` - Production environment variables
- `gunicorn.conf.py` - Gunicorn server configuration
- `config.py` - Flask application configuration
- `run.py` - Application entry point

---

## Success!

Your LINEA application is now running as a production Windows Service accessible on your local network!

**Next Steps:**
1. Test access from all required computers on the network
2. Train users on accessing the application
3. Set up monitoring for logs
4. Plan regular maintenance windows
5. Document any custom workflows or configurations

**Support:**
- Application logs: `logs\` directory
- Service logs: Windows Event Viewer → Application
- Network issues: Check firewall and IP configuration
- Database issues: Verify ODBC DSN configuration

**Common Access URL Format:**
```
http://<SERVER-IP>:8084
Example: http://10.52.20.103:8084
```

---

## Appendix: Network Discovery

### Find Your Server IP for Access

On the server computer:
```powershell
ipconfig | Select-String "IPv4"
```

### Allow Network Discovery (Optional)

If computers cannot find the server:
```powershell
# Enable network discovery (as Administrator)
netsh advfirewall firewall set rule group="Network Discovery" new enable=Yes
```

### Test Network Connectivity

From client computer:
```powershell
# Test ping
ping 10.52.20.103

# Test port
Test-NetConnection -ComputerName 10.52.20.103 -Port 8084
```

---

**Document Version:** 1.0
**Last Updated:** 2026-02-10
**Application:** LINEA Flask Web Application
**Server:** Windows (Development/Production)
