# LINEA - Quick Start Guide

## For Development

1. **Activate Virtual Environment**
   ```powershell
   .\.venv\Scripts\Activate.ps1
   ```

2. **Run Development Server**
   ```powershell
   python run.py
   ```

3. **Access Application**
   - Local: http://localhost:8084

---

## For Production (Windows Service)

### Prerequisites
- Python 3.11 installed
- ODBC DSN (STAAMP_DB) configured
- NSSM installed (for Windows service)
- Firewall configured

### Quick Commands

**Start Service:**
```powershell
Start-Service LINEA-App
```

**Stop Service:**
```powershell
Stop-Service LINEA-App
```

**Restart Service:**
```powershell
Restart-Service LINEA-App
```

**Check Status:**
```powershell
Get-Service LINEA-App
```

### Helper Scripts (Run as Administrator)

- `start-service.ps1` - Start the service
- `stop-service.ps1` - Stop the service
- `restart-service.ps1` - Restart the service
- `check-status.ps1` - Check service status and view logs

### Access URLs

- **Local:** http://localhost:8084
- **Network:** http://10.52.20.103:8084

---

## View Logs

```powershell
# Service output logs
Get-Content logs\service-output.log -Tail 50

# Service error logs
Get-Content logs\service-error.log -Tail 50

# Follow logs in real-time
Get-Content logs\service-output.log -Wait -Tail 20
```

---

## Troubleshooting

### Service won't start?
```powershell
# Check logs
Get-Content logs\service-error.log -Tail 50

# Try manual start
.\.venv\Scripts\Activate.ps1
$env:FLASK_CONFIG="production"
.\.venv\Scripts\gunicorn.exe --config gunicorn.conf.py run:app
```

### Can't access from network?
```powershell
# Check firewall
Get-NetFirewallRule -DisplayName "LINEA Application"

# Test connectivity
Test-NetConnection -ComputerName localhost -Port 8084
```

### Database connection issues?
```powershell
# Verify ODBC DSN
odbcad32.exe
# Look for: STAAMP_DB in System DSN tab
```

---

## Full Documentation

For complete deployment instructions, see:
- **DEPLOYMENT_GUIDE.md** - Complete deployment guide
- **README.md** - Application overview

---

## Important Files

- `.env` - Development environment configuration
- `.env.production` - Production environment configuration
- `gunicorn.conf.py` - Gunicorn server configuration
- `config.py` - Flask application configuration
- `run.py` - Application entry point

---

## Support

**Common Issues:**
1. Service won't start → Check logs in `logs\service-error.log`
2. Can't access from network → Check firewall rules
3. Database errors → Verify ODBC DSN configuration

**Configuration Files:**
- Production config: `.env.production`
- Gunicorn config: `gunicorn.conf.py`
- Flask config: `config.py`

**Log Files:**
- Service output: `logs\service-output.log`
- Service errors: `logs\service-error.log`
- Gunicorn access: `logs\access.log`
- Gunicorn errors: `logs\error.log`
