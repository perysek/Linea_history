# Check LINEA Application Status
# Run this script to check service status and view logs

Write-Host "=== LINEA Application Status ===" -ForegroundColor Cyan
Write-Host ""

# Check service status
try {
    $service = Get-Service LINEA-App -ErrorAction Stop
    Write-Host "Service Name: $($service.Name)" -ForegroundColor White
    Write-Host "Display Name: $($service.DisplayName)" -ForegroundColor White
    Write-Host "Status: $($service.Status)" -ForegroundColor $(if ($service.Status -eq 'Running') { 'Green' } else { 'Red' })
    Write-Host "Start Type: $($service.StartType)" -ForegroundColor White
    Write-Host ""
} catch {
    Write-Host "Service not installed or not accessible" -ForegroundColor Red
    Write-Host "Run installation steps from DEPLOYMENT_GUIDE.md" -ForegroundColor Yellow
    Write-Host ""
}

# Check if application is responding
Write-Host "Testing application connectivity..." -ForegroundColor Cyan
try {
    $response = Test-NetConnection -ComputerName localhost -Port 8084 -WarningAction SilentlyContinue
    if ($response.TcpTestSucceeded) {
        Write-Host "✓ Application is listening on port 8084" -ForegroundColor Green
    } else {
        Write-Host "✗ Application is not responding on port 8084" -ForegroundColor Red
    }
} catch {
    Write-Host "✗ Cannot test connectivity" -ForegroundColor Red
}
Write-Host ""

# Show access URLs
Write-Host "Access URLs:" -ForegroundColor Cyan
Write-Host "  Local:   http://localhost:8084" -ForegroundColor White
Write-Host "  Network: http://10.52.20.103:8084" -ForegroundColor White
Write-Host ""

# Show recent application logs
Write-Host "=== Recent Application Logs ===" -ForegroundColor Cyan
if (Test-Path "logs\service-output.log") {
    Get-Content "logs\service-output.log" -Tail 10
} else {
    Write-Host "No application logs found" -ForegroundColor Yellow
}
Write-Host ""

# Show recent errors
Write-Host "=== Recent Errors ===" -ForegroundColor Cyan
if (Test-Path "logs\service-error.log") {
    $errors = Get-Content "logs\service-error.log" -Tail 10
    if ($errors) {
        $errors | ForEach-Object { Write-Host $_ -ForegroundColor Red }
    } else {
        Write-Host "No errors found" -ForegroundColor Green
    }
} else {
    Write-Host "No error logs found" -ForegroundColor Green
}
Write-Host ""

Write-Host "Press any key to exit..."
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
