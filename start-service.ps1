# Start LINEA Application Service
# Run this script as Administrator

Write-Host "Starting LINEA Application Service..." -ForegroundColor Green

try {
    Start-Service LINEA-App
    Write-Host "Service started successfully!" -ForegroundColor Green

    # Wait a moment for service to initialize
    Start-Sleep -Seconds 3

    # Check status
    $service = Get-Service LINEA-App
    Write-Host "Service Status: $($service.Status)" -ForegroundColor Cyan

    # Show access URLs
    Write-Host "`nApplication is available at:" -ForegroundColor Yellow
    Write-Host "  Local:   http://localhost:8084" -ForegroundColor White
    Write-Host "  Network: http://10.52.20.103:8084" -ForegroundColor White

    # Show recent log entries
    Write-Host "`nRecent log entries:" -ForegroundColor Yellow
    Get-Content "logs\service-output.log" -Tail 10 -ErrorAction SilentlyContinue

} catch {
    Write-Host "Error starting service: $_" -ForegroundColor Red
    Write-Host "Check logs for details:" -ForegroundColor Yellow
    Write-Host "  logs\service-error.log"
}

Write-Host "`nPress any key to exit..."
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
