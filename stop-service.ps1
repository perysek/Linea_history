# Stop LINEA Application Service
# Run this script as Administrator

Write-Host "Stopping LINEA Application Service..." -ForegroundColor Yellow

try {
    Stop-Service LINEA-App
    Write-Host "Service stopped successfully!" -ForegroundColor Green

    # Check status
    $service = Get-Service LINEA-App
    Write-Host "Service Status: $($service.Status)" -ForegroundColor Cyan

} catch {
    Write-Host "Error stopping service: $_" -ForegroundColor Red
}

Write-Host "`nPress any key to exit..."
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
