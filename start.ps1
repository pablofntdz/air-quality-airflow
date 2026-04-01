# start.ps1 — Windows
 
# Comprueba que existe el .env
if (-Not (Test-Path ".env")) {
    Write-Host "ERROR: .env file not found. Run: cp .env.example .env" -ForegroundColor Red
    exit 1
}
 
Write-Host "Starting Airflow (Astro CLI)..."
astro dev start --no-browser --wait 5m
 
# Detecta la red _airflow de Astro
$NETWORK = docker network ls --format "{{.Name}}" | Select-String "_airflow$" | Select-Object -First 1
 
if (-Not $NETWORK) {
    Write-Host "ERROR: Astro network not found. Did astro dev start succeed?" -ForegroundColor Red
    exit 1
}
 
Write-Host "Connecting minio and project_db to Astro network: $NETWORK"
docker network connect $NETWORK minio
docker network connect $NETWORK project_db
 
Write-Host "Starting dashboard..."
docker compose -f "dashboard/docker-compose.yml" up -d
 
Write-Host "Waiting for dashboard container..."
Start-Sleep -Seconds 10
docker network connect $NETWORK dashboard-streamlit
 
Write-Host ""
Write-Host "All services started and connected!" -ForegroundColor Green
Write-Host "  Airflow UI  -- http://localhost:8080"
Write-Host "  MinIO       -- http://localhost:9001"
Write-Host "  Dashboard   -- http://localhost:8501"