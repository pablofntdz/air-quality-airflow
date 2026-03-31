# start.ps1 — Windows
Write-Host "Starting Airflow (Astro CLI)..."
astro dev start --no-browser

# Detecta la red _airflow de Astro (no la _default)
$NETWORK = docker network ls --format "{{.Name}}" | Select-String "_airflow$" | Select-Object -First 1
Write-Host "Connecting minio and project_db to Astro network: $NETWORK"

docker network connect $NETWORK minio
docker network connect $NETWORK project_db

Write-Host "Starting dashboard"
docker compose -f "dashboard/docker-compose.yml" up -d

Write-Host ""
Write-Host "All services started and connected!"
Write-Host "  Airflow UI  -- http://localhost:8080"
Write-Host "  MinIO       -- http://localhost:9001"
Write-Host "  Dashboard   -- http://localhost:8501"