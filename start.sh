#!/bin/bash

echo "Starting Airflow (Astro CLI)..."
astro dev start --no-browser

# Detecta la red _airflow de Astro (no la _default)
NETWORK=$(docker network ls --format "{{.Name}}" | grep "_airflow$" | head -1)
echo "Connecting minio and project_db to Astro network: $NETWORK"

docker network connect $NETWORK minio
docker network connect $NETWORK project_db

echo "Starting dashboard..."
docker compose -f "dashboard/docker-compose.yml" up -d

echo ""
echo "All services started and connected!"
echo "  Airflow UI  - http://localhost:8080"
echo "  MinIO       - http://localhost:9001"
echo "  Dashboard   - http://localhost:8501"