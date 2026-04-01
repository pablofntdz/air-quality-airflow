#!/bin/bash

# Comprueba que existe el .env
if [ ! -f ".env" ]; then
    echo "ERROR: .env file not found. Run: cp .env.example .env"
    exit 1
fi

echo "Starting Airflow (Astro CLI)..."
astro dev start --no-browser --wait 5m

# Detecta la red _airflow de Astro
NETWORK=$(docker network ls --format "{{.Name}}" | grep "_airflow$" | head -1)

if [ -z "$NETWORK" ]; then
    echo "ERROR: Astro network not found. Did astro dev start succeed?"
    exit 1
fi

echo "Connecting minio and project_db to Astro network: $NETWORK"
docker network connect $NETWORK minio
docker network connect $NETWORK project_db

echo "Starting dashboard..."
docker compose -f "dashboard/docker-compose.yml" up -d

echo "Waiting for dashboard container..."
sleep 5
docker network connect $NETWORK dashboard-streamlit

echo ""
echo "All services started and connected!"
echo "  Airflow UI  --> http://localhost:8080"
echo "  MinIO       --> http://localhost:9001"
echo "  Dashboard   --> http://localhost:8501"