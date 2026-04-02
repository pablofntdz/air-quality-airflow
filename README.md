# Air Quality Airflow

Data and inference pipeline for air quality monitoring in Madrid, orchestrated with **Apache Airflow** (Astro CLI), object storage on **MinIO**, and predictions powered by Random Forest models hosted on **HuggingFace**.

---

## Architecture

```
                        ┌─────────────┐
                        │ HuggingFace │  .joblib models
                        └──────┬──────┘
                               │ auto download
                               ▼
┌──────────┐    DAGs    ┌─────────────┐    predictions    ┌──────────────┐
│  MinIO   │ ─────────► │   Airflow   │ ────────────────► │  PostgreSQL  │
│  (data)  │            │  (Astro CLI)│                   │  (results)   │
└──────────┘            └─────────────┘                   └──────────────┘
                                                                  │
                                                                  ▼
                                                          ┌──────────────┐
                                                          │  Dashboard   │
                                                          │  (Streamlit) │
                                                          └──────────────┘
```

## Services

| Service | Description | Port |
|---|---|---|
| Airflow | DAG orchestration (Astro CLI) | 8080 |
| MinIO | Processed data storage | 9000 / 9001 |
| PostgreSQL | Predictions database | 5433 |
| Dashboard | Streamlit visualization | 8501 |

---

## Installation

### Requirements

- [Docker](https://www.docker.com/)
- Python 3.11+

### 0. Install Astro CLI

**Mac:**
```bash
brew install astro
```

**Linux:**
```bash
curl -sSL install.astronomer.io | sudo bash -s
```

**Windows (PowerShell):**
```powershell
winget install -e --id Astronomer.Astro
```

Verify the installation:
```bash
astro version
```

### 1. Clone the repository

```bash
git clone https://github.com/pablofntdz/air-quality-airflow.git
cd air-quality-airflow
```

### 2. Set up environment variables

```bash
cp .env.example .env
```

Edit `.env` with your values (see `.env.example` for reference).

### 3. Start the services

Use the startup script — it launches Airflow, connects all containers to the same network, starts the dashboard, and configures nginx automatically.
 
**Windows:**
```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
./start.ps1
```
 
**Linux/Mac:**
```bash
chmod +x start.sh
./start.sh
```

### 4. Access the interfaces

| Interface | URL | Default credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| MinIO Console | http://localhost:9001 | see `.env` |

---

## Models

Models are hosted on HuggingFace: [`pablofntdz/air-quality-scikit-learn`](https://huggingface.co/pablofntdz/air-quality-scikit-learn)

They are downloaded automatically the **first time** the `inference_pipeline` DAG runs. Subsequent runs reuse the models from the persistent volume without re-downloading.

Available models for stations:
`4, 8, 11, 16, 17, 27, 35, 36, 38, 39, 40, 47, 48, 50, 56, 57, 60`

---

## Project structure

```
air-quality-airflow/
├── dags/
│   ├── inference_data.py           # Inference pipeline
│   ├── ingestion_data.py           # Data ingestion into MinIO
│   ├── transform_data.py           # Data transformation
│   ├── historical_download_data.py # Historical data download
│   └── retraining_models.py        # Model retraining
├── include/
│   ├── config/
│   │   ├── paths.py                # Project paths
│   │   ├── models.py               # Model configuration
│   │   └── urls.py                 # Data source URLs
│   └── src/
│       ├── inference_data.py       # Inference logic
│       ├── download_models.py      # HuggingFace model downloader
│       ├── transform_data.py       # Transformation logic
│       └── download_data.py        # Data download logic
├── dashboard/
│   ├── app.py                      # Streamlit dashboard
│   ├── Dockerfile
│   ├── requirements.txt
│   └── docker-compose.yml
├── tests/
├── Dockerfile
├── requirements.txt
├── docker-compose.override.yml     # PostgreSQL + MinIO
├── start.sh                        # Startup script (Linux/Mac)
├── start.ps1                       # Startup script (Windows)
├── .env.example                    # Environment variables template
└── .gitignore
```

---

## DAGs

| DAG | Description | Trigger |
|---|---|---|
| `download_pipeline` | Downloads raw air quality data | `@hourly` |
| `ingestion_pipeline` | Transforms and ingests data into MinIO | Asset `*_raw_data_ready` |
| `inference_pipeline` | Downloads models and runs NO₂ predictions per station | Asset `final_data_ready` |
| `historical_download_pipeline` | Downloads historical data for retraining | `0 0 1 * *` (monthly) |
| `retrain_pipeline` | Retrains Random Forest models per station | Asset `historical_data_ready` |

---
 
## Dashboard
 
The Streamlit dashboard connects to PostgreSQL and displays:
 
- Interactive map of Madrid monitoring stations
- Predicted vs real NO₂ values per station
- Anomaly detection based on prediction error
- Performance metrics (RMSE, MAE) per station
 
---
## Tests

```bash
astro dev pytest tests/
```