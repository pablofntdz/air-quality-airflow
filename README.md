# Air Quality Airflow

Data and inference pipeline for air quality monitoring in Madrid, orchestrated with **Apache Airflow** (Astro CLI), object storage on **MinIO**, and predictions powered by Random Forest models hosted on **HuggingFace**.

---

## Architecture

```
                        ┌─────────────┐
                        │ HuggingFace │  
                        └──────┬──────┘
                               │ auto download
                               ▼
┌──────────┐    DAGs    ┌─────────────┐    predictions    ┌──────────────┐
│  MinIO   │ ─────────► │   Airflow   │ ────────────────► │  PostgreSQL  │
│  (data)  │            │  (Astro CLI)│                   │  (results)   │
└──────────┘            └─────────────┘                   └──────────────┘
```

## Services

| Service | Description | Port |
|---|---|---|
| Airflow | DAG orchestration (Astro CLI) | 8080 |
| MinIO | Processed data storage | 9000 / 9001 |
| PostgreSQL | Predictions database | 5433 |

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

```bash
# External services (PostgreSQL + MinIO)
docker compose -f docker-compose.override.yml up -d

# Airflow via Astro CLI
astro dev start
```

### 4. Access the interfaces

| Interface | URL | Default credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| MinIO Console | http://localhost:9001 | see `.env` |

---

## 🤖 Models

Models are hosted on HuggingFace: [`pablofntdz/air-quality-scikit-learn`](https://huggingface.co/pablofntdz/air-quality-scikit-learn)

They are downloaded automatically the **first time** the `inference_pipeline` DAG runs. Subsequent runs reuse the models from the persistent volume without re-downloading.

Available models for stations:
`4, 8, 11, 16, 17, 27, 35, 36, 38, 39, 40, 47, 48, 50, 56, 57, 60`

---

## 🗂️ Project structure

```
air-quality-airflow/
├── dags/                        # Airflow DAGs
│   ├── inference_data.py        # Inference pipeline
│   ├── transform_data.py        # Data transformation
│   ├── historical_download_data.py
│   └── ingestion_data.py
├── include/
│   ├── config/
│   │   ├── paths.py             # Project paths
│   │   ├── models.py            # Model configuration
│   │   └── urls.py
│   └── src/
│       ├── inference_data.py    # Inference logic
│       ├── download_models.py   # HuggingFace model downloader
│       ├── transform_data.py
│       └── download_data.py
├── dashboard/
│   └── app.py                   # Results dashboard
├── tests/
├── Dockerfile
├── requirements.txt
├── docker-compose.override.yml  # PostgreSQL + MinIO + volumes
├── .env.example                 # Environment variables template
└── .gitignore
```

---

## 🔄 DAGs

| DAG | Description | Trigger |
|---|---|---|
| `inference_pipeline` | Downloads models and runs air quality inference per station | Asset `final_data_ready` |
| `transform_data` | Transforms and processes raw data | Scheduled |
| `historical_download_data` | Downloads historical data | Manual |
| `ingestion_data` | Data ingestion into MinIO | Scheduled |

---

## 🧪 Tests

```bash
astro dev pytest tests/
```