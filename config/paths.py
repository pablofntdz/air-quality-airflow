from pathlib import Path
import os
# Si corre en Docker usa la ruta de Airflow, si no usa la ruta local
if os.path.exists("/usr/local/airflow"):
    BASE_DIR = Path("/usr/local/airflow")
else:
    BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR  = BASE_DIR / "data2" / "raw"
TMP_DIR  = BASE_DIR / "data2" / "tmp"
MODEL_DIR  = BASE_DIR / "models"
DATA_HOURLY_DIR  = BASE_DIR / "data2" / "data_hourly"

