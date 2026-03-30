import requests
from pathlib import Path
from datetime import datetime
import logging
import time
from datetime import datetime
from include.config.urls import URLS
from include.config.paths import RAW_DIR , TMP_DIR
from dateutil.relativedelta import relativedelta
from airflow.models import Variable
import s3fs
import boto3
import os
# Logging configuration
logging.basicConfig(level=logging.INFO,format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# MinIO config
MINIO_ACCESS_KEY = os.environ["MINIO_ROOT_USER"]
MINIO_SECRET_KEY = os.environ["MINIO_ROOT_PASSWORD"]
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_BUCKET = "aq-raw"


fs = s3fs.S3FileSystem(
    key=MINIO_ACCESS_KEY,
    secret=MINIO_SECRET_KEY,
    client_kwargs={"endpoint_url": MINIO_ENDPOINT},
)
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,  
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

# Helper functions
def setup_bucket():
    env = Variable.get("env")
    if not fs.exists(MINIO_BUCKET):
        fs.mkdir(MINIO_BUCKET)
        logger.info(f"Bucket creado: {MINIO_BUCKET}")

def build_raw_object_path(category: str, filename: str,timestamp : str) -> str:
    """
    Builds an S3/MinIO object path partitioned by date.
    """
    dt = datetime.strptime(timestamp, "%Y%m%d_%H%M%S")

    return (
        f"{category}/"
        f"year={dt:%Y}/month={dt:%m}/day={dt:%d}/"
        f"time={dt:%H%M}/"
        f"{filename}"
    )

def get_timestamp() -> str:
    """
    Returns a timestamp string to include in file names.
    """
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def get_years_to_download(months_back=6):
    """Devuelve los años únicos necesarios para cubrir los últimos N meses"""
    today = datetime.now()
    years = set()
    for i in range(1, months_back + 1):
        d = today - relativedelta(months=i)
        years.add(d.year)
    return list(years)

def download_file(url: str, headers: dict | None = None, retries: int = 3, delay: int = 5):
    """
    Downloads a file from a URL and checks if the request was successful.
    """
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"Intento {attempt} descargando: {url}")
            response = requests.get(url, headers=headers, timeout=60)
            response.raise_for_status()
            return response

        except requests.RequestException as e:
            logger.warning(f"Intento {attempt} fallido: {e}")

            if attempt == retries:
                logger.error(f"No se pudo descargar la URL tras {retries} intentos")
                raise

            time.sleep(delay)

def save_raw_file(content: bytes, bucket: str, object_name: str) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=object_name,
        Body=content
    )

def download_data(url, object_path):
    bucket = MINIO_BUCKET
    logger.info(f"Downloading {url}")
    response = download_file(url)
    save_raw_file(response.content, bucket, object_path)
    logger.info(f"Saved s3://{bucket}/{object_path}")
