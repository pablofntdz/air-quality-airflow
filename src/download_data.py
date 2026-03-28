import requests
from pathlib import Path
from datetime import datetime
import logging
import time
from config.urls import URLS
from config.paths import RAW_DIR , TMP_DIR
# Logging configuration
logging.basicConfig(level=logging.INFO,format="%(asctime)s - %(levelname)s - %(message)s")

logger = logging.getLogger(__name__)

# Helper functions
def setup_dirs():
    for subdir in ["air", "traffic", "meteo"]:
        (RAW_DIR / subdir).mkdir(parents=True, exist_ok=True)
        (TMP_DIR / subdir).mkdir(parents=True, exist_ok=True)

def get_timestamp() -> str:
    """
    Returns a timestamp string to include in file names.
    """
    return datetime.now().strftime("%Y%m%d_%H%M%S")


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


def save_raw_file(content: bytes, output_path: Path) -> None:
    """
    Saves the raw downloaded content to disk.
    """
    with open(output_path, "wb") as f:
        f.write(content)

def download_data(url,raw_path):
    logger.info(f"Downloading {url}")
    response = download_file(url)
    save_raw_file(response.content, raw_path)
    logger.info(f"Saved {raw_path}")

# Main
 
def main():
    setup_dirs()
    start_time = time.time()
    logger.info("Starting data ingestion pipeline")
    timestamp = get_timestamp()
    raw_meteo_path = RAW_DIR / "meteo" / f"measure_meteo_{timestamp}.json"
    raw_air_path = RAW_DIR / "air" / f"measure_air_{timestamp}.csv"
    raw_meteo_est_path = RAW_DIR / "meteo" / f"stations_meteo_{timestamp}.csv"
    raw_air_est_path = RAW_DIR / "air" / f"stations_air_{timestamp}.csv"
    raw_traffic_est_path = RAW_DIR / "traffic" / f"stations_traffic_{timestamp}.csv"
    raw_traffic_path = RAW_DIR / "traffic" / f"measure_traffic_{timestamp}.xml"

    download_data(URLS["air"],raw_air_path)
    download_data(URLS["air_est"],raw_air_est_path)
    download_data(URLS["meteo"],raw_meteo_path)
    download_data(URLS["meteo_est"],raw_meteo_est_path)
    download_data(URLS["traffic"],raw_traffic_path)
    download_data(URLS["traffic_est"],raw_traffic_est_path)
    logger.info("Download completed successfully.")
    end_time = time.time()
    elapsed = end_time - start_time

    logger.info(f"Tiempo de ejecución: {elapsed:.2f} segundos")

if __name__ == "__main__":
    main()