import sys
import time
import urllib.request
import os
from datetime import datetime, timedelta
from urllib.error import URLError, HTTPError
import xml.etree.ElementTree as ET
import pandas as pd

base_dir = "data/Traffic"
inbox_dir = os.path.join(base_dir, "inbox")
staging_dir = os.path.join(base_dir, "staging")

urlToFetch = "https://informo.madrid.es/informo/tmadrid/pm.xml"

max_retries = 5
retry_delay = 20

os.makedirs(base_dir, exist_ok=True)
os.makedirs(inbox_dir, exist_ok=True)
os.makedirs(staging_dir, exist_ok=True)

def xml_url_to_csv(url, csv_file_path):
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/xml,text/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9",
        "Connection": "keep-alive",
    }
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as response:
        xml_data = response.read()

    root = ET.fromstring(xml_data)
    records = [{child.tag: child.text for child in pm} for pm in root.findall("pm")]
    df = pd.DataFrame(records)
    df.to_csv(csv_file_path, index=False, sep=";")

def descargar_una_vez():
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    final_csv = os.path.join(inbox_dir, f"trafico_{timestamp_str}.csv")
    tmp_csv = final_csv + ".tmp"

    for attempt in range(1, max_retries + 1):
        try:
            xml_url_to_csv(urlToFetch, tmp_csv)
            os.replace(tmp_csv, final_csv)  # move atómico
            print(f"[{timestamp_str}] CSV generado en: {final_csv}", flush=True)
            return True

        except HTTPError as e:
            if e.code == 403:
                print(f"[{timestamp_str}] 403 Forbidden. No reintento.", file=sys.stderr, flush=True)
                break
            print(f"[{timestamp_str}] HTTPError {e.code}: {e}. Intento {attempt}/{max_retries}", file=sys.stderr, flush=True)

        except URLError as e:
            print(f"[{timestamp_str}] URLError: {e}. Intento {attempt}/{max_retries}", file=sys.stderr, flush=True)

        except Exception as e:
            print(f"[{timestamp_str}] Error inesperado: {e}. Intento {attempt}/{max_retries}", file=sys.stderr, flush=True)

        if attempt < max_retries:
            time.sleep(retry_delay)

    if os.path.exists(tmp_csv):
        try:
            os.remove(tmp_csv)
        except Exception:
            pass
    return False


print("Downloader arrancado. Descarga inmediata...", flush=True)
descargar_una_vez()

while True:
    time.sleep(30 * 60)
    descargar_una_vez()