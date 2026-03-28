import sys
import time
import urllib.request
import os
from datetime import datetime

#Path
urlToFetch = "https://datos.madrid.es/egob/catalogo/212531-10515086-calidad-aire-tiempo-real.csv"

base_dir = "data/AirQuality"
staging_dir = os.path.join(base_dir, "staging")
inbox_dir   = os.path.join(base_dir, "inbox")

os.makedirs(staging_dir, exist_ok=True)
os.makedirs(inbox_dir, exist_ok=True)

secondsToSleep = 60*60
max_retries = 10
retry_delay = 30


timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")

final_file = os.path.join(inbox_dir, f"calidad_aire_{timestamp_str}.csv")
tmp_file   = os.path.join(staging_dir, f"calidad_aire_{timestamp_str}.csv.tmp")

for attempt in range(1, max_retries + 1):
    try:
        urllib.request.urlretrieve(urlToFetch, tmp_file)
        os.replace(tmp_file, final_file)  # “publicación” atómica
        print(f"[{timestamp_str}] Archivo publicado en inbox: {final_file}")
        break
    except Exception as e:
        print(f"[{timestamp_str}] Error: {e}. Intento {attempt}/{max_retries}", file=sys.stderr)
        if attempt < max_retries:
            time.sleep(retry_delay)
        else:
            print(f"[{timestamp_str}] No se pudo descargar tras {max_retries} intentos.", file=sys.stderr)
            # si quedó tmp corrupto, lo intentamos borrar
            try:
                if os.path.exists(tmp_file):
                    os.remove(tmp_file)
            except:
                pass

time.sleep(secondsToSleep)