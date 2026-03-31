from huggingface_hub import hf_hub_download
from include.config.paths import MODEL_DIR
import shutil

REPO_ID = "pablofntdz/air-quality-scikit-learn"
STATIONS = [4, 8, 11, 16, 17, 27, 35, 36, 38, 39, 40, 47, 48, 50, 56, 57, 60]

def download_models_if_missing():
    MODEL_DIR.mkdir(parents=True, exist_ok=True)

    for station in STATIONS:
        filename = f"rf_estacion_{station}.joblib"   
        local_path = MODEL_DIR / filename

        if local_path.exists():
            print(f"[SKIP] {filename} ya existe.")
            continue

        print(f"[DOWNLOAD] Descargando {filename}...")
        tmp_path = hf_hub_download(
            repo_id=REPO_ID,
            filename=f"models/{filename}",
        )
        shutil.copy(tmp_path, local_path)
        print(f"[OK] {filename} → {local_path}")