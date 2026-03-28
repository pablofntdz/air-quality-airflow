# src/utils.py
from pathlib import Path

def get_latest(directory: Path, pattern: str) -> Path:
    files = sorted(directory.glob(pattern), key=lambda p: p.stat().st_mtime)
    if not files:
        raise FileNotFoundError(f"No hay ficheros en {directory} con patrón '{pattern}'")
    return files[-1]