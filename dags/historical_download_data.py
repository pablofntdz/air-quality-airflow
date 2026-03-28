# dags/historical_download.py
from datetime import datetime
from pathlib import Path
import pandas as pd
import os
import shutil

from airflow.sdk import dag, task, Asset, get_current_context,Variable
from include.src.download_data import download_data, get_years_to_download
from include.src.transform_data import filter_airquality_df, filter_meteo_df, calculate_distance_KDTree_meteo, merge_air_meteo
from include.config.paths import TMP_DIR, DATA_HOURLY_DIR
from include.config.urls import AIR_URLS, METEO_URLS, URLS
from dateutil.relativedelta import relativedelta
from include.config.paths import RAW_DIR
from include.src.utils import get_latest
from dateutil.relativedelta import relativedelta

historical_data_asset = Asset("historical_data_ready")

def make_path(base, subdir, filename):
    path = Path(base) / subdir / filename
    os.makedirs(path.parent, exist_ok=True)
    return str(path)

@dag(
    schedule="0 0 1 * *",  # primer día de cada mes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["retrain", "historical"],
    default_args={"retries": 2}
)
def historical_download_pipeline():

    @task
    def get_years():
        return get_years_to_download(months_back=6)

    @task
    def download_air(years):
        run_id = get_current_context()["run_id"].replace(":", "_").replace("+", "_")
        base_path = TMP_DIR / f"historical_{run_id}" / "air"
        os.makedirs(base_path, exist_ok=True)
        paths = {}
        for year in years:
            url = AIR_URLS[year]
            path = str(base_path / f"air_{year}.csv")
            download_data(url, path)
            paths[str(year)] = path
        return paths

    @task
    def download_meteo(years):
        run_id = get_current_context()["run_id"].replace(":", "_").replace("+", "_")
        base_path = TMP_DIR / f"historical_{run_id}" / "meteo"
        os.makedirs(base_path, exist_ok=True)
        paths = {}
        for year in years:
            url = METEO_URLS[year]
            path = str(base_path / f"meteo_{year}.csv")
            download_data(url, path)
            paths[str(year)] = path
        return paths

    @task
    def process_air(air_paths, months_back=6):
        dfs = []
        for year, path in air_paths.items():
            df = pd.read_csv(path, sep=";")
            # reutilizamos filter_airquality_df pero sin el filtro de 48h
            # filtramos por fecha manualmente
            dfs.append(df)
        df_all = pd.concat(dfs, ignore_index=True)
        # guardamos el raw concatenado para que filter_airquality_df lo procese
        run_id = get_current_context()["run_id"].replace(":", "_").replace("+", "_")
        tmp_path = str(TMP_DIR / f"historical_{run_id}" / "air_concat.csv")
        os.makedirs(Path(tmp_path).parent, exist_ok=True)
        df_all.to_csv(tmp_path, sep=";", index=False)
        return tmp_path

    @task
    def process_meteo(meteo_paths):
        dfs = []
        for year, path in meteo_paths.items():
            df = pd.read_csv(path, sep=";")
            dfs.append(df)
        df_all = pd.concat(dfs, ignore_index=True)
        run_id = get_current_context()["run_id"].replace(":", "_").replace("+", "_")
        tmp_path = str(TMP_DIR / f"historical_{run_id}" / "meteo_concat.csv")
        os.makedirs(Path(tmp_path).parent, exist_ok=True)
        df_all.to_csv(tmp_path, sep=";", index=False)
        return tmp_path

    @task(outlets=[historical_data_asset])
    def build_historical_dataset(air_path, meteo_path):
        # leemos estaciones — usamos las más recientes
        tmp_air_stations = str(TMP_DIR / "stations_air_tmp.csv")
        tmp_meteo_stations = str(TMP_DIR / "stations_meteo_tmp.csv")
        download_data(URLS["air_est"], tmp_air_stations)
        download_data(URLS["meteo_est"], tmp_meteo_stations)

        df_air_stations = pd.read_csv(tmp_air_stations, sep=";")
        df_meteo_stations = pd.read_csv(tmp_meteo_stations, sep=";")

        df_air = pd.read_csv(air_path, sep=";")
        print(f"df_air dataset saved: {len(df_air)} rows")
        df_meteo = pd.read_csv(meteo_path, sep=";")
        print(f"df_meteo dataset saved: {len(df_meteo)} rows")
        df_air = filter_airquality_df(df_air, df_air_stations,True)
        print(f"df_air dataset saved: {len(df_meteo)} rows")
        df_meteo = filter_meteo_df(df_meteo, df_meteo_stations,True)
        print(f"df_meteo dataset saved: {len(df_meteo)} rows")

        df_dist = calculate_distance_KDTree_meteo(df_air, df_meteo)
        print(f"df_dist dataset saved: {len(df_dist)} rows")
        df_historical = merge_air_meteo(df_air, df_dist, df_meteo)
        print(f"df_historical dataset saved: {len(df_historical)} rows")

        # filtrar últimos 6 meses
        
        cutoff = datetime.now() - relativedelta(months=6)
        df_historical = df_historical[df_historical["timestamp"] >= cutoff]

        # añadir features temporales
        df_historical["hour"]  = df_historical["timestamp"].dt.hour
        df_historical["day"]   = df_historical["timestamp"].dt.day
        df_historical["month"] = df_historical["timestamp"].dt.month
        df_historical["year"]  = df_historical["timestamp"].dt.year

        output_path = str(DATA_HOURLY_DIR / "historical" / "dataset_historical.parquet")
        os.makedirs(Path(output_path).parent, exist_ok=True)
        df_historical.to_parquet(output_path, index=False)

        Variable.set("last_historical_output_path",output_path)

        # limpiar tmp
        run_id = get_current_context()["run_id"].replace(":", "_").replace("+", "_")
        shutil.rmtree(TMP_DIR / f"historical_{run_id}", ignore_errors=True)

        print(f"Historical dataset saved: {len(df_historical)} rows")
        return output_path

    # Flujo
    years = get_years()
    air_paths = download_air(years)
    meteo_paths = download_meteo(years)
    air_processed = process_air(air_paths)
    meteo_processed = process_meteo(meteo_paths)
    build_historical_dataset(air_processed, meteo_processed)

historical_download_pipeline()