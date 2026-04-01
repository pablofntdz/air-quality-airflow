from pathlib import Path
from include.src.utils import get_latest
from include.config.paths import DATA_HOURLY_DIR
from include.config.paths import TMP_DIR
from include.src.transform_data import *
import pandas as pd
import shutil
from datetime import datetime 
import os
from io import BytesIO

from airflow.sdk import dag, task, Asset, get_current_context,Variable

MINIO_BUCKET = "aq-processed"

air_raw_data_asset = Asset("air_raw_data_ready")
meteo_raw_data_asset = Asset("meteo_raw_data_ready")
traffic_raw_data_asset = Asset("traffic_raw_data_ready")

final_data_asset = Asset("final_data_ready")

def make_path(base, subdir, filename):
    path = base / subdir / filename
    os.makedirs(path.parent, exist_ok=True)
    return str(path)

@dag(schedule=[air_raw_data_asset,meteo_raw_data_asset,traffic_raw_data_asset], start_date=datetime(2026, 4, 1), catchup=False,tags=["transform", "etl"], default_args={"retries": 2})
def ingestion_pipeline():
    @task
    def task_load_raw_data():
        timestamp = Variable.get("last_download_timestamp")
        context = get_current_context()
        logical_date = context["run_id"].replace(":","_").replace("+","_").replace(".","_")
        df_air, df_air_stations, df_traffic, df_traffic_est, df_meteo, df_est_meteo = load_raw_data(timestamp)
        base_path = TMP_DIR / f"{logical_date}"
        os.makedirs(base_path, exist_ok=True)
        paths = {
            "df_air":          make_path(base_path, "air",             "df_air.parquet"),
            "df_air_stations": make_path(base_path, "air_stations",    "df_air_stations.parquet"),
            "df_traffic":      make_path(base_path, "traffic",         "df_traffic.parquet"),
            "df_traffic_est":  make_path(base_path, "traffic_stations","df_traffic_est.parquet"),
            "df_meteo":        make_path(base_path, "meteo",           "df_meteo.parquet"),
            "df_est_meteo":    make_path(base_path, "meteo_stations",  "df_est_meteo.parquet"),
        }

        df_air.to_parquet(paths["df_air"], index=False)
        df_air_stations.to_parquet(paths["df_air_stations"], index=False)
        df_traffic.to_parquet(paths["df_traffic"], index=False)
        df_traffic_est.to_parquet(paths["df_traffic_est"], index=False)
        df_meteo.to_parquet(paths["df_meteo"], index=False)
        df_est_meteo.to_parquet(paths["df_est_meteo"], index=False)

        return paths
    @task
    def filter_traffic_task(paths):
        df_traffic = pd.read_parquet(paths["df_traffic"])
        df_traffic_est = pd.read_parquet(paths["df_traffic_est"])
        df_traffic = filter_traffic_df(df_traffic, df_traffic_est)
        context = get_current_context()
        print(dir(context))
        ds_nodash = context["run_id"].replace(":","_").replace("+","_").replace(".","_")
        base_path = TMP_DIR / f"{ds_nodash}/processed/traffic"
        os.makedirs(base_path, exist_ok=True)
        output_path = f"{base_path}/df_traffic_filtered.parquet"
        df_traffic.to_parquet(output_path, index=False)
        return output_path

    @task
    def filter_meteo_task(paths):
        df_est_meteo = pd.read_parquet(paths["df_est_meteo"])
        df_meteo = pd.read_parquet(paths["df_meteo"])
        df_meteo = filter_meteo_df(df_meteo, df_est_meteo)
        context = get_current_context()
        ds_nodash = context["run_id"].replace(":","_").replace("+","_").replace(".","_")
        base_path = TMP_DIR / f"{ds_nodash}/processed/meteo"
        os.makedirs(base_path, exist_ok=True)
        output_path = f"{base_path}/df_meteo_filtered.parquet"
        df_meteo.to_parquet(output_path, index=False)
        return output_path

    @task
    def filter_air_task(paths):
        df_air_stations = pd.read_parquet(paths["df_air_stations"])
        df_air = pd.read_parquet(paths["df_air"])
        df_air = filter_airquality_df(df_air, df_air_stations)
        context = get_current_context()
        ds_nodash = context["run_id"].replace(":","_").replace("+","_").replace(".","_")
        base_path = TMP_DIR / f"{ds_nodash}/processed/air"
        os.makedirs(base_path, exist_ok=True)
        output_path = f"{base_path}/df_air_filtered.parquet"
        df_air.to_parquet(output_path, index=False)
        return output_path
    @task
    def merge_air_meteo_task(meteo_path, air_path):
        df_meteo = pd.read_parquet(meteo_path)
        df_air = pd.read_parquet(air_path)
        df_air_meteo_dist = calculate_distance_KDTree_meteo(df_air, df_meteo)
        df_air_meteo = merge_air_meteo( df_air, df_air_meteo_dist,df_meteo )
        context = get_current_context()
        ds_nodash = context["run_id"].replace(":","_").replace("+","_").replace(".","_")
        base_path = TMP_DIR / f"{ds_nodash}/processed/air_meteo"
        os.makedirs(base_path, exist_ok=True)
        output_path = f"{base_path}/df_air_meteo_filtered.parquet"
        df_air_meteo.to_parquet(output_path, index=False)
        return output_path 
    @task
    def merge_air_traffic_task(df_traffic_path, df_air_path):
        df_traffic = pd.read_parquet(df_traffic_path)
        df_air = pd.read_parquet(df_air_path)
        df_dist = calculate_distance_KDTree_traffic(df_traffic, df_air)
        df_traffic_feat = calculate_feature_traffic(df_dist, df_traffic)
        context = get_current_context()
        ds_nodash = context["run_id"].replace(":","_").replace("+","_").replace(".","_")
        base_path = TMP_DIR / f"{ds_nodash}/processed/traffic"
        os.makedirs(base_path, exist_ok=True)
        output_path = f"{base_path}/df_traffic_feat.parquet"
        df_traffic_feat.to_parquet(output_path, index=False)
        return output_path
    
    @task(outlets=[final_data_asset])
    def merge_air_meteo_traffic_task(df_air_meteo_path, df_traffic_feat_path):
        df_air_meteo = pd.read_parquet(df_air_meteo_path)
        df_traffic_feat = pd.read_parquet(df_traffic_feat_path)

        df_final = merge_air_meteo_traffic(df_air_meteo, df_traffic_feat)
        df_final = df_final.drop(columns=["fecha"], errors="ignore")
        df_final = df_final.dropna(subset=["valor", "VV", "DV", "T", "HR", "PB", "P"],how="any")
        df_final['hour'] = df_final['timestamp'].dt.hour
        df_final['day'] = df_final['timestamp'].dt.day
        df_final['month'] = df_final['timestamp'].dt.month
        df_final['year'] = df_final['timestamp'].dt.year
        df_final["timestamp"] = pd.to_datetime(df_final["timestamp"])

        setup_bucket(MINIO_BUCKET)

        context = get_current_context()
        ds_nodash = context["run_id"].replace(":", "_").replace("+", "_").replace(".", "_")
        base_path = f"{ds_nodash}/final"
        output_path = f"{base_path}/df_final.parquet"
        buffer = BytesIO()
        df_final.to_parquet(buffer, index=False)
        buffer.seek(0)

        save_processed_file(buffer.getvalue(), MINIO_BUCKET, output_path)

        Variable.set("last_final_output_path", str(base_path))
        tmp_dir = TMP_DIR / ds_nodash
        shutil.rmtree(tmp_dir, ignore_errors=True)
        print("Pipeline finished -> dataset_final.csv")
    
    # Pipeline Ingestion Data
    paths = task_load_raw_data()
    traffic_path = filter_traffic_task(paths)
    meteo_path = filter_meteo_task(paths)
    air_path = filter_air_task(paths)
    df_air_meteo_path = merge_air_meteo_task(meteo_path, air_path)
    df_traffic_feat_path = merge_air_traffic_task(traffic_path, air_path)
    merge_air_meteo_traffic_task(df_air_meteo_path, df_traffic_feat_path)

ingestion_pipeline()
    