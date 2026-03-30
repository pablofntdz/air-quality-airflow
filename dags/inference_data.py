from pathlib import Path
from include.src.inference_data import load_data,build_features, MODELS,evaluate_model_stations,get_stations
import pandas as pd
from datetime import datetime
import os
from airflow.sdk import dag, task, Asset,Variable, get_current_context
import psycopg2
from psycopg2.extras import execute_batch
from airflow.providers.postgres.hooks.postgres import PostgresHook
from include.config.paths import TMP_DIR
import shutil

final_data_asset = Asset("final_data_ready")

def prepare_stations(base_path, stations):
    input_path = f"{base_path}/df_final.parquet"
    df = load_data(input_path)
    df = build_features(df)
    # carpeta temporal local
    base_path = TMP_DIR / "inference_stations"
    os.makedirs(base_path, exist_ok=True)
    station_paths = {}
    
    for station in stations:
        df_station = df[(df["estacion"] == station) & (df["magnitud"] == 12)].copy()
        if df_station.empty:
            continue
        path = str(base_path / f"station_{station}.parquet")
        df_station.to_parquet(path, index=False)
        station_paths[station] = path
    
    return station_paths

@dag( schedule=[final_data_asset], start_date=datetime(2024, 1, 1), catchup=False , tags=["inference","ml"], default_args={"retries": 2} )
def inference_pipeline():
    @task
    def get_input_path():
        return Variable.get("last_final_output_path")
    @task
    def prepare_data(input_path):
        stations = [4, 8, 11, 16, 17, 27, 35, 36, 38, 39, 40, 47, 48, 50, 56, 57, 60]
        return prepare_stations(input_path, stations)
    
    @task(max_active_tis_per_dag=4)
    def task_load_data(station_id, station_paths):
        path = station_paths.get(str(station_id))
        if not path:
            print(f"No data for station {station_id}, skipping")
            return
        df = pd.read_parquet(path)
        print(f"Rows after magnitud filter ({station_id}): {len(df)}")
        df_result = evaluate_model_stations(df, station_id, 12)
        print(f"Rows after estacion filter ({station_id}): {len(df_result)}")
        if df_result is None:
            return
        
        
        context = get_current_context()
        ds_nodash = context["run_id"].replace(":","_").replace("+","_").replace(".","_")
        base_tmp_path = TMP_DIR / f"{ds_nodash}/processed/traffic"
        os.makedirs(base_tmp_path, exist_ok=True)
        inference_path = str(base_tmp_path / f"station_{station_id}.parquet")
        df_result.to_parquet(inference_path, index=False)

        

        hook = PostgresHook(postgres_conn_id="project_db")
        conn = hook.get_conn()
        cur = conn.cursor()

        records = [
            (
                row["estacion"],
                row["timestamp"],
                row["y_real"],
                row["y_pred"],
                row["magnitud"],
                row["created_at"]
            )
            for _, row in df_result.iterrows()
        ]

        query = """
        INSERT INTO predictions (estacion, timestamp, y_real, y_pred, magnitud, created_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cur.execute("""
        CREATE TABLE IF NOT EXISTS predictions (
        estacion INTEGER,
        magnitud INTEGER,
        timestamp TIMESTAMP,
        created_at TIMESTAMP,
        y_real DOUBLE PRECISION,
        y_pred DOUBLE PRECISION
        )
        """)
        conn.commit()
        execute_batch(cur, query, records)

        conn.commit()
        cur.close()
        conn.close()

        shutil.rmtree(inference_path, ignore_errors=True)
        
        print(f"Saved {len(df_result)} predictions for station {station_id}")
    
    
    input_path = get_input_path()
    station_paths = prepare_data(input_path)
    stations = [4, 8, 11, 16, 17, 27, 35, 36, 38, 39, 40, 47, 48, 50, 56, 57, 60]
    task_load_data.partial(station_paths=station_paths).expand(station_id=stations)


inference_pipeline()