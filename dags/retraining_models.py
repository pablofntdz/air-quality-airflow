# dags/retrain_pipeline.py
from datetime import datetime
from pathlib import Path
import pandas as pd
import numpy as np
import os
import joblib

from airflow.sdk import dag, task, Asset, Variable
from include.config.paths import MODEL_DIR, TMP_DIR
from include.src.inference_data import load_data, build_features, get_stations
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

historical_data_asset = Asset("historical_data_ready")

STATIONS = [4, 8, 11, 16, 17, 27, 35, 36, 38, 39, 40, 47, 48, 50, 56, 57, 60]
MAGNITUD = 12
FEATURES = [
    "VV", "DV", "T", "HR", "PB", "P",
    "hour_sin", "hour_cos", "month_sin", "month_cos", "weekend",
    "valor_lag1", "valor_lag3", "valor_lag12", "valor_lag24",
    "valor_roll3", "valor_roll12", "valor_roll24",
    "valor_std3", "valor_std6", "valor_std12", "valor_std24",
    "valor_diff1", "valor_diff24",
    "VV_lag1", "DV_lag1", "dow_sin", "dow_cos"
]

@dag(
    schedule=[historical_data_asset],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["retrain", "ml"],
    default_args={"retries": 2}
)
def retrain_pipeline():

    @task
    def get_historical_path():
        return Variable.get("last_historical_output_path")

    @task
    def prepare_station_data(input_path):
        df = load_data(input_path)
        df = build_features(df)

        base_path = TMP_DIR / "retrain"
        os.makedirs(base_path, exist_ok=True)
        station_paths = {}

        for station in STATIONS:
            df_station = df[
                (df["estacion"] == station) & 
                (df["magnitud"] == MAGNITUD)
            ].copy()
            if df_station.empty:
                continue
            path = str(base_path / f"station_{station}.parquet")
            df_station.to_parquet(path, index=False)
            station_paths[str(station)] = path

        return station_paths

    @task(max_active_tis_per_dag=4)
    def retrain_station(station_id, station_paths):
        path = station_paths.get(str(station_id))
        if not path:
            print(f"No data for station {station_id}, skipping")
            return

        df = pd.read_parquet(path)
        df = df.dropna(subset=["valor"])

        if len(df) < 100:
            print(f"Not enough data for station {station_id}: {len(df)} rows")
            return

        X = df[FEATURES]
        y = df["valor"]

        split = int(len(df) * 0.8)
        X_train, X_test = X.iloc[:split], X.iloc[split:]
        y_train, y_test = y.iloc[:split], y.iloc[split:]

        model = RandomForestRegressor(
            n_estimators=200,
            random_state=42,
            n_jobs=-1
        )
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))

        model_path = MODEL_DIR / f"rf_estacion_{station_id}.joblib"
        joblib.dump({
            "model": model,
            "rmse": rmse,
        }, model_path)

        print(f"Station {station_id} retrained — RMSE: {rmse:.4f} — saved to {model_path}")

    @task
    def cleanup():
        import shutil
        shutil.rmtree(TMP_DIR / "retrain", ignore_errors=True)
        print("Cleanup done")

    # Flujo
    input_path = get_historical_path()
    station_paths = prepare_station_data(input_path)
    retrained = retrain_station.partial(station_paths=station_paths).expand(station_id=STATIONS)
    cleanup()

retrain_pipeline()