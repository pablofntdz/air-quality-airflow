import joblib
import numpy as np
import pandas as pd
#import matplotlib.pyplot as plt
from include.src.utils import get_latest
from include.config.models import MODELS
from include.config.paths import DATA_HOURLY_DIR,MODEL_DIR
import joblib
import os
MINIO_BUCKET = "aq-processed"
MINIO_ACCESS_KEY = os.environ["MINIO_ROOT_USER"]
MINIO_SECRET_KEY = os.environ["MINIO_ROOT_PASSWORD"]
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")

STORAGE_OPTIONS = {
    "key": MINIO_ACCESS_KEY,
    "secret": MINIO_SECRET_KEY, 
    "client_kwargs": {
        "endpoint_url": MINIO_ENDPOINT}
}
BASE_COLS_NON_NEGATIVE = ["valor", "VV", "DV", "HR", "PB", "P"]
LAG_PERIODS = [1, 2, 3, 4, 6, 12, 24]
ROLL_PERIODS = [3, 6, 12, 24]
FEATURES = [
    "VV",
    "DV",
    "T",
    "HR",
    "PB",
    "P",
    # "intensidad_w",
    # "ocupacion_w",
    # "carga_w",
    "hour_sin",
    "hour_cos",
    "month_sin",
    "month_cos",
    "weekend",
    "valor_lag1",
    "valor_lag3",
    "valor_lag12",
    "valor_lag24",
    "valor_roll3",
    "valor_roll12",
    "valor_roll24",
    "valor_std3",
    "valor_std6",
    "valor_std12",
    "valor_std24",
    "valor_diff1",
    "valor_diff24",
    "VV_lag1",
    "DV_lag1",
    "dow_sin",
    "dow_cos",
]

def load_data(path):
    path = f"s3://{MINIO_BUCKET}/{path}"
    df = pd.read_parquet(path,storage_options=STORAGE_OPTIONS)
    df = df.drop_duplicates(["estacion", "magnitud", "timestamp"]).copy()
    df = df[(df[BASE_COLS_NON_NEGATIVE] >= 0).all(axis=1)].copy()

    return df


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Time features
    df["day_of_week"] = df["timestamp"].dt.dayofweek
    df["weekend"] = (df["day_of_week"] >= 5).astype(int)

    df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)
    df["month_sin"] = np.sin(2 * np.pi * df["month"] / 12)
    df["month_cos"] = np.cos(2 * np.pi * df["month"] / 12)
    df["dow_sin"] = np.sin(2 * np.pi * df["day_of_week"] / 7)
    df["dow_cos"] = np.cos(2 * np.pi * df["day_of_week"] / 7)

    df = df.sort_values(["estacion", "magnitud", "timestamp"]).copy()

    group_keys = ["estacion", "magnitud"]
    valor_group = df.groupby(group_keys)["valor"]
    vv_group = df.groupby(group_keys)["VV"]
    dv_group = df.groupby(group_keys)["DV"]

    # Lag features
    for lag in LAG_PERIODS:
        df[f"valor_lag{lag}"] = valor_group.shift(lag)

    df["VV_lag1"] = vv_group.shift(1)
    df["DV_lag1"] = dv_group.shift(1)

    # Rolling features using past values only
    for window in ROLL_PERIODS:
        df[f"valor_roll{window}"] = valor_group.transform(
            lambda x: x.shift(1).rolling(window).mean()
        )
        df[f"valor_std{window}"] = valor_group.transform(
            lambda x: x.shift(1).rolling(window).std()
        )
 
    # Differences
    df["valor_diff1"] = df["valor_lag1"] - df["valor_lag2"]
    df["valor_diff24"] = df["valor_lag1"] - df["valor_lag24"]
    df = df.drop(columns=["intensidad_w","ocupacion_w","carga_w"],errors="ignore").copy()
    df = df.dropna().copy()
    return df


def evaluate_model(df: pd.DataFrame, estacion, magnitud):
    mask = (df["estacion"] == estacion) & (df["magnitud"] == magnitud)
    df_model = df.loc[mask].copy()

    if df_model.empty:
        print(f"[WARNING] No hay datos para estación={estacion}, magnitud={magnitud}")
        return

    #model = joblib.load(model_path)
    data = joblib.load( MODEL_DIR / "modelo_8_12.pkl")
    model = data["model"]

    X = df_model[FEATURES]
    y = df_model["valor"]
    
    y_pred = model.predict(X)
    print(y_pred,y)

def get_stations(df,magnitud_objetivo):
    col_magnitud = "magnitud"
    col_estacion = "estacion"
    df_mag = df[df[col_magnitud] == magnitud_objetivo].copy()
    estaciones = (df_mag[col_estacion].dropna().unique().tolist())
    return estaciones

def evaluate_model_stations(df,estacion,magnitud_objetivo): 
    target_col = "valor"
    col_estacion = "estacion"
    col_magnitud = "magnitud"
    col_estacion = "estacion"
    print(f"Total rows: {len(df)}")
    print(f"Magnitudes disponibles: {df[col_magnitud].unique()}")
    print(f"Estaciones disponibles: {df[col_estacion].unique()}")

    df_mag = df[df[col_magnitud] == magnitud_objetivo].copy()
    print(f"Rows after magnitud filter ({magnitud_objetivo}): {len(df_mag)}")

    df_station = df_mag[df_mag[col_estacion] == estacion].copy()
    print(f"Rows after estacion filter ({estacion}): {len(df_station)}")
    
    model_path = MODEL_DIR / f"rf_estacion_{estacion}.joblib"
    model = joblib.load(model_path)
    df_station = df_station.dropna(subset=[target_col])
    X = df_station[FEATURES]
    y = df_station[target_col]
    data = joblib.load(model_path)
    model = data["model"]
    y_pred = model.predict(X)
    df_result = df_station[["estacion", "timestamp"]].copy()
    df_result["y_real"] = y.values
    df_result["y_pred"] = y_pred
    df_result["magnitud"] = magnitud_objetivo
    df_result["created_at"] = pd.Timestamp.now()
    return df_result

def main():
    path = get_latest(DATA_HOURLY_DIR, "dataset_final_*.parquet")
    df = load_data(path)
    df = build_features(df)

    for model_cfg in MODELS:
        evaluate_model(
            df=df,
            estacion=model_cfg["estacion"],
            magnitud=model_cfg["magnitud"],
            model_path=model_cfg["model_path"],
        )


if __name__ == "__main__":
    main()