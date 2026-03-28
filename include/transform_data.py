from include.src.utils import get_latest
from config.paths import RAW_DIR,DATA_HOURLY_DIR
import pandas as pd
import numpy as np
from pathlib import Path
from pyproj import Transformer
from scipy.spatial import cKDTree
import json


transformer = Transformer.from_crs("EPSG:4326", "EPSG:25830", always_xy=True)


# ------------------------------------------------------------
# Utils
# ------------------------------------------------------------

def load_raw_data():
    raw_air = get_latest(RAW_DIR / "air", "measure_*.csv")
    raw_meteo = get_latest(RAW_DIR / "meteo", "measure_*.json")
    raw_traffic = get_latest(RAW_DIR / "traffic", "measure_*.xml")
    raw_air_station = get_latest(RAW_DIR / "air", "stations_*.csv")
    raw_meteo_station = get_latest(RAW_DIR / "meteo", "stations_*.csv")
    raw_traffic_station = get_latest(RAW_DIR / "traffic", "stations_*.csv")
    df_air = pd.read_csv(raw_air,sep=";")
    df_air_stations = pd.read_csv(raw_air_station,sep=";")
    df_traffic = pd.read_xml(raw_traffic)
    df_traffic["fecha"] = pd.to_datetime(df_traffic.loc[0,"fecha_hora"],format="%d/%m/%Y %H:%M:%S").floor("h")
    df_traffic = df_traffic.drop(["fecha_hora"],axis=1)
    df_traffic = df_traffic.drop(0,axis=0).reset_index(drop=True)
    df_traffic_est = pd.read_csv(raw_traffic_station,sep=";",encoding="utf-8",encoding_errors="ignore")
    with open(raw_meteo) as f:
        data = json.load(f)          
    df_meteo = pd.DataFrame(data["records"])  
    df_est_meteo = pd.read_csv(raw_meteo_station, sep=";")
    return df_air,df_air_stations,df_traffic,df_traffic_est,df_meteo,df_est_meteo

def transform_coordinates(df, lon_col, lat_col, x_col="st_x", y_col="st_y"):
    lon = pd.to_numeric(df[lon_col].astype(str).str.replace(",", ".", regex=False), errors="coerce")
    lat = pd.to_numeric(df[lat_col].astype(str).str.replace(",", ".", regex=False), errors="coerce")

    stx, sty = transformer.transform(lon.to_numpy(), lat.to_numpy())

    df[lon_col] = lon
    df[lat_col] = lat
    df[x_col] = stx
    df[y_col] = sty
    return df


def melt_hours_with_validation(df, id_vars):
    h_cols = [f"H{i:02d}" for i in range(1, 25)]
    v_cols = [f"V{i:02d}" for i in range(1, 25)]

    df_h = df.melt(
        id_vars=id_vars,
        value_vars=h_cols,
        var_name="hora",
        value_name="valor"
    )

    df_v = df.melt(
        id_vars=id_vars,
        value_vars=v_cols,
        var_name="hora_validacion",
        value_name="validacion"
    )

    df_h["hora"] = df_h["hora"].str[1:].astype(int)
    df_v["hora_validacion"] = df_v["hora_validacion"].str[1:].astype(int)

    join_keys = id_vars.copy()

    df_long = pd.merge(
        df_h,
        df_v[join_keys + ["hora_validacion", "validacion"]],
        left_on=join_keys + ["hora"],
        right_on=join_keys + ["hora_validacion"],
        how="inner"
    )

    df_long = df_long.drop(columns=["hora_validacion"])
    return df_long


# ------------------------------------------------------------
# Traffic
# ------------------------------------------------------------
def filter_traffic_df(df_traffic, df_traffic_est):
    df = pd.merge(df_traffic, df_traffic_est, left_on="idelem",right_on="id", how="left")

    cols = [
        "id", "cod_cent", "fecha", "intensidad",
        "ocupacion", "carga", "longitud", "latitud"
    ]
    df = df[cols].copy()

    df["fecha_hora"] = pd.to_datetime(df["fecha"], errors="coerce")
    df["intensidad"] = pd.to_numeric(df["intensidad"], errors="coerce")
    df["ocupacion"] = pd.to_numeric(df["ocupacion"], errors="coerce")
    df["carga"] = pd.to_numeric(df["carga"], errors="coerce")

    df = transform_coordinates(df, "longitud", "latitud")
    return df


# ------------------------------------------------------------
# Air Quality
# ------------------------------------------------------------
def filter_airquality_df(df_air, df_air_stations):
    id_vars = ["PROVINCIA", "MUNICIPIO", "ESTACION", "MAGNITUD", "PUNTO_MUESTREO", "ANO", "MES", "DIA"]
    df_long = melt_hours_with_validation(df_air, id_vars=id_vars)
    df_long = df_long[df_long["validacion"] == "V"].copy()
    

    df = df_long.drop(["validacion", "PUNTO_MUESTREO", "PROVINCIA", "MUNICIPIO"], axis=1)

    df_air_stations = df_air_stations.rename(columns={"ESTACION": "nom_estacion"})
    df_air_stations = df_air_stations.drop(["DIRECCION", "LONGITUD_ETRS89", "LATITUD_ETRS89", "COD_TIPO", "NOM_TIPO","NO2","SO2","CO","PM10","PM2_5","O3","BTX","Fecha alta"],
        axis=1,
        errors="ignore"
    )
    df = pd.merge(
        df,
        df_air_stations,
        left_on="ESTACION",
        right_on="CODIGO_CORTO",
        how="left"
    )

    df = df.rename(columns={
        "ESTACION": "estacion",
        "MAGNITUD": "magnitud",
        "ANO": "ano",
        "MES": "mes",
        "DIA": "dia",
        "ALTITUD": "altitud",
        "LONGITUD": "longitud",
        "LATITUD": "latitud"
    })

    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    df["magnitud"] = pd.to_numeric(df["magnitud"], errors="coerce")
    df["ano"] = pd.to_numeric(df["ano"], errors="coerce")
    df["mes"] = pd.to_numeric(df["mes"], errors="coerce")
    df["dia"] = pd.to_numeric(df["dia"], errors="coerce")
    df["hora"] = pd.to_numeric(df["hora"], errors="coerce")

    df = transform_coordinates(df, "longitud", "latitud")

    df["timestamp"] = pd.to_datetime(
        {
            "year": df["ano"],
            "month": df["mes"],
            "day": df["dia"],
            "hour": df["hora"],
        },
        errors="coerce"
    )
    df = df.drop(["ano", "mes", "dia", "hora"], axis=1)
    df = df[df["timestamp"] >= pd.Timestamp.now() - pd.Timedelta(hours=48)]
    return df


# ------------------------------------------------------------
# Meteo
# ------------------------------------------------------------
def filter_meteo_df(df_meteo, df_est):
    id_vars = ["PROVINCIA", "MUNICIPIO", "ESTACION", "MAGNITUD", "ANO", "MES", "DIA"]

    df_long = melt_hours_with_validation(df_meteo, id_vars=id_vars)
    df_long = df_long[df_long["validacion"] == "V"].copy()
    df_meteo = df_long.drop(["validacion", "PROVINCIA", "MUNICIPIO"], axis=1)

    cols_keep = [
        "CÓDIGO_CORTO", "ESTACION", "ALTITUD",
        "VV (81)", "DV (82)", "T (83)", "HR (86)", "PB (87)", "P (89)",
        "LONGITUD", "LATITUD"
    ]
    df_est = df_est[cols_keep].copy()

    cols_flags = ["VV (81)", "DV (82)", "T (83)", "HR (86)", "PB (87)", "P (89)"]
    df_est = df_est[df_est[cols_flags].eq("X").all(axis=1)].copy()

    df_est["CÓDIGO_CORTO"] = pd.to_numeric(df_est["CÓDIGO_CORTO"], errors="coerce")
    df_meteo["ESTACION"] = pd.to_numeric(df_meteo["ESTACION"], errors="coerce")
    df_meteo["MAGNITUD"] = pd.to_numeric(df_meteo["MAGNITUD"], errors="coerce")
    df_meteo["valor"] = pd.to_numeric(df_meteo["valor"], errors="coerce")

    estaciones_validas = df_est["CÓDIGO_CORTO"].dropna().astype(int).unique()
    df_meteo = df_meteo[df_meteo["ESTACION"].isin(estaciones_validas)].copy()

    df_meteo = df_meteo[df_meteo["MAGNITUD"].isin([81, 82, 83, 86, 87, 89])].copy()

    df_meteo = (
        df_meteo
        .pivot_table(
            index=["ESTACION", "ANO", "MES", "DIA", "hora"],
            columns="MAGNITUD",
            values="valor",
            aggfunc="first"
        )
        .reset_index()
    )

    df_meteo = df_meteo.rename(columns={
        81: "VV",
        82: "DV",
        83: "T",
        86: "HR",
        87: "PB",
        89: "P"
    })

    df_est = df_est.drop(cols_flags, axis=1)
    df_est = df_est.rename(columns={
        "ESTACION": "nom_estacion_meteo",
        "ALTITUD": "altitud_meteo",
        "LONGITUD": "lon_meteo",
        "LATITUD": "lat_meteo"
    })

    df_est = transform_coordinates(
        df_est,
        lon_col="lon_meteo",
        lat_col="lat_meteo",
        x_col="st_x_meteo",
        y_col="st_y_meteo"
    )

    df_meteo = pd.merge(
        df_meteo,
        df_est,
        left_on="ESTACION",
        right_on="CÓDIGO_CORTO",
        how="left"
    )

    df_meteo = df_meteo.drop(columns=["CÓDIGO_CORTO"], errors="ignore")

    df_meteo = df_meteo.rename(columns={
        "ESTACION": "estacion_meteo",
        "ANO": "ano",
        "MES": "mes",
        "DIA": "dia"
    })

    df_meteo["timestamp_meteo"] = pd.to_datetime(
        {
            "year": df_meteo["ano"],
            "month": df_meteo["mes"],
            "day": df_meteo["dia"],
            "hour": df_meteo["hora"],
        },
        errors="coerce"
    )

    df_meteo = df_meteo.drop(["ano", "mes", "dia", "hora"], axis=1, errors="ignore")
    df_meteo = df_meteo[df_meteo["timestamp_meteo"] >= pd.Timestamp.now() - pd.Timedelta(hours=48)]
    
    return df_meteo


# ------------------------------------------------------------
# Spatial join traffic-air
# ------------------------------------------------------------
def calculate_distance_KDTree_traffic(df_traffic, df_air):
    df_traf = df_traffic[["id", "st_x", "st_y"]].dropna().drop_duplicates().reset_index(drop=True)
    df_air_loc = df_air[["estacion", "st_x", "st_y"]].dropna().drop_duplicates().reset_index(drop=True)

    tree = cKDTree(df_traf[["st_x", "st_y"]].values)
    idx_400 = tree.query_ball_point(df_air_loc[["st_x", "st_y"]], r=400)

    results = []

    for i, traf_idxs in enumerate(idx_400):
        if len(traf_idxs) == 0:
            continue

        station = df_air_loc.iloc[i]
        traf = df_traf.iloc[traf_idxs].copy()

        traf["dist_m"] = np.sqrt(
            (traf["st_x"] - station["st_x"]) ** 2 +
            (traf["st_y"] - station["st_y"]) ** 2
        )

        traf["estacion"] = station["estacion"]
        traf["buffer_100"] = traf["dist_m"] <= 100
        traf["buffer_200"] = traf["dist_m"] <= 200
        traf["buffer_300"] = traf["dist_m"] <= 300

        results.append(
            traf[["estacion", "id", "dist_m", "buffer_100", "buffer_200", "buffer_300"]]
        )

    if results:
        return pd.concat(results, ignore_index=True)

    return pd.DataFrame(columns=["estacion", "id", "dist_m", "buffer_100", "buffer_200", "buffer_300"])


# ------------------------------------------------------------
# Traffic features
# ------------------------------------------------------------
def calculate_feature_traffic(df_dist, df_traffic):
    buffers = ["buffer_100", "buffer_200", "buffer_300"]
    df_dist = df_dist[df_dist[buffers].any(axis=1)].copy()

    df = pd.merge(df_traffic, df_dist, on="id", how="left")
    df = df.dropna(subset=["estacion"]).copy()

    df["peso"] = np.exp(-df["dist_m"] / 150)
    df["intensidad_w"] = df["intensidad"] * df["peso"]
    df["ocupacion_w"] = df["ocupacion"] * df["peso"]
    df["carga_w"] = df["carga"] * df["peso"]

    df_out = (
        df.groupby(["estacion", "fecha"])
        .agg({
            "intensidad_w": "sum",
            "ocupacion_w": "sum",
            "carga_w": "sum"
        })
        .reset_index()
    )

    return df_out


# ------------------------------------------------------------
# Spatial join air-meteo
# ------------------------------------------------------------
def calculate_distance_KDTree_meteo(df_air, df_meteo):
    df_air_loc = (
        df_air[["estacion", "nom_estacion", "longitud", "latitud", "st_x", "st_y"]]
        .drop_duplicates()
        .dropna(subset=["st_x", "st_y"])
        .reset_index(drop=True)
    )

    df_met = (
        df_meteo[["estacion_meteo", "st_x_meteo", "st_y_meteo"]]
        .drop_duplicates()
        .dropna(subset=["st_x_meteo", "st_y_meteo"])
        .reset_index(drop=True)
    )

    tree = cKDTree(df_met[["st_x_meteo", "st_y_meteo"]].to_numpy())
    distancias, indices = tree.query(df_air_loc[["st_x", "st_y"]].to_numpy(), k=1)

    df_air_loc["id_station_meteo_near"] = indices
    df_air_loc["estacion_meteo"] = df_met.iloc[indices]["estacion_meteo"].to_numpy()
    df_air_loc["distance_to_meteo"] = distancias

    return df_air_loc


# ------------------------------------------------------------
# Merge air + meteo
# ------------------------------------------------------------
def merge_air_meteo(df_air, df_air_meteo_dist, df_meteo):
    df_air_meteo_dist = df_air_meteo_dist[["estacion", "estacion_meteo", "distance_to_meteo"]].drop_duplicates()

    df_air_meteo_est = pd.merge(
        df_air,
        df_air_meteo_dist,
        on="estacion",
        how="left"
    )

    df_meteo_sel = df_meteo[[
        "timestamp_meteo", "estacion_meteo",
        "VV", "DV", "T", "HR", "PB", "P",
        "altitud_meteo", "nom_estacion_meteo"
    ]].copy()

    df_air_meteo = pd.merge(
        df_air_meteo_est,
        df_meteo_sel,
        left_on=["timestamp", "estacion_meteo"],
        right_on=["timestamp_meteo", "estacion_meteo"],
        how="left"
    )

    df_air_meteo = df_air_meteo.drop(columns=["timestamp_meteo"], errors="ignore")
    return df_air_meteo


# ------------------------------------------------------------
# Merge final
# ------------------------------------------------------------
def merge_air_meteo_traffic(df_air_meteo, df_traffic_feature):
    df_air_meteo = df_air_meteo.copy()
    df_traffic_feature = df_traffic_feature.copy()

    df_air_meteo["timestamp"] = pd.to_datetime(df_air_meteo["timestamp"], errors="coerce")
    df_traffic_feature["fecha"] = pd.to_datetime(df_traffic_feature["fecha"], errors="coerce")
    df_final = pd.merge(
        df_air_meteo,
        df_traffic_feature,
        left_on=["estacion", "timestamp"],
        right_on=["estacion", "fecha"],
        how="left"
    )

    return df_final


# ------------------------------------------------------------
# Pipeline
# ------------------------------------------------------------
def run_pipeline():

    df_air,df_air_stations,df_traffic,df_traffic_est,df_meteo,df_est_meteo = load_raw_data()
    df_traffic = filter_traffic_df(df_traffic, df_traffic_est)
    df_air = filter_airquality_df(df_air, df_air_stations)
    
    df_meteo = filter_meteo_df(df_meteo, df_est_meteo)
    df_dist = calculate_distance_KDTree_traffic(df_traffic, df_air)
    df_traffic_feat = calculate_feature_traffic(df_dist, df_traffic)

    df_air_meteo_dist = calculate_distance_KDTree_meteo(df_air, df_meteo)
    df_air_meteo = merge_air_meteo(df_air, df_air_meteo_dist, df_meteo)
    
    df_final = merge_air_meteo_traffic(df_air_meteo, df_traffic_feat)
    
    df_final = df_final.drop(columns=["fecha"], errors="ignore")

    df_final = df_final.dropna(
        subset=["valor", "VV", "DV", "T", "HR", "PB", "P"],
        how="any"
    )
    
    df_final['hour'] = df_final['timestamp'].dt.hour
    df_final['day'] = df_final['timestamp'].dt.day
    df_final['month'] = df_final['timestamp'].dt.month
    df_final['year'] = df_final['timestamp'].dt.year
    df_final["timestamp"] = pd.to_datetime(df_final["timestamp"])
    df_final.to_parquet(DATA_HOURLY_DIR / "dataset_final_hourly.parquet", index=False)
    print("Pipeline finished -> dataset_final.csv")


if __name__ == "__main__":
    run_pipeline()