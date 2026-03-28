import pandas as pd
from pathlib import Path
from pyproj import Transformer
from scipy.spatial import cKDTree
from client.client import AEMETClient, OpenMeteoClient
import folium
import requests
import numpy as np
from io import StringIO
import glob
import polars as pl

BASE_DIR = Path(__file__).resolve().parents[1]
file_path_quality_stations = BASE_DIR / "data" / "AirQuality" / "stations" / "informacion_estaciones.csv"
file_path_traffic = "traffic_horas_exactas_v2.csv"
file_path_traffic_est="202468-212-intensidad-trafico-csv.csv"
file_path_meteo_est= "300360-1-meteorologicos-estaciones-csv.csv"
files_air = glob.glob("air_quality/**/*.csv", recursive=True)
files_meteo = glob.glob("meteo/*.csv", recursive=True)
df_air = pd.concat(
    [pd.read_csv(f, sep=";") for f in files_air],
    ignore_index=True
)
df_air_stations = pd.read_csv(file_path_quality_stations, sep=";")
df_traffic = pd.read_csv(file_path_traffic, sep=";")
df_traffic_est = pd.read_csv(file_path_traffic_est, sep=";",    encoding="utf-8",encoding_errors="ignore")

df_meteo = pd.concat(
    [pd.read_csv(f, sep=";") for f in files_meteo],
    ignore_index=True
)
d_est_meteo= pd.read_csv(file_path_meteo_est, sep=";")
def filter_traffic_df(df_traffic: pd.DataFrame,df_traffic_est: pd.DataFrame) ->pd.DataFrame:
    df_traffic_filter= pd.merge(df_traffic,df_traffic_est, left_on="id", right_on="id",how="left")
    df_traffic_filter = df_traffic_filter[["id","cod_cent","fecha","intensidad","ocupacion","carga","longitud","latitud"]]
    
    return df_traffic_filter

def filter_airquality_df(df_air: pd.DataFrame,df_air_stations: pd.DataFrame )->pd.DataFrame:
    id_vars = ["PROVINCIA", "MUNICIPIO", "ESTACION", "MAGNITUD", "PUNTO_MUESTREO", "ANO", "MES", "DIA"]
    h_cols = [f"H{i:02d}" for i in range(1, 25)]
    v_cols = [f"V{i:02d}" for i in range(1, 25)]

    df_h = df_air.melt(
        id_vars=id_vars,
        value_vars=h_cols,
        var_name="hora",
        value_name="valor"
    )

    df_v = df_air.melt(
        id_vars=id_vars,
        value_vars=v_cols,
        var_name="hora_validacion",
        value_name="validacion"
    )

    df_h["hora"] = df_h["hora"].str[1:].astype(int)
    df_v["hora_validacion"] = df_v["hora_validacion"].str[1:].astype(int)

    df_long = df_h.copy()
    df_long["validacion"] = df_v["validacion"]
    df_long = df_long[df_long["validacion"] == "V"]
    df_airquality_filter = df_long.drop(["validacion","PUNTO_MUESTREO","PROVINCIA","MUNICIPIO"],axis=1)

    df_air_stations = df_air_stations.drop(["DIRECCION","LONGITUD_ETRS89","LATITUD_ETRS89","COD_TIPO","NOM_TIPO"],axis=1)
    df_air_stations = df_air_stations.rename(columns={"ESTACION":"Nom_estacion"})

    df_airquality_filter= pd.merge(df_airquality_filter,df_air_stations, left_on='ESTACION', right_on="CODIGO_CORTO",how="left")
    df_airquality_filter.columns =["estacion","magnitud","ano","mes","dia","hora","valor","codigo_corto","nom_estacion","altitud","longitud","latitud"]
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:25830", always_xy=True)
    df_airquality_filter[['st_x','st_y']] = df_airquality_filter.apply(lambda r: transformer.transform(r['longitud'], r['latitud']),axis=1,result_type="expand")
    df_airquality_filter["timestamp"] = pd.to_datetime({
    "year": df_airquality_filter["ano"],
    "month": df_airquality_filter["mes"],
    "day": df_airquality_filter["dia"],
    "hour": df_airquality_filter["hora"]})
    df_airquality_filter = df_airquality_filter.drop([ 'ano', 'mes', 'dia', 'hora'],axis=1,errors="ignore")
    return df_airquality_filter

def filter_meteo_df(df_meteo: pd.DataFrame,df_est:pd.DataFrame )->pd.DataFrame:
    id_vars = ["PROVINCIA", "MUNICIPIO", "ESTACION", "MAGNITUD", "ANO", "MES", "DIA"]
    h_cols = [f"H{i:02d}" for i in range(1, 25)]
    v_cols = [f"V{i:02d}" for i in range(1, 25)]

    df_h = df_meteo.melt(
        id_vars=id_vars,
        value_vars=h_cols,
        var_name="hora",
        value_name="valor"
    )

    df_v = df_meteo.melt(
        id_vars=id_vars,
        value_vars=v_cols,
        var_name="hora_validacion",
        value_name="validacion"
    )

    df_h["hora"] = df_h["hora"].str[1:].astype(int)
    df_v["hora_validacion"] = df_v["hora_validacion"].str[1:].astype(int)

    df_long = df_h.copy()
    df_long["validacion"] = df_v["validacion"]
    df_long = df_long[df_long["validacion"] == "V"]
    df_meteo = df_long.drop(["validacion","PROVINCIA","MUNICIPIO"],axis=1)
    cols=["CÓDIGO_CORTO","ESTACION","ALTITUD","VV (81)" ,"DV (82)", "T (83)","HR (86)","PB (87)" ,"P (89)","LONGITUD","LATITUD"]
    df_est = df_est[cols]
    
    cols=["VV (81)" ,"DV (82)", "T (83)","HR (86)","PB (87)" ,"P (89)"]
    df_est = df_est[df_est[cols].eq("X").all(axis=1)]
    lista_estaciones = df_est["CÓDIGO_CORTO"].astype(int).unique()
    df_meteo = df_meteo[df_meteo["ESTACION"].astype(int).isin(lista_estaciones)]
    
    df_est = df_est.drop(["VV (81)" ,"DV (82)", "T (83)", "HR (86)", "PB (87)" ,"P (89)"],axis=1)
    
    df_est["CÓDIGO_CORTO"] = df_est["CÓDIGO_CORTO"].apply(pd.to_numeric, errors="coerce")
    df_meteo["ESTACION"] = df_meteo["ESTACION"].apply(pd.to_numeric, errors="coerce")
    df_meteo["valor"] = df_meteo["valor"].apply(pd.to_numeric, errors="coerce")
    df_meteo["MAGNITUD"] = df_meteo["MAGNITUD"].apply(pd.to_numeric, errors="coerce")

    df_est=df_est.rename(columns={"ESTACION": "nom_estacion"})
    df_meteo = df_meteo[df_meteo["MAGNITUD"].isin([81,82,83,86,87,89])]
    df_meteo = df_meteo.pivot_table(index=["ESTACION","ANO","MES","DIA","hora"],columns="MAGNITUD",values="valor",aggfunc="first").reset_index()

    
    
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:25830", always_xy=True)
    df_est["LONGITUD"] = pd.to_numeric(
    df_est["LONGITUD"].astype(str).str.replace(",", ".", regex=False),errors="coerce")

    df_est["LATITUD"] = pd.to_numeric(df_est["LATITUD"].astype(str).str.replace(",", ".", regex=False),errors="coerce")

    st_x, st_y = transformer.transform(df_est["LONGITUD"].to_numpy(),df_est["LATITUD"].to_numpy())

    df_est["st_x"] = st_x
    df_est["st_y"] = st_y

    df_meteo = pd.merge(df_meteo,df_est, left_on='ESTACION', right_on="CÓDIGO_CORTO",how="left")
    
    df_meteo = df_meteo.drop(columns="CÓDIGO_CORTO").reset_index(drop=True)
    df_meteo.columns =["estacion_meteo","ano","mes","dia","hora","VV" ,"DV", "T", "HR", "PB" ,"P","nom_estacion_meteo","altitud_meteo","lon_meteo","lat_meteo","st_x_meteo","st_y_meteo"]
    df_meteo["timestamp_meteo"] = pd.to_datetime({"year": df_meteo["ano"],"month": df_meteo["mes"],"day": df_meteo["dia"],"hour": df_meteo["hora"]})
    df_meteo=df_meteo.drop([ 'ano', 'mes', 'dia', 'hora'],axis=1,errors="ignore")
    
    return df_meteo

def calculate_distance_KDTree(df_traffic_filter: pd.DataFrame,df_airquality_filter: pd.DataFrame)->pd.DataFrame:
    tree = cKDTree(df_airquality_filter[["st_x", "st_y"]].to_numpy())
    distancias, indices = tree.query(df_traffic_filter[["st_x", "st_y"]].to_numpy(), k=1)
    df_traffic_filter["distancia_m"] = distancias
    df_traffic_filter["idx_estacion"] = indices

    df_traffic_filter["cod_estacion"] = df_airquality_filter.iloc[indices]["codigo_corto"].values
    df_traffic_filter["nom_estacion"] = df_airquality_filter.iloc[indices]["nom_estacion"].values
    df_traffic_filter = df_traffic_filter[df_traffic_filter["distancia_m"] <= 300]

    return df_traffic_filter

def calculate_distance_KDTree_traffic(df_traffic_filter: pd.DataFrame,df_airquality_filter: pd.DataFrame)->pd.DataFrame:
    df_traffic_location = df_traffic_filter[["id","longitud", "latitud"]].drop_duplicates().reset_index(drop=True)
    df_traffic_location["longitud"] = pd.to_numeric(df_traffic_location["longitud"].astype(str).str.replace(",", "."), errors="coerce")
    df_traffic_location["latitud"] = pd.to_numeric(df_traffic_location["latitud"].astype(str).str.replace(",", "."), errors="coerce")
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:25830", always_xy=True)
    stx, sty = transformer.transform(df_traffic_location["longitud"].values,df_traffic_location["latitud"].values)
    df_traffic_location["st_x"] = stx
    df_traffic_location["st_y"] = sty
    df_airquality_location = df_airquality_filter[["estacion","st_x", "st_y"]].drop_duplicates().reset_index(drop=True)
    tree = cKDTree(df_traffic_location[["st_x", "st_y"]].to_numpy())
    idx_300 = tree.query_ball_point(df_airquality_location[["st_x", "st_y"]], r=300)
    results = []
    for i, traffic_idxs in enumerate(idx_300):
        if len(traffic_idxs) == 0:
            continue

        station = df_airquality_location.iloc[i]
        station_id = station["estacion"]
        station_x = station["st_x"]
        station_y = station["st_y"]

        traf = df_traffic_location.iloc[traffic_idxs].copy()

        traf["dist_m"] = np.sqrt(
            (traf["st_x"] - station_x) ** 2 +
            (traf["st_y"] - station_y) ** 2
        )

        traf["estacion"] = station_id
        traf["buffer_100"] = traf["dist_m"] <= 100
        traf["buffer_200"] = traf["dist_m"] <= 200
        traf["buffer_300"] = traf["dist_m"] <= 300

        results.append(
            traf[["estacion", "id", "dist_m", "buffer_100", "buffer_200", "buffer_300"]]
        )

    if results:
        return pd.concat(results, ignore_index=True)
    
    return pd.DataFrame(columns=["estacion", "id", "dist_m", "buffer_100", "buffer_200", "buffer_300"])

def calculate_feature_traffic(df_traffic_distance: pd.DataFrame,df_traffic: pd.DataFrame)->pd.DataFrame:
    cols =["buffer_100", "buffer_200", "buffer_300"]
    df_traffic_distance = df_traffic_distance[df_traffic_distance[cols].eq(True).any(axis=1)]
    df = pd.merge(df_traffic,df_traffic_distance,left_on='id', right_on="id",how="left")
    print(list(df.columns))
    df= df.dropna(subset=["estacion"])
    df["peso"] = df["dist_m"].apply(lambda x: np.exp(- x / 150))
    df["intensidad_w"] = df["intensidad"] * df["peso"]
    df["ocupacion_w"] = df["ocupacion"] * df["peso"]
    df["carga_w"] = df["carga"] * df["peso"]
    df_air_traffic= df.groupby(["estacion","fecha"]).agg({"intensidad_w":"sum","ocupacion_w":"sum","carga_w":"sum"}).reset_index()
    print(df_air_traffic.head())
    return df_air_traffic

def calculate_distance_KDTree_meteo(df_airquality_filter: pd.DataFrame,df_meteo: pd.DataFrame)->pd.DataFrame:
    df_met = df_meteo[["st_x_meteo", "st_y_meteo", "estacion_meteo"]]
    df_met = df_met.dropna(subset=["st_x_meteo", "st_y_meteo", "estacion_meteo"]).copy()
    df_met = df_met[["st_x_meteo", "st_y_meteo", "estacion_meteo"]].drop_duplicates().reset_index(drop=True)

    df_airquality_filter = df_airquality_filter.drop("timestamp",axis=1)
    df_airquality_filter = df_airquality_filter[["estacion", "codigo_corto", "nom_estacion", "longitud", "latitud", "st_x", "st_y"]].drop_duplicates().reset_index(drop=True)
    df_air = df_airquality_filter.dropna(subset=["st_x", "st_y", "codigo_corto", "nom_estacion"]).copy()

    tree = cKDTree(df_met[["st_x_meteo", "st_y_meteo"]].to_numpy())
    distancias, indices = tree.query(df_air[["st_x", "st_y"]].to_numpy(), k=1)
    df_air["id_station_meteo_near"] = indices
    df_air["estacion_meteo"] = df_met.iloc[indices]["estacion_meteo"].to_numpy()
    if "nombre" in df_met.columns:
        df_air["nom_estacion_meteo"] = df_met.iloc[indices]["nombre"].to_numpy()
    df_air["distance_to_meteo"] = distancias
    return df_air

def merge_air_meteo22222222222(df_airquality_filter: pd.DataFrame,df_air_meteo_dist: pd.DataFrame,df_meteo: pd.DataFrame)->pd.DataFrame:
    df_air_meteo_est= pd.merge(df_airquality_filter,df_air_meteo_dist, left_on=["estacion"], right_on=["estacion"],how="left")
    print(list(df_air_meteo_est.columns))
    df_air_meteo= pd.merge(df_air_meteo_est,df_meteo, left_on=["timestamp",'estacion_meteo'], right_on=["timestamp_meteo","estacion_meteo"],how="left")
    print(list(df_air_meteo.columns))
    df_air_meteo = df_air_meteo.drop(['estacion_meteo','nom_estacion_meteo', 'ano', 'mes', 'dia', 'hora',"codigo_corto",'lon_meteo', 'lat_meteo', 'st_x_meteo', 'st_y_meteo', 'timestamp_meteo'],axis=1,errors="ignore")
    print(list(df_air_meteo.columns))
    return df_air_meteo

def merge_air_meteo(df_airquality_filter: pd.DataFrame,df_air_meteo_dist: pd.DataFrame,df_meteo: pd.DataFrame) -> pd.DataFrame:

    df_air_meteo_dist = df_air_meteo_dist[["estacion", "estacion_meteo", "distance_to_meteo"]].drop_duplicates()
    df_air_meteo_est = pd.merge(df_airquality_filter,df_air_meteo_dist,on="estacion",how="left")
    df_meteo = df_meteo[["timestamp_meteo", "estacion_meteo", "VV", "DV", "T", "HR", "PB", "P", "altitud_meteo"]]
    df_air_meteo = pd.merge(df_air_meteo_est,df_meteo,left_on=["timestamp", "estacion_meteo"],right_on=["timestamp_meteo", "estacion_meteo"],how="left")
    df_air_meteo = df_air_meteo.drop(columns=["timestamp_meteo"],errors="ignore")
    return df_air_meteo

def merge_air_meteo_traffic(df_air_meteo: pd.DataFrame,df_traffic_feature: pd.DataFrame)->pd.DataFrame:
    df_traffic_feature = df_traffic_feature.copy()
    df_air_meteo = df_air_meteo.copy()

    df_air_meteo["timestamp"] = pd.to_datetime(df_air_meteo["timestamp"], errors="coerce")
    df_traffic_feature["fecha"] = pd.to_datetime(df_traffic_feature["fecha"], errors="coerce")

    df_air_meteo_traffic = pd.merge(
        df_air_meteo,
        df_traffic_feature,
        left_on=["estacion", "timestamp"],
        right_on=["estacion", "fecha"],
        how="left"
    )
    print(df_air_meteo_traffic.shape)
    return df_air_meteo_traffic


df_traffic_filter = filter_traffic_df(df_traffic,df_traffic_est)
df_airquality_filter = filter_airquality_df(df_air,df_air_stations)

df_traffic_air_distance=calculate_distance_KDTree_traffic(df_traffic_filter,df_airquality_filter)
df_traffic_feature = calculate_feature_traffic(df_traffic_air_distance,df_traffic_filter)

df_meteo_filter = filter_meteo_df(df_meteo,d_est_meteo)
df_meteo_air_station = calculate_distance_KDTree_meteo(df_airquality_filter,df_meteo_filter)
df_air_meteo = merge_air_meteo(df_airquality_filter,df_meteo_air_station,df_meteo_filter)
df_air_meteo_traffic= merge_air_meteo_traffic(df_air_meteo,df_traffic_feature)
df_air_meteo_traffic.to_csv('nombre_archivo.csv', index=False)