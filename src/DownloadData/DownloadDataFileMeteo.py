import requests
import pandas as pd
from io import StringIO
from pyproj import Transformer

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
    
    df_est = df_est.drop(["LONGITUD_ETRS89","LATITUD_ETRS89","CÓDIGO","COORDENADA_X_ETRS89","COORDENADA_Y_ETRS89","DIRECCION","VIA_NOMBRE","VIA_PAR","NUM_VIA","COD_VIA"],axis=1)
    cols=["VV (81)" ,"DV (82)", "T (83)", "HR (86)", "PB (87)" ,"P (89)"]
    df_est = df_est[df_est[cols].eq("X").all(axis=1)]
    lista_estaciones = df_est["CÓDIGO_CORTO"].astype(int).unique()
    df_meteo = df_meteo[df_meteo["ESTACION"].astype(int).isin(lista_estaciones)]
    df_est = df_est.drop(["VV (81)" ,"DV (82)", "T (83)", "HR (86)", "PB (87)" ,"P (89)","RS (88)"],axis=1)
    
    df_est["CÓDIGO_CORTO"] = df_est["CÓDIGO_CORTO"].apply(pd.to_numeric, errors="coerce")
    df_meteo["ESTACION"] = df_meteo["ESTACION"].apply(pd.to_numeric, errors="coerce")
    df_meteo["valor"] = df_meteo["valor"].apply(pd.to_numeric, errors="coerce")
    df_meteo["MAGNITUD"] = df_meteo["MAGNITUD"].apply(pd.to_numeric, errors="coerce")

    df_est=df_est.rename(columns={"ESTACION": "nom_estacion"})
    df_meteo = df_meteo[df_meteo["MAGNITUD"].isin([81,82,83,86,87,89])]
    df_meteo = df_meteo.pivot_table(index=["ESTACION","ANO","MES","DIA","hora"],columns="MAGNITUD",values="valor",aggfunc="first").reset_index()
    
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:25830", always_xy=True)
    df_est[['st_x','st_y']] = df_est.apply(lambda r: transformer.transform(r['LONGITUD'], r['LATITUD']),axis=1,result_type="expand")

    df_meteo = pd.merge(df_meteo,df_est, left_on='ESTACION', right_on="CÓDIGO_CORTO",how="left")
    df_meteo = df_meteo.drop(columns="CÓDIGO_CORTO").reset_index(drop=True)
    df_meteo.columns =["estacion_meteo","ano","mes","dia","hora","VV" ,"DV", "T", "HR", "PB" ,"P",
                       "nom_estacion_meteo","altitud_meteo","via_clase_meteo","lon_meteo","lat_meteo","st_x_meteo","st_y_meteo"]
    return df_meteo


url= "https://datos.madrid.es/dataset/300392-0-meteorologia-tiempo-real/resource/300392-5-meteorologia-tiempo-real/download/300392-5-meteorologia-tiempo-real.json"
url_est= "https://datos.madrid.es/dataset/300360-0-meteorologicos-estaciones/resource/300360-1-meteorologicos-estaciones-csv/download/300360-1-meteorologicos-estaciones-csv.csv"
response=requests.get(url)
print(response.status_code)
data = response.json()
df_meteo = pd.DataFrame(data["records"])
d_est = pd.read_csv(url_est,sep=";")
df_meteo = filter_meteo_df(df_meteo,d_est)
print(df_meteo.head(25))





# url= "https://informo.madrid.es/informo/tmadrid/pm.xml"
# headers = {'accept': 'application/xml;q=0.9, */*;q=0.8'}
# response=requests.get(url,headers = headers)


# print(response.status_code)
# df = pd.read_xml(url)
# df = pd.read_xml(StringIO(response.text))
# df["fecha_hora"] = pd.to_datetime(df.loc[0,"fecha_hora"],format="%d/%m/%Y %H:%M:%S")
# df = df.drop(index=0).reset_index(drop=True)
# print(df.head())
