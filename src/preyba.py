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
file_path_quality = BASE_DIR / "air_quality/**/*.csv"
file_path_quality_stations = BASE_DIR / "data" / "AirQuality" / "stations" / "informacion_estaciones.csv"
file_path_traffic = BASE_DIR / "data" / "Traffic" / "inbox" / "trafico_20260305_205100.csv"
# df_air = pd.read_csv(file_path_quality, sep=";")
#df_air_stations = pd.read_csv(file_path_quality_stations, sep=";")
# df_traffic = pd.read_csv(file_path_traffic, sep=";")
lista_ids = [
3909, 3910, 3913, 3915, 3916, 3917, 3926, 10078, 10079, 9893,
4022, 4026, 4027, 4028, 4044, 4045, 4048, 9888, 10103, 4054,
4068, 4071, 4073, 5422, 5423, 5424, 5425, 5439, 5440, 3421,
7017, 5465, 5468, 3449, 5515, 5516, 5527, 5547, 5611, 3453,
5639, 5710, 4472, 3513, 5772, 5777, 3411, 3439, 5778, 7004,
4129, 7076, 4127, 4283, 4284, 4285, 4286, 4291, 4292, 10387,
4353, 4301, 10388, 3730, 10678, 4305, 4306, 4309, 10874, 4313,
4314, 4315, 4316, 10885, 10875, 10646, 4354, 10645, 4355, 10647,
10884, 10883, 10878, 4581, 10809, 4848, 5067, 5069, 5083, 5084,
5085, 3742, 5088, 5089, 5090, 5091, 10579, 10580, 10581, 10982,
11030, 11006, 10256, 10250, 10614, 10615, 11007, 3635, 10848, 10849,
10850, 3633, 6573, 5068, 10332, 10814, 4282, 4436, 4437, 4442,
4443, 7071, 4460, 4461, 4465, 4466, 4467, 4468, 4469, 4556,
3408, 4557, 4620, 5414, 5415, 5416, 5417, 5421, 5437, 4555,
10182, 10186, 5783, 5784, 5785, 3594, 5922, 5939, 11037, 11038,
11176, 11175, 11469, 6037, 10124, 11463, 11468, 6116, 6117, 6118,
6119, 6123, 10889, 10890, 6928, 11392, 11393, 6452, 10460, 11125,
11160, 6826
]

import pandas as pd
import glob

files = glob.glob("traffic/**/*.csv", recursive=True)

df = pd.concat(
    (
        pd.read_csv(
            f,
            sep=";",
            na_values=["NaN"],
            encoding="latin1"
        )
        for f in files
    ),
    ignore_index=True
)

df = df[
    df["fecha"].str.endswith(":00:00") &
    (df["error"] == "N") &
    (df["tipo_elem"] == "URB") &
    (df["id"].isin(lista_ids))
]

df = df.drop(columns=["tipo_elem", "error", "vmed", "periodo_integracion"])

df.to_csv("traffic_horas_exactas_v2.csv", sep=";", index=False)