# tests/test_transform.py
import pytest
import pandas as pd
import numpy as np
from include.src.transform_data import (
    filter_traffic_df,
    transform_coordinates,
    melt_hours_with_validation,
    calculate_feature_traffic,
    calculate_distance_KDTree_traffic
)

# ------------------------------------------------------------
# Fixtures — datos de prueba reutilizables
# ------------------------------------------------------------
@pytest.fixture
def df_traffic():
    return pd.DataFrame({
        "idelem": [1, 2, 3],
        "intensidad": ["100", "200", "300"],
        "ocupacion": ["10", "20", "30"],
        "carga": ["5", "10", "15"],
        "fecha": ["2026-03-19 08:00:00"] * 3
    })

@pytest.fixture
def df_traffic_est():
    return pd.DataFrame({
        "id": [1, 2, 3],
        "cod_cent": ["A", "B", "C"],
        "longitud": ["-3,7038", "-3,8000", "-3,6000"],
        "latitud": ["40,4168", "40,5000", "40,3000"]
    })

# ------------------------------------------------------------
# filter_traffic_df
# ------------------------------------------------------------
def test_filter_traffic_df_columns(df_traffic, df_traffic_est):
    result = filter_traffic_df(df_traffic, df_traffic_est)
    expected_cols = ["id", "cod_cent", "fecha", "intensidad", "ocupacion", "carga", "longitud", "latitud", "st_x", "st_y", "fecha_hora"]
    for col in expected_cols:
        assert col in result.columns, f"Columna {col} no encontrada"

def test_filter_traffic_df_numeric_types(df_traffic, df_traffic_est):
    result = filter_traffic_df(df_traffic, df_traffic_est)
    assert pd.api.types.is_numeric_dtype(result["intensidad"])
    assert pd.api.types.is_numeric_dtype(result["ocupacion"])
    assert pd.api.types.is_numeric_dtype(result["carga"])

def test_filter_traffic_df_not_empty(df_traffic, df_traffic_est):
    result = filter_traffic_df(df_traffic, df_traffic_est)
    assert not result.empty

def test_filter_traffic_df_merge_correct(df_traffic, df_traffic_est):
    result = filter_traffic_df(df_traffic, df_traffic_est)
    assert len(result) == len(df_traffic)

def test_filter_traffic_df_coordinates(df_traffic, df_traffic_est):
    result = filter_traffic_df(df_traffic, df_traffic_est)
    assert "st_x" in result.columns
    assert "st_y" in result.columns
    assert result["st_x"].notna().all()
    assert result["st_y"].notna().all()

# ------------------------------------------------------------
# transform_coordinates
# ------------------------------------------------------------
def test_transform_coordinates():
    df = pd.DataFrame({
        "longitud": [-3.7038],
        "latitud": [40.4168]
    })
    result = transform_coordinates(df, "longitud", "latitud")
    assert "st_x" in result.columns
    assert "st_y" in result.columns
    assert result["st_x"].iloc[0] > 0
    assert result["st_y"].iloc[0] > 0

def test_transform_coordinates_comma_separator():
    df = pd.DataFrame({
        "longitud": ["-3,7038"],
        "latitud": ["40,4168"]
    })
    result = transform_coordinates(df, "longitud", "latitud")
    assert result["st_x"].notna().all()

# ------------------------------------------------------------
# calculate_feature_traffic
# ------------------------------------------------------------
def test_calculate_feature_traffic():
    df_dist = pd.DataFrame({
        "estacion": [1, 1],
        "id": [10, 11],
        "dist_m": [50, 150],
        "buffer_100": [True, False],
        "buffer_200": [True, True],
        "buffer_300": [True, True]
    })
    df_traffic = pd.DataFrame({
        "id": [10, 11],
        "intensidad": [100.0, 200.0],
        "ocupacion": [10.0, 20.0],
        "carga": [5.0, 10.0],
        "fecha": ["2026-03-19 08:00:00", "2026-03-19 08:00:00"]
    })
    result = calculate_feature_traffic(df_dist, df_traffic)
    assert not result.empty
    assert "intensidad_w" in result.columns
    assert "ocupacion_w" in result.columns
    assert "carga_w" in result.columns