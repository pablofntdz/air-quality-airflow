# dashboard/app.py
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
# ===========================
# Config
# ===========================
st.set_page_config(page_title="Air Quality Predictions", layout="wide")
st.title("Air Quality Predictions Dashboard")

@st.cache_data(ttl=300)  # refresca cada 5 minutos
def load_predictions():
    engine = create_engine("postgresql://postgres:12345@project_db:5432/project_db")
    df = pd.read_sql("SELECT * FROM predictions ORDER BY timestamp DESC", engine)
    return df

# ===========================
# Load data 
# ===========================
df = load_predictions()
df["timestamp"] = pd.to_datetime(df["timestamp"])
df["error"] = (df["y_real"] - df["y_pred"]).abs()
df["alerta"] = df["error"] > df.groupby("estacion")["error"].transform("mean") * 2

# ===========================
# Sidebar
# ===========================
with st.sidebar:
    st.title("Filtros")
    
    estaciones = sorted(df["estacion"].unique())
    estacion_sel = st.selectbox("Estación", estaciones)
    
    dias = st.slider("Últimos N días", 1, 30, 7)
    cutoff = datetime.now() - timedelta(days=dias)
    
    st.divider()
    st.metric("Total predicciones", len(df))
    st.metric("Estaciones", df["estacion"].nunique())
    st.metric("Alertas activas", df["alerta"].sum())

# ===========================
# Filter
# ===========================
df_s = df[
    (df["estacion"] == estacion_sel) &
    (df["timestamp"] >= cutoff)
].sort_values("timestamp")

# ===========================
# KPIs
# ===========================
col1, col2, col3, col4 = st.columns(4)

rmse = (df_s["error"] ** 2).mean() ** 0.5
mae = df_s["error"].mean()
alertas = df_s["alerta"].sum()
ultimo = df_s["timestamp"].max()

col1.metric("RMSE", f"{rmse:.2f}")
col2.metric("MAE", f"{mae:.2f}")
col3.metric("Alertas", int(alertas))
col4.metric("Última predicción", ultimo.strftime("%H:%M %d/%m") if pd.notna(ultimo) else "N/A")

st.divider()

# ===========================
# Serie temporal predicho vs real
# ===========================
st.subheader(f"Predicho vs Real — Estación {estacion_sel}")

fig = go.Figure()
fig.add_trace(go.Scatter(
    x=df_s["timestamp"], y=df_s["y_real"],
    mode="lines", name="Real",
    line=dict(color="blue")
))
fig.add_trace(go.Scatter(
    x=df_s["timestamp"], y=df_s["y_pred"],
    mode="lines", name="Predicho",
    line=dict(color="orange", dash="dash")
))

# Marcar alertas
alertas_df = df_s[df_s["alerta"]]
fig.add_trace(go.Scatter(
    x=alertas_df["timestamp"], y=alertas_df["y_real"],
    mode="markers", name="Alerta",
    marker=dict(color="red", size=10, symbol="x")
))

fig.update_layout(height=400, xaxis_title="Fecha", yaxis_title="NO₂ (µg/m³)")
st.plotly_chart(fig, use_container_width=True)

# ===========================
# Alertas
# ===========================
if not alertas_df.empty:
    st.warning(f"⚠️ {len(alertas_df)} alertas detectadas para la estación {estacion_sel}")
    st.dataframe(
        alertas_df[["timestamp", "y_real", "y_pred", "error"]].rename(columns={
            "timestamp": "Fecha",
            "y_real": "Real",
            "y_pred": "Predicho",
            "error": "Error"
        }),
        use_container_width=True
    )

st.divider()

# ===========================
# Mapa de estaciones
# ===========================
st.subheader("Mapa de estaciones — Error medio por estación")

STATION_COORDS = {
    4:  (40.4230, -3.7122),
    8:  (40.4213, -3.6822),
    11: (40.4531, -3.6922),
    16: (40.4631, -3.7022),
    17: (40.4131, -3.6722),
    27: (40.3931, -3.7122),
    35: (40.4031, -3.6622),
    36: (40.4331, -3.7222),
    38: (40.4431, -3.6522),
    39: (40.4731, -3.7322),
    40: (40.3831, -3.6922),
    47: (40.4831, -3.7122),
    48: (40.3731, -3.7022),
    50: (40.4931, -3.6822),
    56: (40.3631, -3.6722),
    57: (40.5031, -3.7222),
    60: (40.3531, -3.7122),
}

df_map = df.groupby("estacion").agg(
    error_medio=("error", "mean"),
    alertas=("alerta", "sum"),
    predicciones=("y_pred", "count")
).reset_index()

df_map["lat"] = df_map["estacion"].map(lambda x: STATION_COORDS.get(x, (40.41, -3.70))[0])
df_map["lon"] = df_map["estacion"].map(lambda x: STATION_COORDS.get(x, (40.41, -3.70))[1])

fig_map = px.scatter_mapbox(
    df_map,
    lat="lat", lon="lon",
    size="error_medio",
    color="error_medio",
    color_continuous_scale="RdYlGn_r",
    hover_name="estacion",
    hover_data={"alertas": True, "predicciones": True, "error_medio": ":.2f"},
    zoom=11,
    mapbox_style="carto-positron",
    title="Error medio por estación"
)
fig_map.update_layout(height=500)
st.plotly_chart(fig_map, use_container_width=True)

# ===========================
# Métricas por estación
# ===========================
st.subheader("Métricas por estación")

df_metrics = df.groupby("estacion").agg(
    rmse=("error", lambda x: (x**2).mean()**0.5),
    mae=("error", "mean"),
    alertas=("alerta", "sum"),
    n=("y_pred", "count")
).reset_index().round(2)

st.dataframe(df_metrics, use_container_width=True)