#! DASHBOARD DE ANÁLISIS, PRONÓSTICO Y CLASIFICACIÓN DE INVERSIONES EN CRIPTOMONEDAS

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sklearn.cluster import KMeans
from sklearn.tree import DecisionTreeClassifier, plot_tree
from sklearn.preprocessing import StandardScaler
from prophet import Prophet
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import locale

# ---------------- CONFIGURACIÓN DE PÁGINA ----------------
st.set_page_config(page_title="Análisis Cripto", page_icon="💰", layout="wide")
st.title("ANÁLISIS Y PRONÓSTICO DE CRIPTOMONEDAS")

# ---------------- CONFIGURAR LOCALIZACIÓN ----------------
locale.setlocale(locale.LC_TIME, 'es_ES' if 'es_ES' in locale.locale_alias else 'Spanish_Spain.1252')

# ---------------- CONEXIÓN A LA BASE DE DATOS ----------------
@st.cache_data(show_spinner=False)
def cargar_datos():
    engine = create_engine("mysql+pymysql://root:Ha1Da40318@localhost/dss_criptomonedas")
    query = """
        SELECT 
            m.Nombre AS Moneda, f.Fecha, t.Precio_Cierre, t.Precio_Apertura, t.Volumen,
            t.Precio_Maximo, t.Precio_Minimo, f.Mes, f.Año, f.Trimestre, f.Semestre,
            f.Hora, f.idFecha, m.idMoneda, t.idTransaccion
        FROM transaccion t
        JOIN fecha f ON t.idFecha = f.idFecha
        JOIN moneda m ON t.idMoneda = m.idMoneda
    """
    df = pd.read_sql(query, engine)
    df["Inversion"] = df["Precio_Cierre"] * df["Volumen"]
    return df

with st.spinner("Cargando datos desde la base de datos..."):
    df = cargar_datos()

if df.empty:
    st.warning("No se encontraron datos.")
    st.stop()

# ---------------- FILTROS EN LA PRIMERA FILA ----------------
col1, col2 = st.columns([3, 1])
with col1:
    moneda = st.selectbox("Selecciona Criptomoneda:", df["Moneda"].unique())
with col2:
    años_a_predecir = st.slider("Años a Pronosticar:", min_value=1, max_value=5, value=1)

df_moneda = df[df["Moneda"] == moneda].copy().sort_values("Fecha")

# ---------------- SECCIÓN 1: PRONÓSTICO CON PROPHET ----------------
st.subheader("📈 1. Pronóstico de Precio de Cierre")
try:
    df_forecast = df_moneda[["Fecha", "Precio_Cierre"]].rename(columns={"Fecha": "ds", "Precio_Cierre": "y"})
    df_forecast["ds"] = pd.to_datetime(df_forecast["ds"])

    if len(df_forecast) > 1000:
        df_forecast = df_forecast.groupby("ds").mean(numeric_only=True).reset_index()

    modelo = Prophet()
    modelo.fit(df_forecast)
    future = modelo.make_future_dataframe(periods=365 * años_a_predecir)
    forecast = modelo.predict(future)

    forecast['Mes'] = forecast['ds'].dt.strftime('%b %Y')

    fig1 = go.Figure()
    fig1.add_trace(go.Scatter(
        x=forecast['ds'], y=forecast['yhat'],
        mode='lines',
        name='Pronóstico',
        line=dict(color='rgba(0,116,217,1)', width=2),
        fill='tozeroy',
        fillcolor='rgba(0,116,217,0.2)'
    ))
    fig1.update_layout(
        title=f"📈 Pronóstico de Precio de Cierre ({años_a_predecir} año(s))",
        xaxis_title="Mes y Año",
        yaxis_title="Precio estimado de cierre",
        template="plotly_white"
    )
    st.plotly_chart(fig1, use_container_width=True)

    st.caption("**Eje X**: Fecha proyectada (mes y año). **Eje Y**: Precio de cierre estimado para la moneda seleccionada.")

except Exception as e:
    st.error(f"⚠️ Error al generar el pronóstico: {e}")

# ---------------- SECCIÓN 2: CLASIFICACIÓN DE INVERSIONES ----------------
st.subheader("📊 2. Clasificación de Inversión")

st.markdown("#### A. Clasificación por Rangos Fijos")
st.markdown("""
📘 **Descripción:** Clasificación manual en 3 grupos según monto invertido:
- **Baja:** Inversiones menores a 10,000
- **Media:** Entre 10,000 y 50,000
- **Alta:** Mayores a 50,000

📌 **Utilidad:** Ayuda a comprender la distribución general del volumen de inversión.
""")
df_moneda["Grupo_Fijo"] = pd.cut(df_moneda["Inversion"], bins=[-float("inf"), 10000, 50000, float("inf")], labels=["Baja", "Media", "Alta"])
df_fijo_count = df_moneda["Grupo_Fijo"].value_counts().sort_index()

fig_fijo = px.bar(
    df_fijo_count,
    x=df_fijo_count.index,
    y=df_fijo_count.values,
    labels={"x": "Categoría de Inversión", "y": "Número de transacciones"},
    title="Distribución de Inversiones por Rangos Fijos",
    color=df_fijo_count.index,
    color_discrete_map={"Baja": "#a6cee3", "Media": "#1f78b4", "Alta": "#08306b"},
    template="plotly_white"
)
fig_fijo.update_layout(yaxis_title="Cantidad de transacciones", xaxis_title="Categoría")

# ---------------- GRAFICAS EN FILA ----------------
col_rangos, col_kmeans = st.columns(2)
with col_rangos:
    st.plotly_chart(fig_fijo, use_container_width=True)

with col_kmeans:
    st.markdown("#### B. Agrupamiento con KMeans")
    try:
        st.markdown("""
📘 **Descripción:** Clasificación automática usando el algoritmo **KMeans**, que agrupa las transacciones en 3 categorías basadas en patrones similares (clusters).

📌 **Utilidad:** Descubre agrupaciones naturales no visibles manualmente. Útil para segmentar perfiles de inversión o detectar comportamientos inusuales.
""")
        X = df_moneda[["Inversion"]].dropna()
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        kmeans = KMeans(n_clusters=3, random_state=0)
        kmeans.fit(X_scaled)
        df_moneda["Cluster"] = kmeans.labels_

        fig2 = px.scatter(
            df_moneda,
            x="Fecha",
            y="Inversion",
            color="Cluster",
            title="Agrupamiento de Inversiones por KMeans",
            labels={"Fecha": "Fecha de transacción", "Inversion": "Monto Invertido"},
            template="plotly_white"
        )
        st.plotly_chart(fig2, use_container_width=True)

    except Exception as e:
        st.error(f"Error al aplicar KMeans: {e}")

# ---------------- FOOTER ----------------
st.markdown("---")
st.markdown("Desarrollado para DSS Criptomonedas · Streamlit + MySQL + ML")
