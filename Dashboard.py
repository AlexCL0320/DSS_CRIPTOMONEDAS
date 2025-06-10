
#NOTE: ---------------------------------------------------------------- APARTADO DE IMPORTACIONES
#! Importaciones principales para la app
import streamlit as st  # type: ignore

#! libreria de sql para operaciones de carga en python
import pymysql
from datetime import datetime
#! Conector para Base de Datos MySQL
import mysql.connector 
import locale
locale.setlocale(locale.LC_TIME, 'Spanish_Spain.1252')
#! Manejo de números muy grandes con formato legible
from numerize.numerize import numerize  # type: ignore

#! Manipulación y análisis de datos
import pandas as pd  

#! Visualización de datos con Plotly
import plotly.express as px
import plotly.graph_objects as go

#! Algoritmos de clustering y preprocesamiento
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

#! Modelado de series temporales con Prophet
from prophet import Prophet

#! Conexión a bases de datos mediante SQLAlchemy
from sqlalchemy import create_engine

#! Personalización y visualización avanzada de tablas en Streamlit
from st_aggrid import AgGrid, GridOptionsBuilder

#! Obtenemos los iconos de https://streamlit-emoji-shortcodes-streamlit-app-gwckff.streamlit.app/
st.set_page_config(page_title="ANÁLISIS DE CRIPTOS", page_icon="💲", layout="wide")

#! Cargar los Estilos Definidos
with open('style.css') as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

#! Título principal del dashboard
st.markdown(
    '<h1 class="titulo-principal" style="margin-bottom:60px;">ANÁLISIS Y PREDICCIÓN DE CRIPTOMONEDAS</h1>',
    unsafe_allow_html=True
)

#NOTE: ---------------------------------------------------------------- APARTADO DE OBTENCIÓN DE DATOS
#! Obtener los datos
def obtener_datos():
    try:
        #! Crear conexión a MySQL
        conn = mysql.connector.connect(
            #! Se determina al local Host
            host="localhost",
            #! Nombre de Usuario
            user="root",
            #! Contraseña
            password="ROOT",
            database="dss_criptomonedas"
        )

        #! Realizar consulta SQL
        query = """
        SELECT 
            m.Nombre AS Moneda,
            f.Fecha,
            f.Año,
            f.Mes,
            f.Trimestre,
            f.Semestre,
            t.Precio_Apertura,
            t.Precio_Cierre,
            t.Precio_Maximo,
            t.Precio_Minimo,
            t.Volumen
        FROM transaccion t
        JOIN moneda m ON t.idMoneda = m.idMoneda
        JOIN fecha f ON t.idFecha = f.idFecha;
        """

        #! Ejecutar y leer resultado en DataFrame
        df = pd.read_sql(query, conn)
        conn.close()
        print("Conexión y lectura de datos exitosa.")
        return df
    
    except mysql.connector.Error as e:
        print(f"Error en la conexión o consulta: {e}")
        return pd.DataFrame() 
    
df = obtener_datos()

#NOTE: ---------------------------------------------------------------- APARTADO DE FILTROS DEL USUARIO
#! Filtros en la barra lateral
st.sidebar.header("🔎 Filtros de Busqueda")

#! Verifica que el DataFrame no esté vacío
if not df.empty:
    #! Opciones únicas para cada filtro
    monedas = df["Moneda"].unique()
    años = df["Año"].unique()
    meses = df["Mes"].unique()

    #! Selección de Año Máximo y Minimo
    año_min = min(años)
    año_max = max(años)

    #! Filtros para que el usuario pueda seleccionar opciones en la barra lateral
    #! Título para seleccionar la moneda
    st.sidebar.markdown('<p class="menu-lateral">Seleccionar Moneda</p>', unsafe_allow_html=True)
    #! Selector desplegable para elegir la moneda
    filtro_moneda = st.sidebar.selectbox("Moneda", options=monedas, label_visibility="collapsed")

    #! Título para seleccionar el rango de años
    st.sidebar.markdown('<p class="menu-lateral">Seleccionar rango de años</p>', unsafe_allow_html=True)
    #! Slider para elegir el rango de años entre año_min y año_max
    filtro_año = st.sidebar.slider("Rango de años", año_min, año_max, (año_min, año_max), label_visibility="collapsed")

    #! Título para seleccionar los meses
    st.sidebar.markdown('<p class="menu-lateral">Seleccionar Mes</p>', unsafe_allow_html=True)
    #! Selector múltiple para elegir uno o varios meses, por defecto selecciona todos
    filtro_mes = st.sidebar.multiselect("Meses", options=meses, default=meses, label_visibility="collapsed")

    #! Aplicar filtros al DataFrame
    df_filtrado = df[
        (df["Moneda"] == filtro_moneda) &
        (df["Año"] >= filtro_año[0]) &
        (df["Año"] <= filtro_año[1]) &
        (df["Mes"].isin(filtro_mes))
    ]

else:
    st.sidebar.warning("No se pudieron cargar los datos de la base de datos.")
#NOTE: ---------------------------------------------------------------- IAMGEN
#! Imagen de la Parte Inferior del Sidebar
st.sidebar.image("DATA/logo1.png", use_container_width=True)

#NOTE: ---------------------------------------------------------------- MENSAJES DE DESCRIPCIÓN DE MÉTRICAS
#! Función para mostrar métricas con información para el usuario
def mostrar_Mensaje(titulo, valor, descripcion):
    st.info(titulo)
    st.metric(label="Mensaje", value=valor,label_visibility="collapsed")
    with st.expander("❓ Ver descripción"):
        st.write(descripcion)

#NOTE: ---------------------------------------------------------------- MENSAJES DE DESCRIPCIÓN DE MÉTRICAS
#! Función para mostrar las métricas clave
def cuadritos(df_filtrado, moneda, años):

    #!Mensaje mostrando el periodo
    periodo = f"{min(años)} - {max(años)}" if len(años) > 1 else str(años[0])
    st.markdown(
        f'<h3 id="Metricas-clave" class="titulo-secundario" style="margin-bottom:60px;">Análisis de la criptomoneda <strong>{moneda}</strong> para el periodo: <strong>{periodo}</strong></h3>',
        unsafe_allow_html=True
    )
    #NOTE: ---------------------------------------------------------------- CREACIÓN DE MÉTRICAS
    #! Cálculo de métricas
    precio_promedio = round(df_filtrado["Precio_Cierre"].mean(), 4)
    precio_maximo = round(df_filtrado["Precio_Maximo"].max(), 4)
    precio_minimo = round(df_filtrado["Precio_Minimo"].min(), 4)
    volumen_total = round(df_filtrado["Volumen"].sum(), 2)
    variacion_media = round((df_filtrado["Precio_Cierre"] - df_filtrado["Precio_Apertura"]).mean(), 4)

    #! Visualización en pequeños espacios
    col1, col2, col3, col4, col5 = st.columns(5, gap="small")
    #! Espacio 1
    with col1:
        mostrar_Mensaje(
            "💵 Precio Prom. de Cierre",
            f"${precio_promedio:,.4f}",
            "Promedio del precio de cierre de la criptomoneda durante el periodo seleccionado."
        )
    #! Espacio 2
    with col2:
        mostrar_Mensaje(
            "📈 Precio Máximo",
            f"${precio_maximo:,.4f}",
            "Precio máximo registrado de la criptomoneda en el periodo."
        )
    #! Espacio 3
    with col3:
        mostrar_Mensaje(
            "📉 Precio Mínimo",
            f"${precio_minimo:,.4f}",
            "Precio mínimo registrado de la criptomoneda en el periodo."
        )
    #! Espacio 4
    with col4:
        mostrar_Mensaje(
            "📊 Volumen Total",
            numerize(volumen_total),
            "Volumen total de transacciones durante el periodo."
        )
    #! Espacio 5
    with col5:
        mostrar_Mensaje(
            "📉 Variación Media",
            f"{variacion_media:,.4f}",
            "Diferencia promedio entre el precio de cierre y apertura en el periodo."
        )

if not df_filtrado.empty:
    cuadritos(df_filtrado, filtro_moneda, filtro_año)
else:
    st.warning("No hay datos para mostrar con los filtros seleccionados.")

#NOTE: ---------------------------------------------------------------- EVOLUCIÓN DE PRECIOS DE CIERRE
#! Grafica de Comportamiento de la moneda
st.markdown(
    '<h3 id="evolucion-anual" class="titulo-secundario" style="margin-top: 30px; margin-bottom: 60px;"> Evolución de Precios de Cierre </h3>',
    unsafe_allow_html=True
)
#NOTE: ---------------------------------------------------------------- GRAFICA DE EVOLUCIÓN DE PRECIOS
col1, col2 = st.columns(2, gap="medium")
with col1: 
    #! Definir la gráfica
    fig = px.line(
        df_filtrado.sort_values("Fecha"),
        x="Fecha",
        y="Precio_Cierre",
        title=f"Histórico de {filtro_moneda} en el tiempo",
        labels={"Precio_Cierre": "Precio (USD)", "Fecha": "Fecha"},
        markers=True,
    )

    #! Cambiar la Vista de la Grafica
    fig.update_layout(
        template="plotly_white", 
        title={
            'text': f"Precio de Cierre de {filtro_moneda} en el tiempo",
            'x': 0.5, 
            'xanchor': 'center',
            'font': dict(size=22, family="Segoe UI, Tahoma, Geneva, Verdana, sans-serif", color="#333")
        },
        xaxis=dict(
            title="Fecha",
            showgrid=True,
            gridcolor="lightgray",
            tickangle=45,
            tickfont=dict(size=11),
            zeroline=False,
        ),
        yaxis=dict(
            title="Precio (USD)",
            showgrid=True,
            gridcolor="lightgray",
            tickprefix="$",
            zeroline=False,
        ),
        margin=dict(l=50, r=30, t=70, b=50),
        hovermode="x unified",
    )


    #! Personalizar la gráfica
    fig.update_traces(
        marker=dict(size=1, color="#e39e54"),
        line=dict(width=1, color="#e39e54"),
        hovertemplate='%{x}<br>Precio: $%{y:.4f}<extra></extra>',
    )

    #! Desplegamos la gráfica
    st.plotly_chart(fig, use_container_width=True)

#NOTE: ---------------------------------------------------------------- TABLA DE EVOLUCIÓN DE PRECIOS
#! Listado de Meses en Español
meses_es = {
    'January': 'Enero', 'February': 'Febrero', 'March': 'Marzo',
    'April': 'Abril', 'May': 'Mayo', 'June': 'Junio',
    'July': 'Julio', 'August': 'Agosto', 'September': 'Septiembre',
    'October': 'Octubre', 'November': 'Noviembre', 'December': 'Diciembre'
}

with col2:    
    #! Creación de Columnas
    tabla_cierre = df_filtrado[["Fecha", "Precio_Cierre"]].copy()
    tabla_cierre['Fecha'] = pd.to_datetime(tabla_cierre['Fecha'])
    tabla_cierre['Año'] = tabla_cierre['Fecha'].dt.year
    tabla_cierre['Mes'] = tabla_cierre['Fecha'].dt.month_name().map(meses_es)
    tabla_cierre = tabla_cierre[['Año', 'Mes', 'Precio_Cierre']]

    #! Configurar AgGrid
    gb = GridOptionsBuilder.from_dataframe(tabla_cierre)
    gb.configure_default_column(editable=False, filter=False, sortable=True, resizable=True)
    gb.configure_column("Precio_Cierre", type=["numericColumn", "numberColumnFilter", "customNumericFormat"], precision=2, valueFormatter="x.toLocaleString()")
    gridOptions = gb.build()

    #! Mostrar tabla
    AgGrid(
        tabla_cierre,
        gridOptions=gridOptions,
        enable_enterprise_modules=False,
        fit_columns_on_grid_load=True,
        allow_unsafe_jscode=True, 
        height=450,
        width='100%',
        theme='material',  
        columns_auto_size_mode='FIT_CONTENTS'
    )

#NOTE: ---------------------------------------------------------------- APARTADO DE ANÁLISIS Y PRONÓSTICO
#! Separador de Análisis y Pronóstico
st.markdown(
    '<h3 id="evolucion-anual" class="titulo-secundario" style="margin-top: 30px; margin-bottom: 60px;"> Análisis y Pronóstico de Criptomonedas </h3>',
    unsafe_allow_html=True
)

#NOTE: ---------------------------------------------------------------- OBTENCION DE DATOS PARA ANÁLISIS Y PRONOSTICOS
#! Obtención de Datos Específicos para el pronóstico
@st.cache_data(show_spinner=False)
def cargar_datos():
    engine = create_engine("mysql+pymysql://root:ROOT@localhost/dss_criptomonedas")
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
#NOTE: ---------------------------------------------------------------- MENSAJE Y FILTROS PARA ANÁLISIS
#! Mensaje de Moneda seleccionada y filtro de periodo para análisis de pronósitos
col1, col2 = st.columns([3, 1])
#! Columna Izquierda del menaje
with col1:
    st.markdown(
        f'<div class="menu-lateral-grande">🔸 Moneda seleccionada: <span>{filtro_moneda}</span></div>',
        unsafe_allow_html=True
    )
#! Columna derecha de selector
with col2:
    años_a_predecir = st.slider("🔸 Años a pronosticar:", min_value=1, max_value=5, value=1)

df_moneda = df[df["Moneda"] == filtro_moneda].copy().sort_values("Fecha")


#NOTE: ---------------------------------------------------------------- PRONÓSTICO
# ---------------- SECCIÓN 1: PRONÓSTICO CON PROPHET ----------------
st.subheader("📈 1. Pronóstico de Precio de Cierre")
st.markdown("<br>", unsafe_allow_html=True)
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

    st.caption("*Eje X: Fecha proyectada (mes y año). **Eje Y*: Precio de cierre estimado para la moneda seleccionada.")

except Exception as e:
    st.error(f"⚠ Error al generar el pronóstico: {e}")

#NOTE: --------------------------------------------------------------- 

import calendar

def RealizarPredicción(forecast, idMoneda): 
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="ROOT",
            database="dss_criptomonedas"
        )
        cursor = conn.cursor()

        df_pred = forecast[['ds', 'yhat']].copy()
        df_pred['ds'] = pd.to_datetime(df_pred['ds'])
        df_pred = df_pred[df_pred['ds'] > datetime.now()].head(7).reset_index(drop=True)

        for i, row in df_pred.iterrows():
            fecha_dt = row['ds']
            precio = round(row['yhat'], 4)

            if i == 0:
                variacion = 0.0
            else:
                precio_anterior = round(df_pred.loc[i - 1, 'yhat'], 4)
                variacion = round(precio - precio_anterior, 4)

            # Descomponer fecha y hora
            fecha = fecha_dt.date()
            hora = fecha_dt.time()
            año = fecha_dt.year
            mes = calendar.month_name[fecha_dt.month]
            dia_semana = fecha_dt.weekday() + 1  # Lunes=1, Domingo=7
            trimestre = (fecha_dt.month - 1) // 3 + 1
            semestre = 1 if fecha_dt.month <= 6 else 2

            # Asegurar idDiaSemana
            cursor.execute("SELECT idDiaSemana FROM diasemana WHERE idDiaSemana = %s", (dia_semana,))
            if not cursor.fetchone():
                cursor.execute("INSERT INTO diasemana (idDiaSemana, Nombre) VALUES (%s, %s)", (dia_semana, fecha_dt.strftime('%A')))
                conn.commit()

            # Buscar o insertar en fecha
            cursor.execute("SELECT idFecha FROM fecha WHERE Fecha = %s", (fecha,))
            result_fecha = cursor.fetchone()
            if result_fecha:
                idFecha = result_fecha[0]
            else:
                cursor.execute("""
                    INSERT INTO fecha (idDiaSemana, Fecha, Hora, Mes, Año, Semestre, Trimestre)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (dia_semana, fecha, hora, mes, año, semestre, trimestre))
                conn.commit()
                idFecha = cursor.lastrowid

            cursor.execute("""
                INSERT INTO tendencia (idFecha, idMoneda, Variación, PrecioPromedio)
                VALUES (%s, %s, %s, %s)
            """, (idFecha, idMoneda, variacion, precio))

        conn.commit()
        cursor.close()
        conn.close()

    except mysql.connector.Error as e:
        print(e)

id_moneda = df[df["Moneda"] == filtro_moneda]["idMoneda"].iloc[0]
RealizarPredicción(forecast, int(id_moneda))



#NOTE: --------------------------------------------------------------- CLASIFICACIÓN DE INVERSIONES
#! ---------------- SECCIÓN 2: CLASIFICACIÓN DE INVERSIONES ----------------
st.subheader("📊 2. Clasificación de Inversión")
st.markdown("<br>", unsafe_allow_html=True)
#! ---------------- GRAFICAS EN FILA ----------------
col_rangos, col_kmeans = st.columns(2)
#NOTE: ---------------------------------------------------------------- RANGOS FIJOS
with col_rangos:
    st.markdown("#### A. Clasificación por Rangos Fijos")
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

    st.plotly_chart(fig_fijo, use_container_width=True)

#NOTE: ---------------------------------------------------------------- AGRUÁCION POR KMEANS
with col_kmeans:
    st.markdown("#### B. Agrupamiento con KMeans")
    try:
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