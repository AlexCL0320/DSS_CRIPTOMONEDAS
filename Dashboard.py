import pandas as pd
from sqlalchemy import create_engine

def cargar_datos():
    try:
        print("🔄 Conectando a la base de datos...")
        engine = create_engine("mysql+pymysql://root:Ha1Da40318@localhost/dss_criptomonedas")

        query = """
            SELECT 
                m.Nombre AS Moneda,
                f.Fecha,
                t.Precio_Cierre,
                t.Precio_Apertura,
                t.Volumen,
                t.Precio_Maximo,
                t.Precio_Minimo,
                f.Mes,
                f.Año,
                f.Trimestre,
                f.Semestre,
                f.Hora,
                f.idFecha,
                m.idMoneda,
                t.idTransaccion,
                t.Precio_Cierre AS Inversion
            FROM transaccion t
            JOIN fecha f ON t.idFecha = f.idFecha
            JOIN moneda m ON t.idMoneda = m.idMoneda
        """

        df = pd.read_sql(query, engine)
        print("✅ Consulta realizada. Filas cargadas:", len(df))
        return df

    except Exception as e:
        print(f"❌ Error: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    df = cargar_datos()

    if df.empty:
        print("⚠️ No se cargaron datos.")
    else:
        print("📋 Primeras filas del DataFrame:")
        print(df.head())
