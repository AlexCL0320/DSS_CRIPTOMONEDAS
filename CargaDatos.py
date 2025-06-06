import mysql.connector
import pandas as pd

# -------------------------------
# 1. CONEXIÓN A LA BASE DE DATOS
# -------------------------------

conn = mysql.connector.connect(
    host="localhost",
    user="tu_usuario",         # 🔁 CAMBIA ESTO
    password="tu_contraseña",  # 🔁 CAMBIA ESTO
)

cur = conn.cursor()

# Crear la base de datos si no existe
cur.execute("CREATE DATABASE IF NOT EXISTS criptomonedas_db")
cur.execute("USE criptomonedas_db")

# -------------------------------
# 2. CREACIÓN DE TABLAS
# -------------------------------

cur.execute("""
CREATE TABLE IF NOT EXISTS DiaSemana (
    idDiaSemana INT AUTO_INCREMENT PRIMARY KEY,
    NombreDia VARCHAR(15) NOT NULL
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS Fecha (
    idFecha INT AUTO_INCREMENT PRIMARY KEY,
    idDiaSemana INT,
    Fecha DATE NOT NULL,
    Hora TIME NOT NULL,
    Mes VARCHAR(20) NOT NULL,
    Año INT NOT NULL,
    Semestre INT NOT NULL,
    Trimestre INT NOT NULL,
    FOREIGN KEY (idDiaSemana) REFERENCES DiaSemana(idDiaSemana)
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS Moneda (
    idMoneda INT AUTO_INCREMENT PRIMARY KEY,
    PrecioInicial DECIMAL(18,4) NOT NULL,
    Nombre VARCHAR(30) NOT NULL UNIQUE,
    Simbolo VARCHAR(5) NOT NULL
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS Transaccion (
    idTransaccion INT AUTO_INCREMENT PRIMARY KEY,
    idFecha INT,
    idMoneda INT,
    Precio_Apertura DECIMAL(18,4) NOT NULL,
    Precio_Cierre DECIMAL(18,4) NOT NULL,
    Precio_Maximo DECIMAL(18,4) NOT NULL,
    Precio_Minimo DECIMAL(18,4) NOT NULL,
    Volumen DECIMAL(18,4) NOT NULL,
    FOREIGN KEY (idFecha) REFERENCES Fecha(idFecha),
    FOREIGN KEY (idMoneda) REFERENCES Moneda(idMoneda)
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS Tendencia (
    idTendencia INT AUTO_INCREMENT PRIMARY KEY,
    idFecha INT,
    idMoneda INT,
    Variación DECIMAL(18,4) NOT NULL,
    PrecioPromedio DECIMAL(18,4) NOT NULL,
    FOREIGN KEY (idFecha) REFERENCES Fecha(idFecha),
    FOREIGN KEY (idMoneda) REFERENCES Moneda(idMoneda)
);
""")

conn.commit()
print("✅ Tablas creadas con éxito")

# -------------------------------
# 3. CARGA DE CSV
# -------------------------------

df = pd.read_csv("ARCHIVO_ATOMIZADO.csv")

# Verifica las columnas
print("🔍 Columnas detectadas:", df.columns.tolist())
print(df.head(3))

# -------------------------------
# 4. CARGA DE DATOS A TABLAS
# -------------------------------

# Inserta días de la semana únicos
dias = df['NombreDia'].dropna().unique()
for dia in dias:
    cur.execute("INSERT IGNORE INTO DiaSemana (NombreDia) VALUES (%s)", (dia,))

# Inserta monedas únicas
monedas = df[['Nombre', 'Simbolo', 'PrecioInicial']].drop_duplicates()
for _, row in monedas.iterrows():
    cur.execute("INSERT IGNORE INTO Moneda (Nombre, Simbolo, PrecioInicial) VALUES (%s, %s, %s)",
                (row['Nombre'], row['Simbolo'], row['PrecioInicial']))

# Inserta fechas
fechas = df[['Fecha', 'Hora', 'Mes', 'Año', 'Semestre', 'Trimestre', 'NombreDia']].drop_duplicates()
for _, row in fechas.iterrows():
    # Obtener idDiaSemana
    cur.execute("SELECT idDiaSemana FROM DiaSemana WHERE NombreDia = %s", (row['NombreDia'],))
    id_dia = cur.fetchone()[0]

    cur.execute("""
        INSERT IGNORE INTO Fecha (idDiaSemana, Fecha, Hora, Mes, Año, Semestre, Trimestre)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (id_dia, row['Fecha'], row['Hora'], row['Mes'], row['Año'], row['Semestre'], row['Trimestre']))

conn.commit()
print("📆 Fechas y monedas insertadas.")

# Mapas para recuperar idFecha y idMoneda
cur.execute("SELECT idFecha, Fecha, Hora FROM Fecha")
map_fechas = {(str(f) + str(h)): i for i, f, h in cur.fetchall()}

cur.execute("SELECT idMoneda, Nombre FROM Moneda")
map_monedas = {n: i for i, n in cur.fetchall()}

# Inserta transacciones y tendencias
for _, row in df.iterrows():
    key_fecha = str(row['Fecha']) + str(row['Hora'])
    id_fecha = map_fechas.get(key_fecha)
    id_moneda = map_monedas.get(row['Nombre'])

    if pd.notna(row['Precio_Apertura']):
        cur.execute("""
            INSERT INTO Transaccion (idFecha, idMoneda, Precio_Apertura, Precio_Cierre, Precio_Maximo, Precio_Minimo, Volumen)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            id_fecha, id_moneda,
            row['Precio_Apertura'], row['Precio_Cierre'],
            row['Precio_Maximo'], row['Precio_Minimo'], row['Volumen']
        ))

    if pd.notna(row['Variación']):
        cur.execute("""
            INSERT INTO Tendencia (idFecha, idMoneda, Variación, PrecioPromedio)
            VALUES (%s, %s, %s, %s)
        """, (
            id_fecha, id_moneda,
            row['Variación'], row['PrecioPromedio']
        ))

conn.commit()
cur.close()
conn.close()

print("✅ Datos cargados correctamente.")


#Prubas