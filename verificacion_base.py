import mysql.connector
import pandas as pd

#& Establecemos la conexion a la base de datosS

conn = mysql.connector.connect(
    host="localhost",
    user="root",         
    password="ROOT",
    charset='utf8mb4',
    use_unicode=True
)

conexion = conn.cursor()
print("Conexion Exitosa")



