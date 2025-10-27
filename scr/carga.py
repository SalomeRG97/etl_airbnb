import pandas as pd
import sqlite3
import logging
import os

from scr.logs import Logs

class Carga:
    def __init__(self, archivo_csv, nombre_bd="airbnb_cdmx.db", archivo_excel="listings_summary_cargado.xlsx"):
        self.archivo_csv = archivo_csv
        self.nombre_bd = nombre_bd
        self.archivo_excel = archivo_excel
        self.logs = Logs("carga")

        self.logs.log("Inicio del proceso de carga", "info")

    def crear_conexion_sqlite(self):
        try:
            conn = sqlite3.connect(self.nombre_bd)
            self.logs.log("Conexión a SQLite establecida correctamente.", "info")
            return conn
        except Exception as e:
            self.logs.log(f"Error al conectar con SQLite: {e}", "error")
            raise

    def crear_tabla_listings(self, conn):
        try:
            df = pd.read_csv(self.archivo_csv)
            columnas = df.columns
            columnas_sql = ", ".join([f"'{col}' TEXT" for col in columnas])
            sql = f"CREATE TABLE IF NOT EXISTS listings_summary ({columnas_sql});"
            conn.execute(sql)
            conn.commit()
            self.logs.log("Tabla 'listings_summary' creada correctamente.", "info")
        except Exception as e:
            self.logs.log(f"Error al crear la tabla: {e}", "error")
            raise

    def insertar_datos_sqlite(self, conn):
        try:
            df = pd.read_csv(self.archivo_csv)
            df.to_sql("listings_summary", conn, if_exists="replace", index=False)
            conn.commit()
            filas = conn.execute("SELECT COUNT(*) FROM listings_summary").fetchone()[0]
            self.logs.log(f"{filas} registros insertados en la base de datos SQLite.", "info")
        except Exception as e:
            self.logs.log(f"Error al insertar los datos: {e}", "error")
            raise

    def exportar_excel(self):
        try:
            df = pd.read_csv(self.archivo_csv)
            df.to_excel(self.archivo_excel, index=False)
            self.logs.log(f"Datos exportados correctamente a {self.archivo_excel}.", "info")
        except Exception as e:
            self.logs.log(f"Error al exportar a Excel: {e}", "error")
            raise

    def ejecutar_carga(self):
        try:
            conn = self.crear_conexion_sqlite()
            self.crear_tabla_listings(conn)
            self.insertar_datos_sqlite(conn)
            self.exportar_excel()
            self.logs.log("Proceso de carga completado exitosamente.", "info")
            conn.close()
        except Exception as e:
            self.logs.log(f"Fallo en el proceso de carga: {e}", "error")
            raise


if __name__ == "__main__":
    ruta_csv = os.path.join("data", "listings_summary.csv")
    carga = Carga(archivo_csv=ruta_csv)
    carga.ejecutar_carga()
