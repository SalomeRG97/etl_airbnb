import pandas as pd
import logging
from datetime import datetime

class Logs:
    def __init__(self, nombre_archivo="transformacion"):
        fecha = datetime.now().strftime("%Y%m%d_%H%M")
        logging.basicConfig(
            filename=f"../logs/{nombre_archivo}_{fecha}.log",
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger()

class Transformacion:
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.logs = Logs().logger
        self.logs.info(f"Transformacion iniciada. Registros iniciales: {self.df.shape[0]}")

    def limpiar_duplicados(self):
        antes = self.df.shape[0]
        self.df.drop_duplicates(inplace=True)
        despues = self.df.shape[0]
        self.logs.info(f"Duplicados eliminados: {antes - despues}")

    def limpiar_nulos(self, columnas: list):
        for col in columnas:
            n_nulos = self.df[col].isna().sum()
            self.df[col].fillna("", inplace=True)
            self.logs.info(f"Nulos en columna {col}: {n_nulos} reemplazados por ''")

    def normalizar_precios(self, col="price"):
        if col in self.df.columns:
            self.df[col] = self.df[col].replace({'\$':'', ',':''}, regex=True).astype(float)
            self.logs.info(f"Columna {col} normalizada a float")

    def convertir_fechas(self, col="last_review"):
        if col in self.df.columns:
            self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
            self.logs.info(f"Columna {col} convertida a datetime")

    def derivar_columnas_fecha(self, col="last_review"):
        if col in self.df.columns:
            self.df["year"] = self.df[col].dt.year
            self.df["month"] = self.df[col].dt.month
            self.df["day"] = self.df[col].dt.day
            self.df["quarter"] = self.df[col].dt.quarter
            self.logs.info(f"Columnas año, mes, día y trimestre derivadas de {col}")

    def guardar_csv(self, ruta="../data/listings_limpio.csv"):
        self.df.to_csv(ruta, index=False)
        self.logs.info(f"DataFrame limpio guardado en {ruta}")
