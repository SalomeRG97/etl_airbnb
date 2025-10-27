import pandas as pd
import logging
from datetime import datetime
import os

from scr.logs import Logs

class Transformacion:
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.logs = Logs(file_name="transformacion")
        self.logs.log(f"Transformacion iniciada. Registros iniciales: {self.df.shape[0]}", "info")

    def limpiar_duplicados(self):
        antes = self.df.shape[0]
        self.df.drop_duplicates(inplace=True)
        despues = self.df.shape[0]
        self.logs.log(f"Duplicados eliminados: {antes - despues}", "info")

    def limpiar_nulos(self, columnas: list):
        for col in columnas:
            n_nulos = self.df[col].isna().sum()
            self.df[col].fillna("", inplace=True)
            self.logs.log(f"Nulos en columna {col}: {n_nulos} reemplazados por ''", "info")

    def normalizar_precios(self, col="price"):
        if col in self.df.columns:
            # Reemplaza símbolos $ y comas
            self.df[col] = (
                self.df[col]
                .replace({r"\$": "", ",": ""}, regex=True)
                .replace("", pd.NA)  # Reemplaza vacíos por NaN
            )
            # Convierte a float ignorando errores
            self.df[col] = pd.to_numeric(self.df[col], errors="coerce")
            self.logs.log(f"Columna {col} normalizada a float (NaN para valores inválidos)", "info")

    def convertir_fechas(self, col="last_review"):
        if col in self.df.columns:
            self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
            self.logs.log(f"Columna {col} convertida a datetime", "info")

    def derivar_columnas_fecha(self, col="last_review"):
        if col in self.df.columns:
            self.df["year"] = self.df[col].dt.year
            self.df["month"] = self.df[col].dt.month
            self.df["day"] = self.df[col].dt.day
            self.df["quarter"] = self.df[col].dt.quarter
            self.logs.info(f"Columnas año, mes, día y trimestre derivadas de {col}")

    def guardar_csv(self, ruta="../data/listings_limpio.csv"):
        os.makedirs(os.path.dirname(os.path.abspath(ruta)), exist_ok=True)
        self.df.to_csv(ruta, index=False)
        self.logs.log(f"DataFrame limpio guardado en {ruta}", "info")
