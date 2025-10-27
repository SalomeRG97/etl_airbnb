# ETL de Datos Airbnb México

## Descripción
Este proyecto realiza un **ETL completo** (Extracción, Transformación y Carga) de los datos de Airbnb México.

- Los datos originales son archivos **CSV comprimidos (`.csv.gz`)** descargados desde Airbnb.
- Se almacenan temporalmente en **MongoDB** para facilitar consultas y transformaciones.
- Se generan **DataFrames limpios**, listos para análisis avanzado o carga en un Data Warehouse.
- Se registra toda la actividad de transformación en **logs**.

## Estructura del Proyecto
```bash
etl_airbnb/
├── data/
│ ├── listings.csv.gz 
│ ├── reviews.csv.gz 
│ ├── calendar.csv.gz 
│ └── listings_limpio.csv 
├── logs/
├── scr/ 
│ ├── init.py
│ ├── extraccion.py
│ ├── main.py
│ └── transformacion.py
├── notebooks/
│ └── exploracion_airbnb.ipynb
└── README.md
```

## Requisitos

- **Python 3.10+**
- Paquetes Python:
- pip install pandas pymongo
- Jupyter Notebook (opcional, para EDA)
- MongoDB (local o vía Docker)

## Uso
- **Ejecutar ETL desde terminal**
- python scr/main.py
- Esto realizará:
- Conexión a MongoDB.
- Extracción de las colecciones listings, reviews y calendar.
- Transformación de los datos:
- Limpieza de nulos y duplicados.
- Normalización de precios.
- Conversión de fechas a formato ISO.
- Derivación de columnas temporales (año, mes, día, trimestre).
- Guardado de CSV limpios en data/.

## Logs
- Los logs se generan en logs/.
- Registran:
- Transformaciones aplicadas.
- Cantidad de registros antes y después de cada limpieza.
- Errores de conversión y valores nulos.
- Columnas derivadas.
