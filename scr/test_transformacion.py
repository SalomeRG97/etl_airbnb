import pandas as pd
from transformacion import Transformacion

# Crear un DataFrame de ejemplo
data = {
    "id": [1, 2, 2, 3],
    "price": ["$120", "$200", "$200", None],
    "last_review": ["2025-01-01", "2025-05-15", None, "2025-08-09"],
    "name": ["Loft", "Apartamento", "Apartamento", None]
}

df = pd.DataFrame(data)
print("=== DataFrame Original ===")
print(df)

# Instanciar la clase de transformación
t = Transformacion(df)

# Aplicar métodos de limpieza
t.limpiar_duplicados()
t.limpiar_nulos(["name", "price"])
t.normalizar_precios("price")
t.convertir_fechas("last_review")
t.derivar_columnas_fecha("last_review")

# Guardar CSV limpio
t.guardar_csv("../data/listings_limpio.csv")

print("\n=== DataFrame Transformado ===")
print(t.df.head())
