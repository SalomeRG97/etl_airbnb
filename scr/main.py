from .extraccion import Extraccion
import pandas as pd

# Datos para conectar a mongodb
uri = "mongodb://localhost:27017/"
database = "local"
listings_collection_name = "listings"
reviews_collection_name = "reviews"
calendar_collection_name = "calendar"

db = Extraccion().connect_to_mongodb(uri, database)

listings_collection = Extraccion().get_collection(db, listings_collection_name)
reviews_collection = Extraccion().get_collection(db, reviews_collection_name)
calendar_collection = Extraccion().get_collection(db, calendar_collection_name)

listings_docs = Extraccion().get_documents_to_df(listings_collection)
reviews_docs = Extraccion().get_documents_to_df(reviews_collection)
calendar_docs = Extraccion().get_documents_to_df(calendar_collection)

print("Listings:")
print(listings_docs)
print("Reviews:")
print(reviews_docs)
print("Calendar:")
print(calendar_docs)

def ejecutar_etl():
    # Datos para conectar a MongoDB
    uri = "mongodb://localhost:27017/"
    database = "airbnb"
    listings_collection_name = "listings"
    reviews_collection_name = "reviews"
    calendar_collection_name = "calendar"

    extraccion = Extraccion()
    db = extraccion.connect_to_mongodb(uri, database)

    listings_col = extraccion.get_collection(db, listings_collection_name)
    reviews_col = extraccion.get_collection(db, reviews_collection_name)
    calendar_col = extraccion.get_collection(db, calendar_collection_name)

    listings_df = extraccion.get_documents_to_df(listings_col)
    reviews_df = extraccion.get_documents_to_df(reviews_col)
    calendar_df = extraccion.get_documents_to_df(calendar_col)

    return listings_df, reviews_df, calendar_df


# Solo se ejecuta si se corre desde Bash
if __name__ == "__main__":
    l, r, c = ejecutar_etl()
    print("Listings:", l.shape)
    print("Reviews:", r.shape)
    print("Calendar:", c.shape)
