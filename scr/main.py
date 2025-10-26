from extraccion import Extraccion
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