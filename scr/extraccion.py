import pandas as pd

from pymongo import MongoClient
from pymongo.errors import ConfigurationError

from .logs import Logs

class Extraccion:
    def __init__(self):
        self.logs = Logs()

    def connect_to_mongodb(self, uri, database):
        try:
            client = MongoClient(uri)
            db = client[database]
            db.list_collection_names()
            self.logs.log(f"Connected to MongoDB database: {database}", "info")
            return db
        except ConnectionError as e:
            self.logs.log(f"Error connecting to MongoDB: {e}", "error")
            return None

    def get_collection(self, db, collection_name):
        return db[collection_name]

    def get_documents_to_df(self, collection, limit = 0):
        self.logs.log(f"Getting documents from collection: {collection.name}", "info")
        self.logs.log(f"Document size: {len(list(collection.find().limit(limit)))}", "info")
        return pd.DataFrame(list(collection.find().limit(limit)))

    def validate_df(self, df = pd.DataFrame()):
        if len(df) > 0:
            return True
        return False

