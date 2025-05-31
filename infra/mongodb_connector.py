import os
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure


class MongoDBConnector:
    """
    Connector class for MongoDB operations
    """
    def __init__(self, uri=None, db_name=None):
        self.uri = uri or os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
        self.db_name = db_name or os.getenv('MONGO_DATABASE', 'jobs_db')
        self.client = None
        self.db = None
        self.connect()

    def connect(self):
        """
        Establish connection to MongoDB
        """
        try:
            self.client = MongoClient(self.uri)
            self.db = self.client[self.db_name]
            # Test connection
            self.client.admin.command('ping')
            print(f"Successfully connected to MongoDB at {self.uri}")
        except ConnectionFailure as e:
            print(f"Failed to connect to MongoDB: {e}")
            raise

    def close(self):
        """
        Close the MongoDB connection
        """
        if self.client:
            self.client.close()
            print("MongoDB connection closed")

    def insert_one(self, collection_name, item):
        """
        Insert a single document into a collection
        """
        collection = self.db[collection_name]
        result = collection.insert_one(item)
        return result.inserted_id

    def insert_many(self, collection_name, items):
        """
        Insert multiple documents into a collection
        """
        collection = self.db[collection_name]
        result = collection.insert_many(items)
        return result.inserted_ids

    def find_one(self, collection_name, query=None):
        """
        Find one document in a collection
        """
        collection = self.db[collection_name]
        return collection.find_one(query or {})

    def find_many(self, collection_name, query=None, limit=None):
        """
        Find multiple documents in a collection
        """
        collection = self.db[collection_name]
        cursor = collection.find(query or {})
        if limit:
            cursor = cursor.limit(limit)
        return list(cursor)

    def count(self, collection_name, query=None):
        """
        Count documents in a collection
        """
        collection = self.db[collection_name]
        return collection.count_documents(query or {})

    def update_one(self, collection_name, query, update_data):
        """
        Update a single document in a collection
        """
        collection = self.db[collection_name]
        result = collection.update_one(query, {"$set": update_data})
        return result.modified_count

    def delete_one(self, collection_name, query):
        """
        Delete a single document from a collection
        """
        collection = self.db[collection_name]
        result = collection.delete_one(query)
        return result.deleted_count


