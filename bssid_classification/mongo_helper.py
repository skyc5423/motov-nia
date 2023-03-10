'''
File: mongo_helper.py
Project: nia
File Created: 2022-11-26 16:14:02
Author: sangminlee
-----
This script ...
Reference
...
'''

import pymongo
import pandas as pd


class MongoHelper(object):
    MONGO_URL = 'mongodb://localhost:27017'

    def __init__(self, db_name='NIA', user_id=None, password=None):
        con = pymongo.MongoClient(self.MONGO_URL)
        # con = pymongo.MongoClient(self.MONGO_URL % (user_id, password))
        self.db_name = db_name
        self.db = con[self.db_name]

    def query(self, collection_name, condition=None):
        return self.query_key(collection_name, [], condition)

    def query_key(self, collection_name, key_list, condition=None):
        key_dict = {'_id': 0}
        for key in key_list:
            key_dict[key] = 1
        collection = self.db[collection_name]
        if condition is None:
            cursor = collection.find({}, key_dict)
        else:
            cursor = collection.find(condition, key_dict)
        return pd.DataFrame([doc for doc in cursor])

    def create_collection(self, collection_name, data_frame):
        new_collection = self.db[collection_name]
        new_collection.insert_many(data_frame.to_dict('records'))

    def update_collection(self, collection_name, condition, new_val):
        collection = self.db[collection_name]
        new_val = {"$set": new_val}
        collection.update_one(condition, new_val)

    def add_element_collection(self, collection_name, condition, new_val):
        collection = self.db[collection_name]
        new_val = {"$addToSet": new_val}
        collection.update_one(condition, new_val)

    def query_with_id(self):
        return

    def add_document(self, collection, document):
        return self.db[collection].insert_one(document)

    def aggregation(self, collection_name, pipeline):
        collection = self.db[collection_name]
        cursor = collection.aggregate(pipeline)
        rtn = []
        for doc in cursor:
            rtn.append(doc)
        return pd.DataFrame(rtn)
