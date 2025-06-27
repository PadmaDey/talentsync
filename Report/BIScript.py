import os
import pandas as pd
from pymongo import MongoClient

mongo_url = input("Enter the Mongo connection string: ")
client = MongoClient(mongo_url)
print("Python-MongoDB connection created succesfully -\n", client)

db = client['talentsync-backend-production']
collection_names = db.list_collection_names()

for coll_name in collection_names:
    collection = db[coll_name]
    documents = list(collection.find({}))

    if not documents:
        continue

    df = pd.json_normalize(documents)

    output_path = (f"{coll_name}.csv")
    df.to_csv(output_path, index=False)

