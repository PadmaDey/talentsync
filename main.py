import os
import pandas as pd # type: ignore
from pymongo import MongoClient # type: ignore

mongo_url = "mongodb://padma:d6g5h1dh18dh1d6h81fd86d1UBFIfguifd@3.146.176.180:27017/talentsync-backend-production"
client = MongoClient(mongo_url)

output_dir = "production_data/raw_data_dump"
os.makedirs(output_dir, exist_ok=True)

db = client['talentsync-backend-production']
collection_names = db.list_collection_names()

for coll_name in collection_names:
    collection = db[coll_name]
    documents = list(collection.find({}))
    
    df = pd.json_normalize(documents)
    
    output_path = os.path.join(output_dir, f"{coll_name}.csv")
    df.to_csv(output_path, index=False)
    print(f"Dumped: {output_path}")

print("\nAll collections dumped successfully!")
