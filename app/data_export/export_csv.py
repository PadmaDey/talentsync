import os
import pandas as pd # type: ignore
from app.db.connection import client
from dotenv import load_dotenv

load_dotenv()

def export_coll_to_csv(output_dir: str = "production_data/raw_data_dump"):
    # print(f"Exporting to: {output_dir}")  

    os.makedirs(output_dir, exist_ok=True)
    print("Folder created succesfully")

    db = client['talentsync-backend-production']
    collection_names = db.list_collection_names()
    # print(collection_names) 

    for coll_name in collection_names:
        collection = db[coll_name]
        documents = list(collection.find({}))
        # print(f"Collection {coll_name}: {len(documents)} documents")  

        if not documents:
            # print(f"Skipped empty collection: {coll_name}")
            continue

        df = pd.json_normalize(documents)

        output_path = os.path.join(output_dir, f"{coll_name}.csv")
        df.to_csv(output_path, index=False)
        # print(f"Exported {coll_name} to {output_path}")

