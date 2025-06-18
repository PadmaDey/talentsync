output_dir = "../production_data/raw_data_dump"
os.makedirs(output_dir, exist_ok=True)

db = client['talentsync-backend-production']
collection_names = db.list_collection_names()

for coll_name in collection_names:
    collection = db[coll_name]
    documents = list(collection.find({}))
    
    df = pd.json_normalize(documents)
    
    output_path = os.path.join(output_dir, f"{coll_name}.csv")
    df.to_csv(output_path, index=False)
