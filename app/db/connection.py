import os
from dotenv import load_dotenv # type: ignore
from pymongo import MongoClient # type: ignore

load_dotenv("app\env\.env")

mongo_url = os.getenv("mongo_url")
# print(mongo_url)
client = MongoClient(mongo_url)
# print("Python-MongoDB connection created succesfully -\n", client)
