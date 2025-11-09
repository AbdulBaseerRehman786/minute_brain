from pymongo import MongoClient
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

mongo_uri = os.getenv("MONGO_URI")
db_name = os.getenv("DB_NAME")

print("Loaded MONGO_URI:", repr(mongo_uri))
print("Loaded DB_NAME:", repr(db_name))
print("Type of MONGO_URI:", type(mongo_uri))

if not mongo_uri or not isinstance(mongo_uri, str):
    raise ValueError("❌ MONGO_URI is not loaded properly or is not a string")

client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
db = client[db_name]
print("✅ Connected successfully to MongoDB Atlas")

