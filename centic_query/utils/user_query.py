import os
from dotenv import load_dotenv
import pandas as pd
# Load environment variables from .env file
load_dotenv()
import pymongo
def user_query():
    client = pymongo.MongoClient(MONGODB)
    db = client.blockchain_etl
    collection = db.transactions
    cursor = collection.find()
    result = list(cursor)
    df_subgraph = pd.DataFrame(result)
