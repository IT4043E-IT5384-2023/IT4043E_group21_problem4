import os
from dotenv import load_dotenv
import pandas as pd
# Load environment variables from .env file
load_dotenv()
import pymongo
def user_query():
    client = pymongo.MongoClient(MONGODB)
    db = client.blockchain_etl
    collection = db.subgraphs_2
    cursor = collection.find()
    result = list(cursor)
    df_subgraph = pd.DataFrame(result)
    df_pairs = pd.read_csv("/kaggle/input/pairss/0x38_wallets_pairs.csv")
