import json
import pandas as pd
from pymongo import MongoClient

with open("./data/projects.json", encoding="utf-8") as f:
    kg = json.load(f)
    
df = pd.DataFrame(kg).explode("deployedChains")

top_5_project = ['ens', 'cryptokitties', 'sandbox', 'otherdeed', 'clonex']

top_5_project_df = df[df["id"].isin(top_5_project)][['id','contract_addresses','deployedChains']].iloc[1:6,]

# Connect to MongoDB
client = MongoClient('mongodb://etlReaderAnalysis:etl_reader_analysis__Gr2rEVBXyPWzIrP@34.126.84.83:27017,34.142.204.61:27017,34.142.219.60:27017/')
db = client["ethereum_blockchain_etl"]  
collection = db["transactions"] 

test_res = {}

# Define the query
for i in range(3,4):
    print("Start with row " + str(i))
    
    try:
        pipeline = [
            {"$match": {"to_address": {"$in": top_5_project_df.iloc[i,1]}}},
            {"$group": {"_id": "$from_address"}}
        ]

        # Execute the aggregation pipeline
        distinct_from_addresses = list(collection.aggregate(pipeline, batchSize=1000, maxTimeMS=50000))

        # Extract the distinct values
        distinct_from_addresses_values = [item["_id"] for item in distinct_from_addresses]
        
        test_res[top_5_project_df.iloc[i,0]] = distinct_from_addresses

    except:
        # Handle the timeout exception
        print(f"Timeout occurred for batch, continuing to the next batch.")
        
    print("Done with row " + str(i))

# Specify the file path
file_path = "./data/parti_sample.json"

# Save the dictionary to a JSON file
with open(file_path, 'w') as json_file:
    json.dump(test_res, json_file)

print(f'Dictionary saved to {file_path}')