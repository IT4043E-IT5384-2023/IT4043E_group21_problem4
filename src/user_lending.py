import json
import pandas as pd
from pymongo import MongoClient

def query_user_lending_events():
    # Connect to MongoDB
    client = MongoClient('mongodb://etlReaderAnalysis:etl_reader_analysis__Gr2rEVBXyPWzIrP@34.126.84.83:27017,34.142.204.61:27017,34.142.219.60:27017/')
    db = client["ethereum_blockchain_etl"]  
    db_lending_events = db["lending_events"] 

    # Specify the path to your JSON file
    json_file_path = "participants.json"

    # Open the file and load its content into a dictionary
    with open(json_file_path, 'r') as json_file:
        data_as_dict = json.load(json_file)

    lending = {}
    project = 'ens'
    print("Start with " + project)
    res = []
    start = 0
    end = 100
    try:
        query = {
            "user": {"$in": data_as_dict[project][start:end]}
        }

        # Execute the query
        result = db_lending_events.find(query)

        # Print the result
        for document in result:
            res.append(document)
        res_df = pd.DataFrame(res)
        lending[project] = res_df

        if i % 2000 == 0:
            print("Done {}%".format(end / len(data_as_dict[project]) * 100))

    except:
        # Handle the timeout exception
        print(f"Timeout occurred for batch {start}-{end}, continuing to the next batch.")
        print(start, end)

    print("Done with " + project)
        
    # Specify the file path
    file_path = "111.json"

    data_dict = lending[project].to_dict(orient='records')

    # Save the dictionary to a JSON file
    with open(file_path, 'w') as json_file:
        json.dump(data_dict, json_file)

    print(f'Dictionary saved to {file_path}')


if __name__ == "__main__":
    query_user_lending_events()