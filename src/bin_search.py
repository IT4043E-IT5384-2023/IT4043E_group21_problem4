import os
import pytz
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
from datetime import datetime

import pymongo


TIME_ZONE = pytz.timezone("Asia/Ho_Chi_Minh")


def main():
    MONGO_RAW_CONNECTION_URL = os.getenv("MONGO_RAW_CONNECTION_URL")
    MONGO_RAW_CONNECTION_URL="mongodb://etlReaderAnalysis:etl_reader_analysis__Gr2rEVBXyPWzIrP@34.126.84.83:27017,34.142.204.61:27017,34.142.219.60:27017/"
    assert MONGO_RAW_CONNECTION_URL is not None
    mongo_client = pymongo.MongoClient(MONGO_RAW_CONNECTION_URL)

    db = mongo_client.get_database("ethereum_blockchain_etl")
    collection = db.get_collection("transactions")

    start_timestamp = int(datetime(2023, 8, 15, 0, 0, 0, tzinfo=TIME_ZONE).timestamp())
    end_timestamp   = int(datetime(2023, 11, 15, 0, 0, 0, tzinfo=TIME_ZONE).timestamp())

    projection = {
        "block_timestamp": 1,
    }

    left = 0
    right = collection.estimated_document_count()
    position = -1

    while left <= right:
        mid = (left + right) // 2
        print("mid =", mid)

        for item in collection.find({}, projection).sort("block_timestamp", pymongo.ASCENDING).limit(1).skip(mid):
            block_timestamp = item["block_timestamp"]
            if start_timestamp <= block_timestamp:
                position = mid
                print("position =", position)
                right = mid - 1
            else:
                left = mid + 1

    print("final =", position)


if __name__ == "__main__":
    main()