import os
import json
import logging
import pytz
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
from datetime import datetime

import pymongo

from utils.logger import setup_logging
from utils.indexed_datasets import IndexedDatasetBuilder, IndexedDataset


TIME_ZONE = pytz.timezone("Asia/Ho_Chi_Minh")
logger = logging.getLogger("user_transactions")


def main():
    MONGO_CONNECTION_URL = os.getenv("MONGO_CONNECTION_URL")
    assert MONGO_CONNECTION_URL is not None
    mongo_client = pymongo.MongoClient(MONGO_CONNECTION_URL)

    db = mongo_client.get_database("ethereum_blockchain_etl")
    collection = db.get_collection("transactions")

    start_timestamp = int(datetime(2023, 8, 15, 0, 0, 0, tzinfo=TIME_ZONE).timestamp())
    end_timestamp   = int(datetime(2023, 11, 15, 0, 0, 0, tzinfo=TIME_ZONE).timestamp())

    query = {
        "block_timestamp": {
            "$gte": start_timestamp, 
            "$lte": end_timestamp
        }
    }

    projection = {
        "_id": 1,
        "block_timestamp": 1,
        "from_address": 1,
        "to_address": 1,
        "value": 1,
        "gas": 1,
        "gas_price": 1,
        "related_addresses": 1,
        "transaction_type": 1
    }

    with open("./data/participants.json", encoding="utf-8") as f:
        participants = json.load(f)

    all_users = []
    for project, users in participants.items():
        logger.info("Number of users in project '%s' = %d", project, len(users))
        all_users += users

    all_users = set(all_users)


    logger.info("Collect user transactions from timestamp %d to %d", start_timestamp, end_timestamp)
    data_path = "./data/user_txs"
    builder = IndexedDatasetBuilder(data_path)

    batch_size = 10000

    try:
        for item in collection.find(query, projection).batch_size(batch_size):
            block_timestamp = item["block_timestamp"]
            block_id        = item["_id"]
            from_address    = item["from_address"]
            to_address      = item["to_address"]

            if from_address in users or to_address in users:
                logger.info("Fetch block id %s at timestamp %d", block_id, block_timestamp)
                builder.add_item(item)

    except KeyboardInterrupt:
        logger.error("KeyboardInterrupt at block timestamp %d", block_timestamp)
    except:
        logger.exception("Error")
        
    builder.finalize()
    logger.info("Finish")


def view():
    data = IndexedDataset("./data/user_txs.data")
    print(len(data))


if __name__ == "__main__":
    setup_logging(log_dir="outputs/logs/user_txs", include_time=True)
    main()
    view()