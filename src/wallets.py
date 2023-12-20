import os
import pytz
import json
import logging
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

import pymongo
from pymongo.collection import Collection
import pandas as pd
from multithread_processing.base_job import BaseJob

TIME_ZONE = pytz.timezone("Asia/Ho_Chi_Minh")
logger = logging.getLogger("wallet")


class GetWallets(BaseJob):
    def __init__(
            self,
            type: str,
            collection: Collection,
            user_addresses: list[str], 
            output_path: str,
            batch_size: int=2500, 
            max_workers: int=4):
        super().__init__(user_addresses, batch_size, max_workers)

        self.type = type
        self.collection = collection
        self.output_path = output_path
        self.results = []

    def _execute_batch(self, works: list[str]):
        if self.type == "wallets":
            records_gen = self.collection.find({"address": {"$in": works}}).batch_size(1000)
        elif self.type == "multichain_wallets":
            records_gen = self.collection.find({"_id": {"$in": works}}).batch_size(1000)
        else:
            raise NotImplementedError()

        records = []
        for record in records_gen:
            records.append(record)
            logger.debug(f"Got {len(records)} records")
        
        self.results += records

    def _end(self):
        super()._end()

        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        df = pd.DataFrame(self.results)
        # df.to_parquet(self.output_path)
        df.to_csv(self.output_path, index=False)


if __name__ == "__main__":
    import sys
    sys.path.append(os.path.abspath(os.path.join(__file__, "..")))
    
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument("-l", "--log-level", default="info")
    parser.add_argument("-c", "--collection", choices=["wallets", "multichain_wallets"], default="wallets")
    args = parser.parse_args()
    collection_name = args.collection

    from utils.logger import setup_logging
    setup_logging(log_dir=f"outputs/logs/{collection_name}", include_time=True, log_level=args.log_level)

    # -- Init mongo database connection
    MONGO_KG_CONNECTION_URL = os.getenv("MONGO_KG_CONNECTION_URL")
    assert MONGO_KG_CONNECTION_URL is not None
    mongo_client = pymongo.MongoClient(MONGO_KG_CONNECTION_URL)

    db = mongo_client.get_database("knowledge_graph")
    collection = db.get_collection(collection_name)

    # -- Load user addresses
    with open("./data/participants.json", encoding="utf-8") as f:
        participants = json.load(f)

    user_addresses = []
    for project, users in participants.items():
        logger.info("Number of users in project '%s' = %d", project, len(users))
        user_addresses += users
    user_addresses = list(set(user_addresses))
    logger.info("Total number of users: %d", len(user_addresses))

    # -- Run task
    task = GetWallets(collection_name, collection, user_addresses, f"./data/wallets/raw_{collection_name}.parquet")
    task.run()