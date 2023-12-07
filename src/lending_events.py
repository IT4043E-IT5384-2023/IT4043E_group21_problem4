import os
import json
import logging
import pytz
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
# from datetime import datetime
from argparse import ArgumentParser

import pymongo
import pandas as pd
from multithread_processing.base_job import BaseJob

from utils.logger import setup_logging


TIME_ZONE = pytz.timezone("Asia/Ho_Chi_Minh")
logger = logging.getLogger("LendingEvents")


class QueryLendingEvents:
    @classmethod
    def get_query(cls, event_type: str, users: list[str]):
        if hasattr(cls, event_type) is False:
            raise ValueError(f"Event type {event_type} not found")
        return getattr(cls, event_type)(users)
    
    @classmethod
    def borrow(cls, users: list[str]):
        return {
            "event_type": "BORROW",
            "on_behalf_of": {
                "$in": users
            }
        }
    
    @classmethod
    def deposit(cls, users: list[str]):
        return {
            "event_type": "DEPOSIT",
            "on_behalf_of": {
                "$in": users
            }
        }
    
    @classmethod
    def liquidate(cls, users: list[str]):
        return {
            "event_type": "LIQUIDATE",
            "liquidator": {
                "$in": users
            }
        }
    
    @classmethod
    def repay(cls, users: list[str]):
        return {
            "event_type": "REPAY",
            "user": {
                "$in": users
            }
        }
    
    @classmethod
    def withdraw(cls, users: list[str]):
        return {
            "event_type": "WITHDRAW",
            "user": {
                "$in": users
            }
        }
    
    @classmethod
    def flashloan(cls, users: list[str]):
        return {
            "event_type": "FLASHLOAN",
            "initiator": {
                "$in": users
            }
        }


class GetLendingEvents(BaseJob):
    def __init__(self, collection, user_addresses: list[str], event_type: str, output_path: str, max_workers=4, batch_size=2500, verbose: bool=False):
        super().__init__(work_iterable=user_addresses, max_workers=max_workers, batch_size=batch_size)

        self.collection = collection
        self.output_path = output_path
        self.event_type = event_type.lower()
        self.results = []
        self.verbose = verbose

    def _execute_batch(self, works: list[str]):
        r"""
        
        Parameters
        ----------
            works: list[str]
                Batch of ``user_addresses``
        """

        records_gen = self.collection.find(QueryLendingEvents.get_query(self.event_type, works), {"_id": 0}).batch_size(1000)
        # records_gen = self.collection.find({}, {"_id": 0}).batch_size(1000)
        records = []
        for record in records_gen:
            records.append(record)
            if self.verbose:
                logger.info(f"Got {len(records)} records")

        self.results.append(pd.DataFrame(records))

    def _end(self):
        super()._end()

        df = pd.concat(self.results, ignore_index=True)
        os.makedirs(os.path.dirname(os.path.abspath(self.output_path)), exist_ok=True)
        df.to_csv(self.output_path)


if __name__ == "__main__":
    setup_logging(log_dir="outputs/logs/user_txs", include_time=True)
    parser = ArgumentParser()
    # parser.add_argument("-s", "--start-date", type=str, default="15/08/2023")
    # parser.add_argument("-e", "--end-date", type=str, default="15/11/2023")
    parser.add_argument("-o", "--output-csv-path", type=str, default="data/lending_events.csv")
    parser.add_argument("-t", "--event-type", type=str, required=True)
    parser.add_argument("-b", "--batch-size", type=int, default=2500)
    parser.add_argument("-j", "--max-workers", type=int, default=4)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    # -- Parser date time
    # start_date  = args.start_date
    # end_date    = args.end_date
    # start_date  = [int(d) for d in args.start_date.split("/")][::-1]
    # end_date    = [int(d) for d in args.end_date.split("/")][::-1]
    # start_date  = datetime(*start_date, 0, 0, 0, tzinfo=TIME_ZONE)
    # end_date    = datetime(*end_date, 0, 0, 0, tzinfo=TIME_ZONE)

    # -- Init mongo database connection
    MONGO_CONNECTION_URL = os.getenv("MONGO_CONNECTION_URL")
    assert MONGO_CONNECTION_URL is not None
    mongo_client = pymongo.MongoClient(MONGO_CONNECTION_URL)

    db = mongo_client.get_database("ethereum_blockchain_etl")
    collection = db.get_collection("lending_events")

    # -- Load user addresses
    with open("./data/participants.json", encoding="utf-8") as f:
        participants = json.load(f)

    user_addresses = []
    for project, users in participants.items():
        logger.info("Number of users in project '%s' = %d", project, len(users))
        user_addresses += users

    # -- Start tasks
    task = GetLendingEvents(
        collection, 
        user_addresses, 
        event_type=args.event_type, 
        output_path=args.output_csv_path, 
        max_workers=args.max_workers, 
        batch_size=args.batch_size,
        verbose=args.verbose,
    )
    task.run()
