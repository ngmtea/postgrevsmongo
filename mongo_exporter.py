import sys
from pymongo import MongoClient
from pymongo import UpdateOne

import logging
import time

_logger = logging.getLogger(__name__)


class TransferMongodbStreamingExporter(object):
    def __init__(
            self,
            collection_url: str = 'mongodb://mtea:mtea1211@178.128.93.195:27027',
            db_name: str = 'mtea',
            collection_name: str = 'importer_test',
    ) -> None:
        try:
            # FIXME Unable to read from .env file
            self.client: MongoClient = MongoClient(collection_url)
        except:
            sys.exit(1)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def export_items(self, items: list) -> None:
        _logger.info(f"*****  Start exporting events to MongoDB Database *****")
        _logger.info(f"Total items: {len(items)}")
        start_time = int(time.time())
        self.export_events(items)
        end_time = int(time.time())
        logging.info("Total time exporting to MongoDB Database: " + str(end_time - start_time))

    def export_events(self, operations_data: list) -> None:
        if not operations_data:
            return

        bulk_operation = [
            UpdateOne({"_id": data["_id"]}, {"$set": data}, upsert=True)
            for data in operations_data
        ]

        try:
            self.collection.bulk_write(bulk_operation)
        except Exception as bwe:
            print(bwe)

    def delete_events_for_testing(self):
        filter_: dict = {"block_number": {"$lte": 21175364}}
        self.collection.delete_many(filter=filter_)
