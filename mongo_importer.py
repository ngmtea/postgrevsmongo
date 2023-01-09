import logging
import time
from pymongo import MongoClient

logger = logging.getLogger("Connect Mongo Database")
logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.INFO)


class Mongo:
    def __init__(self):
        self._conn = None
        url = f"mongodb://mtea:mtea1211@178.128.93.195:27027"
        self.connection = MongoClient(url)
        db_name = 'mtea'
        self.mongo_db = self.connection[db_name]
        self.transaction_collection = self.mongo_db['transfer_event_1day']

    def get_transactions_transfer_native(self, from_block, to_block):
        transactions = self.transaction_collection.find(
            {"$and": [{"value": {"$ne": "0"}},
                      {"block_number": {"$gte": from_block, "$lte": to_block}}]})
        return transactions

    def test(self):
        logger.info("Start processing")
        start_time = int(time.time())

        logger.info("Reading from MongoDB..")
        list_ = self.get_transactions_transfer_native(from_block=23884425, to_block=24086025)
        dict_ = {}
        read_db_time = int(time.time())
        logger.info(f"Reading from MongoDB completed, takes " + str(read_db_time - start_time) + "s")

        logger.info("Finding top transferred..")
        for event in list_:
                address = event['from_address']
                contract_address = event['contract_address']
                key = f'{address}_{contract_address}'
                if key not in dict_:
                    dict_[key] = {}
                    dict_[key]['total_transactions_count'] = 1
                    dict_[key]['total_transactions_value'] = event['value']
                else:
                    dict_[key]['total_transactions_count'] += 1
                    dict_[key]['total_transactions_value'] += event['value']
        sorted_dict_ft_count = dict(sorted(dict_.items(), key=lambda item: item[1]['total_transactions_count'], reverse=True))
        sorted_dict_ft_value = dict(sorted(dict_.items(), key=lambda item: item[1]['total_transactions_value'], reverse=True))

        top_10_count = list(sorted_dict_ft_count.items())[:10]
        top_10_value = list(sorted_dict_ft_value.items())[:10]

        end_time = int(time.time())
        logger.info("Finding top completed, takes  " + str(end_time - read_db_time) + "s")

        logger.info("Total time processed " + str(end_time - start_time) + "s")

        logger.info("Top 10 according to number of transactions:")
        logger.info(top_10_count)

        logger.info("Top 10 according to value of transactions:")
        logger.info(top_10_value)

    def get_transactions_by_token_address(self, token_address):
        transactions = self.transaction_collection.find(
            {"$and": [{"value": {"$ne": "0"}},
                      {"contract_address": token_address}]})
        return transactions

    def get_transactions_by_value(self, from_value, to_value):
        transactions = self.transaction_collection.find(
            {"$and": [{"value": {"$gte": from_value, "$lte": to_value}}]})
        return transactions

    def get_transactions_by_block_and_token_address(self, from_block, to_block, token_address):
        transactions = self.transaction_collection.find(
            {"$and": [{"value": {"$ne": "0"}},
                      {"block_number": {"$gte": from_block, "$lte": to_block}},
                      {"contract_address": token_address}]})
        return transactions

    def get_transactions_by_block_and_value(self, from_block, to_block, from_value, to_value):
        transactions = self.transaction_collection.find(
            {"$and": [{"value": {"$gte": from_value, "$lte": to_value}},
                      {"block_number": {"$gte": from_block, "$lte": to_block}}]})
        return transactions

    def get_transactions_by_token_address_and_value(self, token_address,from_value, to_value):
        transactions = self.transaction_collection.find(
            {"$and": [{"value": {"$ne": "0"}},
                      {"contract_address": token_address},
                      {"value": {"$gte": from_value, "$lte": to_value}}]})
        return transactions

    def get_transactions_by_all(self, token_address, from_block, to_block, from_value, to_value):
        transactions = self.transaction_collection.find(
            {"$and": [{"value": {"$ne": "0"}},
                      {"contract_address": token_address},
                      {"block_number": {"$gte": from_block, "$lte": to_block}},
                      {"value": {"$gte": from_value, "$lte": to_value}}]})
        return transactions

    def get_cur(self, query, from_block, to_block, token_address, from_value, to_value):
        global cur
        if query == "by_block":
            cur = self.get_transactions_transfer_native(from_block=from_block, to_block=to_block)
        elif query == "by_token_address":
            cur = self.get_transactions_by_token_address(token_address=token_address)
        elif query == "by_value":
            cur = self.get_transactions_by_value(from_value=from_value, to_value=to_value)
        elif query == "by_block_and_token_address":
            cur = self.get_transactions_by_block_and_token_address(from_block=from_block,
                                                                   to_block=to_block,
                                                                   token_address=token_address)
        elif query == "by_block_and_value":
            cur = self.get_transactions_by_block_and_value(from_block=from_block,
                                                           to_block=to_block,
                                                           from_value=from_value,
                                                           to_value=to_value)
        elif query == "by_token_address_and_value":
            cur = self.get_transactions_by_token_address_and_value(token_address=token_address,
                                                                   from_value=from_value,
                                                                   to_value=to_value)
        elif query == "by_all":
            cur = self.get_transactions_by_all(token_address=token_address,
                                               from_block=from_block,
                                               to_block=to_block,
                                               from_value=from_value,
                                               to_value=to_value)
        return cur

    def test_query(self, query):
        from_block = 24085024
        to_block = 24086024
        token_address = '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c'
        from_value = 1
        to_value = 5
        start_time = int(time.time())
        logger.info(f"Start processing, token address: {token_address}, total blocks 1001, value from {from_value} to {to_value}")

        logger.info("Query to MongoDB..")
        cur = self.get_cur(query,
                           from_block=from_block,
                           to_block=to_block,
                           token_address=token_address,
                           from_value=from_value,
                           to_value=to_value)
        read_db_time = int(time.time())
        logger.info(f"Query completed, takes " + str(read_db_time - start_time) + "s")

        logger.info("Converting Mongo Cursor to Python List..")
        result = list(cur)
        convert_time = int(time.time())
        logger.info(f"Convert completed, total items {len(result)}, takes " + str(convert_time - read_db_time) + "s")

        end_time = int(time.time())
        logger.info("Total time processed " + str(end_time - start_time) + "s")









