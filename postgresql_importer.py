import psycopg2
from psycopg2.extras import execute_values
from mongo_exporter import TransferMongodbStreamingExporter
from postgre_exporter import TransferPostgresqlStreamingExporter

import logging
import time

logger = logging.getLogger("Connect Postgresql Database")


class Postgresql:
    def __init__(
            self
    ):
        self.conn = psycopg2.connect(
            database='postgres',
            user='trava_writer',
            password='trava_writer123',
            host='178.128.93.195',
            port='5434'
        )
        self.curs = self.conn.cursor()

    def test(self):
        logger.info("Start calculating with Postgresql DB")
        start_time = int(time.time())

        logger.info("Querying to find top10 value...")
        self.curs.execute("""select tr.contract_address, tr.from_address, sum(tr.value)
                            from transfer_event_1week as tr
                            group by tr.from_address, tr.contract_address
                            order by sum desc""")
        top_10_count = self.curs.fetchall()[:10]
        find_top_value_time = int(time.time())
        logger.info(f"Finding top10 value completed, takes {str(find_top_value_time - start_time)}s")

        logger.info("Querying to find top10 number of transactions...")
        self.curs.execute("""select tr.contract_address, tr.from_address, count(tr.value)
                            from transfer_event_1week as tr
                            group by tr.from_address, tr.contract_address
                            order by count desc""")
        top_10_value = self.curs.fetchall()[:10]
        find_top_number_time = int(time.time())
        logger.info(f"Finding top10 value completed, takes {str(find_top_number_time - find_top_value_time)}s")

        end_time = int(time.time())
        logger.info("Finished. Total time " + str(end_time - start_time) + "s")

        logger.info("Top 10 according to number of transactions:")
        logger.info(top_10_count)

        logger.info("Top 10 according to value of transactions:")
        logger.info(top_10_value)

    def get_cur(self, query, table, from_block, to_block, token_address, from_value, to_value):
        global sql
        if query == "by_block":
            sql = f"""
                    select *
                    from {table} as tr
                    where tr.block_number>={from_block} and tr.block_number<={to_block}"""
        elif query == "by_token_address":
            sql = f"""
                    select *
                    from {table} as tr
                    where tr.contract_address='{token_address}'"""
        elif query == "by_value":
            sql = f"""
                    select *
                    from {table} as tr
                    where tr.value>={from_value} and tr.value<={to_value}"""
        elif query == "by_block_and_token_address":
            sql = f"""
                    select *
                    from {table} as tr
                    where tr.block_number>={from_block} and tr.block_number<={to_block} 
                    and tr.contract_address='{token_address}'"""
        elif query == "by_block_and_value":
            sql = f"""
                    select *
                    from {table} as tr
                    where tr.block_number>={from_block} and tr.block_number<={to_block} 
                    and tr.value>={from_value} and tr.value<={to_value}"""
        elif query == "by_token_address_and_value":
            sql = f"""
                    select *
                    from {table} as tr
                    where tr.contract_address='{token_address}'
                    and tr.value>={from_value} and tr.value<={to_value}"""
        elif query == "by_all":
            sql = f"""
                    select *
                    from {table} as tr
                    where tr.contract_address='{token_address}'
                    and tr.value>={from_value} and tr.value<={to_value}
                    and tr.block_number>={from_block} and tr.block_number<={to_block}"""
        return sql

    def test_query(self, query):
        table = "transfer_event_1week"
        from_block = 24085024
        to_block = 24086024
        from_value = 1
        to_value = 5
        token_address = '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c'
        start_time = int(time.time())
        logger.info(f"Start processing, token address: {token_address}, total blocks 1001, value from {from_value} to {to_value}")

        logger.info("Query to Postgresql..")
        sql = self.get_cur(query,
                           table=table,
                           from_block=from_block,
                           to_block=to_block,
                           token_address=token_address,
                           from_value=from_value,
                           to_value=to_value)
        self.curs.execute(sql)
        read_db_time = int(time.time())
        logger.info(f"Query completed, takes " + str(read_db_time - start_time) + "s")

        logger.info("Converting to Python List..")
        result = self.curs.fetchall()
        convert_time = int(time.time())
        logger.info(f"Convert completed, total items {len(result)}, takes " + str(convert_time - read_db_time) + "s")

        end_time = int(time.time())
        logger.info("Total time processed " + str(end_time - start_time) + "s")

    def test_importing_time(self, table):
        self.curs.execute(f"Select * FROM {table} LIMIT 0")
        colnames = [desc[0] for desc in self.curs.description]
        logger.info(colnames)

        sql = f"select * from {table}"
        logger.info(f"Start processing")

        logger.info("Reading to Postgresql to Python List..")
        self.curs.execute(sql)
        list_ = self.curs.fetchall()
        logger.info(f"Read completed")

        list_rs = []

        for event in list_:
            rs = {}
            for i in range(len(colnames)):
                rs[colnames[i]] = event[i]
            list_rs.append(rs)

        postgre_exporter = TransferPostgresqlStreamingExporter(curs=self.curs, conn=self.conn)
        postgre_exporter.export_items(list_rs)

        # for event in list_rs:
        #     event['_id'] = f"{event['block_number']}_{event['log_index']}"
        #
        # mongo_exporter = TransferMongodbStreamingExporter()
        # mongo_exporter.export_items(list_rs)




