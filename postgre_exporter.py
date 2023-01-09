import psycopg2
from psycopg2.extras import execute_values

import logging
import time

_logger = logging.getLogger(__name__)

sql_table = 'importer_test1k'


class TransferPostgresqlStreamingExporter:
    def __init__(
            self,
            curs,
            conn,
            is_create_table=False
    ):
        self.curs = curs
        self.conn = conn
        self.is_create_table = is_create_table

    def export_items(self, items: list) -> None:
        self.check_table()
        _logger.info(f"*****  Start exporting events to PostGreSql Database *****")
        _logger.info(f"Total items: {len(items)}")
        start_time = int(time.time())
        self.export_events(items)
        end_time = int(time.time())
        logging.info("Total time exporting to PostGreSql Database: " + str(end_time - start_time))

    def check_table(self):
        try:
            self.curs.execute(f"Select * FROM {sql_table} LIMIT 0;")
        except Exception:
            self.conn.commit()
            self.is_create_table = True
            pass

    def export_events(self, operations_data: list) -> None:
        if not operations_data:
            return
        if self.is_create_table is True:
            self.is_create_table = False
            self.create_table_and_columns(operations_data[0])

        # add values to all cols
        columns = operations_data[0].keys()
        columns = ['"' + column + '"' for column in columns]
        # query = "INSERT INTO " + sql_table + "({}) VALUES %s ON CONFLICT (block_number, log_index) DO NOTHING".format(','.join(columns))
        query = "INSERT INTO " + sql_table + "({}) VALUES %s".format(','.join(columns))
        values = [[value for value in operation_data.values()] for operation_data in operations_data]
        execute_values(self.curs, query, values)
        self.conn.commit()

        self.conn.close()

    def create_table_and_columns(self, operation_data: dict):

        # add new table (drop if it already exists)
        self.curs.execute(f"DROP TABLE IF EXISTS {sql_table};")
        self.conn.commit()
        add_table_sql = f"CREATE TABLE {sql_table}();"
        self.curs.execute(add_table_sql)
        self.conn.commit()

        for key in operation_data.keys():
            if key in ['block_number', 'log_index']:
                self.curs.execute(f'ALTER TABLE {sql_table} ADD COLUMN "{key}" bigint;')
            elif key is 'value':
                self.curs.execute(f'ALTER TABLE {sql_table} ADD COLUMN "{key}" double precision;')
            else:
                self.curs.execute(f'ALTER TABLE {sql_table} ADD COLUMN "{key}" text;')
            self.conn.commit()

        # self.curs.execute(f'ALTER TABLE {sql_table} ADD UNIQUE (block_number, log_index)')
        # self.conn.commit()



