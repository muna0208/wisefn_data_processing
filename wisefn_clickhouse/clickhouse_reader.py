from wisefn_clickhouse.clickhouse_connection import ClickHouseConnection
from wisefn_clickhouse.wisefn2clickhouse_schema import ClickHouseSchema
import pandas as pd
import logging
import os
import gc
from wisefn_clickhouse.config import get_config
import sys
from logging.handlers import RotatingFileHandler

logger = logging.getLogger('clickhouse_reader')


class ClickHouseReader:
    def __init__(self, **argv):
        self.connection = ClickHouseConnection(**argv)
        self.client = self.connection.get_client() 
        self.database = None
        if 'database' in argv:
            self.database = argv['database']
        else:
            logger.error(f'database error {self.database}')
        self.schema = ClickHouseSchema(database=self.database)

    def read_financial(self, table_name, cond):
        df = self.client.query_dataframe(f'SELECT * FROM {table_name} {cond}')
        return df
    
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(filename)s %(lineno)d %(levelname)s - %(message)s')
    config_file = '/home/mining/PycharmProjects/wisefn_data_processing/db3.config'
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    conf = get_config(config_file)

    db = ClickHouseReader(database=conf['ClickHouse']['database'])
    # df = db.read_financial(table_name='TC_COMPANY', cond="WHERE DNDATE >= '20210226'")
    df = db.read_financial(table_name='TC_COMPANY', cond="")


