from wisefn_mariadb.db_connection import MariaDBConnection
from wisefn_mariadb.wisefn2mariadb_schema import WiseFNSchema
import pandas as pd
import logging
import os
import gc
from wisefn_mariadb.config import get_config
import sys
from logging.handlers import RotatingFileHandler


logger = logging.getLogger('dbreader')


class MariaDBReader:
    def __init__(self, connection_uri=None):
        self.connection = MariaDBConnection(connection_uri=connection_uri)
        self.engine = self.connection.get_alchemy_engine()
        self.schema = WiseFNSchema

    def read_financial(self, table_name, cond='', **argv):
        df = None
        try:
            df = pd.read_sql(f'SELECT * FROM {table_name} {cond}', con=self.engine, **argv)
        except Exception as e:
            logger.error(f'{e}')
        return df
    
if __name__ == '__main__':
    config_file = '/home/mining/PycharmProjects/wisefn_data_processing/db3.config'
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    conf = get_config(config_file)

    db = MariaDBReader(conf['MariaDB']['connection_uri'] + '/' + conf['MariaDB']['db_name'])
    logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(filename)s %(lineno)d %(levelname)s - %(message)s')
    findata = db.read_financial(table_name='TF_CMP_FINDATA', cond="WHERE CMP_CD = '000020'")
    item = db.read_financial(table_name='TZ_ITEM', cond="")

