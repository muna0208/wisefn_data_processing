import clickhouse_driver
from sqlalchemy import create_engine, Column, MetaData, literal
from clickhouse_sqlalchemy import Table, make_session, get_declarative_base, types, engines
import pandas as pd
import datetime
import csv
import re
import logging
from wisefn_clickhouse.config import get_config


logger = logging.getLogger('clickhouse wisefnschema')

class ClickHouseSchema:
    def __init__(self,
                 host='localhost',
                 user='default',
                 password='',
                 database='',
                 settings={'use_numpy': True}
                 ):
        self.database = database
        self.client = clickhouse_driver.Client(host=host, database=database, settings=settings)

    def _create_database(self, database):
        query = '''
            CREATE DATABASE IF NOT EXISTS {}
        '''.format(database)
        logger.info(f'{query}')
        self.client.execute(query)
    
    def _create_user(self, user, password):
        # modify /etc/clickhouse.server/users.xml to enable default user privilege
        query = f"CREATE USER IF NOT EXISTS {user} HOST ANY IDENTIFIED WITH SHA256_PASSWORD  BY '{password}'"
        logger.info(f'{query}')
        self.client.execute(query)
        query = f"GRANT *.* TO {user}"
        logger.info(f'{query}')
        self.client.execute(query)

    def _create_table(self, data):
        query = f"DROP TABLE IF EXISTS {data.iloc[0, :].loc['파일명']}"
        self.client.execute(query)

        signature = ''
        signature += 'DNDATE String,'
        for _, d in data[['컬럼명', 'DataType', 'Null',]].iterrows():
            signature += d['컬럼명'] + ' ' + ('Float64' if 'decimal' in d['DataType'] else 'String')
            signature += ','
        signature = signature[:-1]   
        query = f'''
            CREATE TABLE {data.iloc[0, :].loc['파일명']}
            (
                {signature}
            )
            ENGINE = ReplacingMergeTree()
            ORDER BY ({','.join(['DNDATE'] + list(data[data['Key'] == 'PK']['컬럼명']))})
            SETTINGS index_granularity = 8192;
        '''
        logger.info(f'{query}')
        self.client.execute(query)

    def _create_table_from_data_frame(self, df):
        index_list = df[~df['파일명'].isna()].index
        index_list = index_list.append(df.index[-1:])
        intervals = list(zip(index_list[:-1], [df.index[i-1] if i != index_list[-1] else df.index[i] for i in index_list[1:]]))
        for intv in intervals:
            logger.info(f'intervals {intv}')
            try:
                self._create_table(df.loc[intv[0]:intv[1], :])
            except Exception as e:
                logger.error(f'{e}')

    def get_columns(self, table_name):
        query = 'SELECT * FROM {} LIMIT 0'.format(table_name)
        _, cols = self.client.execute(query, with_column_types=True)
        return [x[0] for x in cols]

    def get_schema(self, table_name):
        return self.client.execute('DESC {}'.format(table_name))


if __name__ == '__main__':
    import sys
    excel_file='data/DBSpec_DaumSoft_20210407.xlsx'
    logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(filename)s %(lineno)d %(levelname)s - %(message)s')

    cover = pd.read_excel(excel_file, sheet_name=0, skiprows=3)
    financial = pd.read_excel(excel_file, sheet_name=1, skiprows=2, index_col=None)
    concensus = pd.read_excel(excel_file, sheet_name=2, skiprows=2)
    items = pd.read_excel(excel_file, sheet_name=3, skiprows=2)
    code_info = pd.read_excel(excel_file, sheet_name=4, skiprows=1, dtype='str')

    config_file = '/home/mining/projects/wisefn_data_processing/db2513.config'
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    conf = get_config(config_file)

    schema = ClickHouseSchema(        
        host=conf['ClickHouse']['host'], 
        user=conf['ClickHouse']['user'], 
        password=conf['ClickHouse']['password'])
    if len(sys.argv) > 2 and sys.argv[2] == 'create_db':
        schema._create_database('wisefn')
        schema._create_user('mining', 'mining@2017')

    schema = ClickHouseSchema(
        host=conf['ClickHouse']['host'], 
        database=conf['ClickHouse']['database'], 
        user=conf['ClickHouse']['user'], 
        password=conf['ClickHouse']['password'])
    schema._create_table_from_data_frame(financial)
    schema._create_table_from_data_frame(concensus)
    schema._create_table_from_data_frame(items)
