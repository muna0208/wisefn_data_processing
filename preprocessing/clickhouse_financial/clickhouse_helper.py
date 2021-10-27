import clickhouse_driver
from sqlalchemy import create_engine, Column, MetaData, literal
from clickhouse_sqlalchemy import Table, make_session, get_declarative_base, types, engines
import pandas as pd
import datetime
import csv
import re
import logging
import sys
from logging.handlers import RotatingFileHandler
from preprocessing.clickhouse_financial.shared_columns import full_item_columns, original_item_columns
from utils.config import get_config


logger = logging.getLogger('clickhouse financial access')

class ClickHouseConnection:
    def __init__(self,
                 host='localhost',
                 user='default',
                 password='',
                 database='',
                 settings={'use_numpy': True}
                 ):
        self.database = database
        self.client = None
        try:
            self.client = clickhouse_driver.Client(host=host, user=user, password=password, database=database, settings=settings)
        except Exception as e:
            logger.error(f'{e}')

    def get_columns(self, table_name):
        query = f'SELECT * FROM {table_name} LIMIT 0'
        _, cols = self.client.execute(query, with_column_types=True)
        return [x[0] for x in cols]

    def add_column(self, table_name, col_name, col_type):
        query = f'ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {col_name} {col_type}'
        try:
            self.client.execute(query)
        except Exception as e:
            logger.error(f'{e}')

    def get_schema(self, table_name):
        return self.client.execute('DESC f{table_name}')
        
    def get_client(self):
        return self.client


class ClickHouseSchema:
    def __init__(self,
                 host='localhost',
                 user='default',
                 password='',
                 database='financial',
                 settings={'use_numpy': True}
                 ):
        self.database = database
        self.client = clickhouse_driver.Client(host=host, user=user, password=password, database=database, settings=settings)
        # _ts field is added by 'alter table add column _ts DateTime default toDateTime()', 
        # if 'defalut now()' is used, for the data inserted before the column _ts is added, 
        # _ts value will be the current time when we select the data, which is undesirable.
        # However, when you create a new table, you can use now() as default.
        self.default_datetime = "toDateTime('1970-01-01 00:00:00')"  # "now()"

    def _create_database(self, database):
        query = '''
            CREATE DATABASE IF NOT EXISTS {}
        '''.format(database)
        logger.info(f'{query}')
        self.client.execute(query)
    
    def _create_quarterly_tables(self, drop_table=False):
        ### quarterly_cmpcns
        table_name = 'quarterly_cmpcns'
        if drop_table:
            query = f"DROP TABLE IF EXISTS {table_name}"
            self.client.execute(query)
        signature = (f'_ts DateTime DEFAULT {self.default_datetime}, '
                'TERM_TYP String, CMP_CD String, CNS_DT String, YYMM String, '
                'MKT_TYP String, GICS_CD String, '
                'WI26_CD String, NO_TYP String, CONS_YN String, '
                'Net_Sales Float64, Operating_Profit Float64, '
                'Pre_tax_Profit_from_Continuing_Operations Float64, Net_Profit Float64, '
                'Net_Profit_Owners Float64, ROE_Owners Float64, EPS_Owners Float64, '
                'BPS_Owners Float64, EVEBITDA Float64, PE Float64, PB Float64, '
                'DPS_Adj_Comm_Cash Float64, DPS_Adj_Comm_Cash_FY_End Float64')
        query = f'''
            CREATE TABLE {table_name}
            (
                {signature}
            )
            ENGINE = ReplacingMergeTree()
            ORDER BY (CMP_CD,CNS_DT,YYMM,TERM_TYP)
            SETTINGS index_granularity = 8192;
        '''
        logger.info(f'{query}')
        try:
            self.client.execute(query)
        except Exception as e:
            logger.error(f'{e}')

        ### quarterly_cmpfin
        table_name = 'quarterly_cmpfin'
        if drop_table:
            query = f"DROP TABLE IF EXISTS {table_name}"
            self.client.execute(query)
        signature = (f'_ts DateTime DEFAULT {self.default_datetime}, '
            "YYMM String, CMP_CD String, MKT_TYP String, "
            "GICS_CD String, WI26_CD String, CAL_YEAR String, CAL_QTR String, "
            "CAL_USE_YN String, FS_YEAR String, FS_QTR String, FS_USE_YN String, "
            "IFRS_CHK String, MASTER_CHK String, QTR_MASTER String, "
            "TERM_TYP String, START_DT String, END_DT String, DATA_EXIST_YN String, "
            "FST_PUB_DT String, PF_DT String, LAA_YN String, LAQ_YN String, "
            "SEQ String, ") + original_item_columns
        query = f'''
            CREATE TABLE {table_name}
            (
                {signature}
            )
            ENGINE = ReplacingMergeTree()
            ORDER BY (CMP_CD,YYMM,TERM_TYP)
            SETTINGS index_granularity = 8192;
        '''
        logger.info(f'{query}')
        try:
            self.client.execute(query)
        except Exception as e:
            logger.error(f'{e}')

        ### quarterly_cmpfin_all
        table_name = 'quarterly_cmpfin_all'
        if drop_table:
            query = f"DROP TABLE IF EXISTS {table_name}"
            self.client.execute(query)
        signature = (f'_ts DateTime DEFAULT {self.default_datetime}, '
            "YYMM String, CMP_CD String, MKT_TYP String, "
            "GICS_CD String, WI26_CD String, CAL_YEAR String, CAL_QTR String, "
            "CAL_USE_YN String, FS_YEAR String, FS_QTR String, FS_USE_YN String, "
            "IFRS_CHK String, MASTER_CHK String, QTR_MASTER String, "
            "TERM_TYP String, START_DT String, END_DT String, DATA_EXIST_YN String, "
            "FST_PUB_DT String, PF_DT String, LAA_YN String, LAQ_YN String, "
            "SEQ String, ") + full_item_columns
        query = f'''
            CREATE TABLE {table_name}
            (
                {signature}
            )
            ENGINE = ReplacingMergeTree()
            ORDER BY (CMP_CD,YYMM,TERM_TYP)
            SETTINGS index_granularity = 8192;
        '''
        logger.info(f'{query}')
        try:
            self.client.execute(query)
        except Exception as e:
            logger.error(f'{e}')

        ### quarterly_seccns
        table_name = 'quarterly_seccns'
        if drop_table:
            query = f"DROP TABLE IF EXISTS {table_name}"
            self.client.execute(query)
        signature = (f'_ts DateTime DEFAULT {self.default_datetime}, '
                'TERM_TYP String, SEC_CD String, CNS_DT String, YYMM String, '
                'MKT_TYP String, Net_Sales Float64, Operating_Profit Float64, '
                'Pre_tax_Profit_from_Continuing_Operations Float64, Net_Profit Float64, '
                'Net_Profit_Owners Float64, ROE_Owners Float64, EPS_Owners Float64, '
                'BPS_Owners Float64, EVEBITDA Float64, PE Float64, PB Float64, '
                'DPS_Adj_Comm_Cash Float64, DPS_Adj_Comm_Cash_FY_End Float64')
        query = f'''
            CREATE TABLE {table_name}
            (
                {signature}
            )
            ENGINE = ReplacingMergeTree()
            ORDER BY (SEC_CD,CNS_DT,YYMM,TERM_TYP);
        '''
        logger.info(f'{query}')
        try:
            self.client.execute(query)
        except Exception as e:
            logger.error(f'{e}')

      ### quarterly_secfin
        table_name = 'quarterly_secfin'
        if drop_table:
            query = f"DROP TABLE IF EXISTS {table_name}"
            self.client.execute(query)
        signature = (f'_ts DateTime DEFAULT {self.default_datetime}, '
            "TERM_TYP String, YYMM String, SEC_CD String, MKT_TYP String, ") + original_item_columns
        query = f'''
            CREATE TABLE {table_name}
            (
                {signature}
            )
            ENGINE = ReplacingMergeTree()
            ORDER BY (SEC_CD,YYMM,TERM_TYP)
            SETTINGS index_granularity = 8192;
        '''
        logger.info(f'{query}')
        try:
            self.client.execute(query)
        except Exception as e:
            logger.error(f'{e}')

        ### quarterly_secfin_all
        table_name = 'quarterly_secfin_all'
        if drop_table:
            query = f"DROP TABLE IF EXISTS {table_name}"
            self.client.execute(query)
        signature = (f'_ts DateTime DEFAULT {self.default_datetime}, '
            "TERM_TYP String, YYMM String, SEC_CD String, MKT_TYP String, ") + full_item_columns   
        query = f'''
            CREATE TABLE {table_name}
            (
                {signature}
            )
            ENGINE = ReplacingMergeTree()
            ORDER BY (SEC_CD,YYMM,TERM_TYP)
            SETTINGS index_granularity = 8192;
        '''
        logger.info(f'{query}')
        try:
            self.client.execute(query)
        except Exception as e:
            logger.error(f'{e}')

    def _create_annual_tables(self, drop_table=False):
        table_name = 'annual_cmpcns'
        if drop_table:
            query = f"DROP TABLE IF EXISTS {table_name}"
            self.client.execute(query)
        signature = (f'_ts DateTime DEFAULT {self.default_datetime}, '
                'TERM_TYP String, CMP_CD String, CNS_DT String, YEAR String, '
                'MKT_TYP String, GICS_CD String, '
                'WI26_CD String, NO_TYP String, CONS_YN String, '
                'Net_Sales Float64, Operating_Profit Float64, '
                'Pre_tax_Profit_from_Continuing_Operations Float64, Net_Profit Float64, '
                'Net_Profit_Owners Float64, ROE_Owners Float64, EPS_Owners Float64, '
                'BPS_Owners Float64, EVEBITDA Float64, PE Float64, PB Float64, '
                'DPS_Adj_Comm_Cash Float64, DPS_Adj_Comm_Cash_FY_End Float64')
        query = f'''
            CREATE TABLE {table_name}
            (
                {signature}
            )
            ENGINE = ReplacingMergeTree()
            ORDER BY (CMP_CD,CNS_DT,YEAR,TERM_TYP)
            SETTINGS index_granularity = 8192;
        '''
        logger.info(f'{query}')
        try:
            self.client.execute(query)
        except Exception as e:
            logger.error(f'{e}')

        ### annual_cmpfin
        table_name = 'annual_cmpfin'
        if drop_table:
            query = f"DROP TABLE IF EXISTS {table_name}"
            self.client.execute(query)
        signature = (f'_ts DateTime DEFAULT {self.default_datetime}, '
            "YEAR String, CMP_CD String, MKT_TYP String, "
            "GICS_CD String, WI26_CD String, CAL_YEAR String, CAL_QTR String, "
            "CAL_USE_YN String, FS_YEAR String, FS_QTR String, FS_USE_YN String, "
            "IFRS_CHK String, MASTER_CHK String, QTR_MASTER String, "
            "TERM_TYP String, START_DT String, END_DT String, DATA_EXIST_YN String, "
            "FST_PUB_DT String, PF_DT String, LAA_YN String, LAQ_YN String, "
            "SEQ String, ") + original_item_columns
        query = f'''
            CREATE TABLE {table_name}
            (
                {signature}
            )
            ENGINE = ReplacingMergeTree()
            ORDER BY (CMP_CD,YEAR,TERM_TYP)
            SETTINGS index_granularity = 8192;
        '''
        logger.info(f'{query}')
        try:
            self.client.execute(query)
        except Exception as e:
            logger.error(f'{e}')

        ### annual_cmpfin_all
        table_name = 'annual_cmpfin_all'
        if drop_table:
            query = f"DROP TABLE IF EXISTS {table_name}"
            self.client.execute(query)
        signature = (f'_ts DateTime DEFAULT {self.default_datetime}, '
            "YEAR String, CMP_CD String, MKT_TYP String, "
            "GICS_CD String, WI26_CD String, CAL_YEAR String, CAL_QTR String, "
            "CAL_USE_YN String, FS_YEAR String, FS_QTR String, FS_USE_YN String, "
            "IFRS_CHK String, MASTER_CHK String, QTR_MASTER String, "
            "TERM_TYP String, START_DT String, END_DT String, DATA_EXIST_YN String, "
            "FST_PUB_DT String, PF_DT String, LAA_YN String, LAQ_YN String, "
            "SEQ String, ") + full_item_columns
        query = f'''
            CREATE TABLE {table_name}
            (
                {signature}
            )
            ENGINE = ReplacingMergeTree()
            ORDER BY (CMP_CD,YEAR,TERM_TYP)
            SETTINGS index_granularity = 8192;
        '''
        logger.info(f'{query}')
        try:
            self.client.execute(query)
        except Exception as e:
            logger.error(f'{e}')

         ### annual_seccns
        table_name = 'annual_seccns'
        if drop_table:
            query = f"DROP TABLE IF EXISTS {table_name}"
            self.client.execute(query)
        signature = (f'_ts DateTime DEFAULT {self.default_datetime}, '
                'TERM_TYP String, SEC_CD String, CNS_DT String, YEAR String, '
                'MKT_TYP String, Net_Sales Float64, Operating_Profit Float64, '
                'Pre_tax_Profit_from_Continuing_Operations Float64, Net_Profit Float64, '
                'Net_Profit_Owners Float64, ROE_Owners Float64, EPS_Owners Float64, '
                'BPS_Owners Float64, EVEBITDA Float64, PE Float64, PB Float64, '
                'DPS_Adj_Comm_Cash Float64, DPS_Adj_Comm_Cash_FY_End Float64')
        query = f'''
            CREATE TABLE {table_name}
            (
                {signature}
            )
            ENGINE = ReplacingMergeTree()
            ORDER BY (SEC_CD,CNS_DT,YEAR,TERM_TYP);
        '''
        logger.info(f'{query}')
        try:
            self.client.execute(query)
        except Exception as e:
            logger.error(f'{e}')

        ### annual_secfin
        table_name = 'annual_secfin'
        if drop_table:
            query = f"DROP TABLE IF EXISTS {table_name}"
            self.client.execute(query)
        signature = (f'_ts DateTime DEFAULT {self.default_datetime}, '
            "TERM_TYP String, SEC_CD String, YEAR String,  MKT_TYP String, ") + original_item_columns
        query = f'''
            CREATE TABLE {table_name}
            (
                {signature}
            )
            ENGINE = ReplacingMergeTree()
            ORDER BY (SEC_CD,YEAR,TERM_TYP)
            SETTINGS index_granularity = 8192;
        '''
        logger.info(f'{query}')
        try:
            self.client.execute(query)
        except Exception as e:
            logger.error(f'{e}')

        ### annual_secfin_all
        table_name = 'annual_secfin_all'
        if drop_table:
            query = f"DROP TABLE IF EXISTS {table_name}"
            self.client.execute(query)
        signature = (f'_ts DateTime DEFAULT {self.default_datetime}, '
            "TERM_TYP String, SEC_CD String, YEAR String,  MKT_TYP String, ") + full_item_columns
        query = f'''
            CREATE TABLE {table_name}
            (
                {signature}
            )
            ENGINE = ReplacingMergeTree()
            ORDER BY (SEC_CD,YEAR,TERM_TYP)
            SETTINGS index_granularity = 8192;
        '''
        logger.info(f'{query}')
        try:
            self.client.execute(query)
        except Exception as e:
            logger.error(f'{e}')

    def _create_tables(self, drop_table=False):
        table_name = 'cmpcns_bycmp'
        if drop_table:
            query = f"DROP TABLE IF EXISTS {table_name}"
            self.client.execute(query)
        signature = (f'_ts DateTime DEFAULT {self.default_datetime}, '
                'TERM_TYP String, CMP_CD String, CNS_DT String, YYMM String, '
                'MKT_TYP String, GICS_CD String, '
                'WI26_CD String, NO_TYP String, CONS_YN String, '
                'Net_Sales Float64, Operating_Profit Float64, '
                'Pre_tax_Profit_from_Continuing_Operations Float64, Net_Profit Float64, '
                'Net_Profit_Owners Float64, ROE_Owners Float64, EPS_Owners Float64, '
                'BPS_Owners Float64, EVEBITDA Float64, PE Float64, PB Float64, '
                'DPS_Adj_Comm_Cash Float64, DPS_Adj_Comm_Cash_FY_End Float64')
        query = f'''
            CREATE TABLE {table_name}
            (
                {signature}
            )
            ENGINE = ReplacingMergeTree()
            ORDER BY (CMP_CD,CNS_DT,YYMM,TERM_TYP)
            SETTINGS index_granularity = 8192;
        '''
        logger.info(f'{query}')
        try:
            self.client.execute(query)
        except Exception as e:
            logger.error(f'{e}')

        ### cmpcns_byitem
        table_name = 'cmpcns_byitem'
        if drop_table:
            query = f"DROP TABLE IF EXISTS {table_name}"
            self.client.execute(query)
        signature = (f'_ts DateTime DEFAULT {self.default_datetime}, '
            'TERM_TYP String, ITEM_NM String, CNS_DT String, YYMM String')
        query = f'''
            CREATE TABLE {table_name}
            (
                {signature}
            )
            ENGINE = ReplacingMergeTree()
            ORDER BY (ITEM_NM,CNS_DT,YYMM,TERM_TYP)
            SETTINGS index_granularity = 8192;
        '''
        logger.info(f'{query}')
        try:
            self.client.execute(query)
        except Exception as e:
            logger.error(f'{e}')

        ### cmpfin_bycmp
        table_name = 'cmpfin_bycmp'
        if drop_table:
            query = f"DROP TABLE IF EXISTS {table_name}"
            self.client.execute(query)
        signature = (f'_ts DateTime DEFAULT {self.default_datetime}, '
            "YYMM String, CMP_CD String, MKT_TYP String, "
            "GICS_CD String, WI26_CD String, CAL_YEAR String, CAL_QTR String, "
            "CAL_USE_YN String, FS_YEAR String, FS_QTR String, FS_USE_YN String, "
            "IFRS_CHK String, MASTER_CHK String, QTR_MASTER String, "
            "TERM_TYP String, START_DT String, END_DT String, DATA_EXIST_YN String, "
            "FST_PUB_DT String, PF_DT String, LAA_YN String, LAQ_YN String, "
            "SEQ String, ") + original_item_columns
        query = f'''
            CREATE TABLE {table_name}
            (
                {signature}
            )
            ENGINE = ReplacingMergeTree()
            ORDER BY (CMP_CD,YYMM,TERM_TYP)
            SETTINGS index_granularity = 8192;
        '''
        logger.info(f'{query}')
        try:
            self.client.execute(query)
        except Exception as e:
            logger.error(f'{e}')


        ### cmpfin_bycmp_all
        table_name = 'cmpfin_bycmp_all'
        if drop_table:
            query = f"DROP TABLE IF EXISTS {table_name}"
            self.client.execute(query)
        signature = (f'_ts DateTime DEFAULT {self.default_datetime}, '
            "YYMM String, CMP_CD String, MKT_TYP String, "
            "GICS_CD String, WI26_CD String, CAL_YEAR String, CAL_QTR String, "
            "CAL_USE_YN String, FS_YEAR String, FS_QTR String, FS_USE_YN String, "
            "IFRS_CHK String, MASTER_CHK String, QTR_MASTER String, "
            "TERM_TYP String, START_DT String, END_DT String, DATA_EXIST_YN String, "
            "FST_PUB_DT String, PF_DT String, LAA_YN String, LAQ_YN String, "
            "SEQ String, ") + full_item_columns
        query = f'''
            CREATE TABLE {table_name}
            (
                {signature}
            )
            ENGINE = ReplacingMergeTree()
            ORDER BY (CMP_CD,YYMM,TERM_TYP)
            SETTINGS index_granularity = 8192;
        '''
        logger.info(f'{query}')
        try:
            self.client.execute(query)
        except Exception as e:
            logger.error(f'{e}')


        ### cmpfin_byitem
        table_name = 'cmpfin_byitem'
        if drop_table:
            query = f"DROP TABLE IF EXISTS {table_name}"
            self.client.execute(query)
        signature = (f'_ts DateTime DEFAULT {self.default_datetime}, '
            'TERM_TYP String, ITEM_NM String, YYMM String')
        query = f'''
            CREATE TABLE {table_name}
            (
                {signature}
            )
            ENGINE = ReplacingMergeTree()
            ORDER BY (ITEM_NM,YYMM,TERM_TYP)
            SETTINGS index_granularity = 8192;
        '''
        logger.info(f'{query}')
        try:
            self.client.execute(query)
        except Exception as e:
            logger.error(f'{e}')

        ### seccns_byitem
        table_name = 'seccns_byitem'
        if drop_table:
            query = f"DROP TABLE IF EXISTS {table_name}"
            self.client.execute(query)
        signature = (f'_ts DateTime DEFAULT {self.default_datetime}, '
            'TERM_TYP String, ITEM_NM String, CNS_DT String, YYMM String')
        query = f'''
            CREATE TABLE {table_name}
            (
                {signature}
            )
            ENGINE = ReplacingMergeTree()
            ORDER BY (ITEM_NM,CNS_DT,YYMM,TERM_TYP)
            SETTINGS index_granularity = 8192;
        '''
        logger.info(f'{query}')
        try:
            self.client.execute(query)
        except Exception as e:
            logger.error(f'{e}')

        ### seccns_bysec
        table_name = 'seccns_bysec'
        if drop_table:
            query = f"DROP TABLE IF EXISTS {table_name}"
            self.client.execute(query)
        signature = (f'_ts DateTime DEFAULT {self.default_datetime}, '
                'TERM_TYP String, SEC_CD String, CNS_DT String, YYMM String, '
                'MKT_TYP String, Net_Sales Float64, Operating_Profit Float64, '
                'Pre_tax_Profit_from_Continuing_Operations Float64, Net_Profit Float64, '
                'Net_Profit_Owners Float64, ROE_Owners Float64, EPS_Owners Float64, '
                'BPS_Owners Float64, EVEBITDA Float64, PE Float64, PB Float64, '
                'DPS_Adj_Comm_Cash Float64, DPS_Adj_Comm_Cash_FY_End Float64')
        query = f'''
            CREATE TABLE {table_name}
            (
                {signature}
            )
            ENGINE = ReplacingMergeTree()
            ORDER BY (SEC_CD,CNS_DT,YYMM,TERM_TYP)
            SETTINGS index_granularity = 8192;
        '''
        logger.info(f'{query}')
        try:
            self.client.execute(query)
        except Exception as e:
            logger.error(f'{e}')

        ### secfin_byitem
        table_name = 'secfin_byitem'
        if drop_table:
            query = f"DROP TABLE IF EXISTS {table_name}"
            self.client.execute(query)
        signature = (f'_ts DateTime DEFAULT {self.default_datetime}, '
            'TERM_TYP String, ITEM_NM String, YYMM String')
        query = f'''
            CREATE TABLE {table_name}
            (
                {signature}
            )
            ENGINE = ReplacingMergeTree()
            ORDER BY (ITEM_NM,YYMM,TERM_TYP)
            SETTINGS index_granularity = 8192;
        '''
        logger.info(f'{query}')
        try:
            self.client.execute(query)
        except Exception as e:
            logger.error(f'{e}')

        ### secfin_bysec
        table_name = 'secfin_bysec'
        if drop_table:
            query = f"DROP TABLE IF EXISTS {table_name}"
            self.client.execute(query)
        signature = (f'_ts DateTime DEFAULT {self.default_datetime}, '
            "TERM_TYP String, YYMM String, SEC_CD String, MKT_TYP String, ") + original_item_columns
        query = f'''
            CREATE TABLE {table_name}
            (
                {signature}
            )
            ENGINE = ReplacingMergeTree()
            ORDER BY (SEC_CD,YYMM,TERM_TYP)
            SETTINGS index_granularity = 8192;
        '''
        logger.info(f'{query}')
        try:
            self.client.execute(query)
        except Exception as e:
            logger.error(f'{e}')


        ### secfin_bysec_all
        table_name = 'secfin_bysec_all'
        if drop_table:
            query = f"DROP TABLE IF EXISTS {table_name}"
            self.client.execute(query)
        signature = (f'_ts DateTime DEFAULT {self.default_datetime}, '
            "TERM_TYP String, YYMM String, SEC_CD String, MKT_TYP String, ") + full_item_columns
        query = f'''
            CREATE TABLE {table_name}
            (
                {signature}
            )
            ENGINE = ReplacingMergeTree()
            ORDER BY (SEC_CD,YYMM,TERM_TYP)
            SETTINGS index_granularity = 8192;
        '''
        logger.info(f'{query}')
        try:
            self.client.execute(query)
        except Exception as e:
            logger.error(f'{e}')

    def get_columns(self, table_name):
        query = 'SELECT * FROM {} LIMIT 0'.format(table_name)
        _, cols = self.client.execute(query, with_column_types=True)
        return [x[0] for x in cols]

    def get_schema(self, table_name):
        query = 'DESC {} '.format(table_name)
        result = self.client.execute(query)
        return result

if __name__ == '__main__':
    config_file = '/home/mining/projects/wisefn_data_processing/db2513.config'

    if len(sys.argv) > 1:
        config_file = sys.argv[1]

    conf = get_config(config_file)
    logging.basicConfig(level=eval(conf['Logging']['level']),
        handlers=[eval(conf['Logging']['handler'])],
        format=conf['Logging']['format'],
        )

    schema = ClickHouseSchema(host=conf['ClickHouse']['host'],
                 user=conf['ClickHouse']['user'],
                 password=conf['ClickHouse']['password'],
                 database='',)
    schema._create_database('financial')
    schema = ClickHouseSchema(host=conf['ClickHouse']['host'],
                user=conf['ClickHouse']['user'],
                password=conf['ClickHouse']['password'],
                database='financial',)
    schema._create_tables()
    schema._create_annual_tables()
    schema._create_quarterly_tables()
    result = schema.get_columns('secfin_bysec_all')
