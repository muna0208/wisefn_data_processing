import csv
import datetime
import logging
import pandas as pd
import numpy as np
import os
import sys

from preprocessing.config import config
from preprocessing.defines import TermAcctMethod, TermAcctDir, FinAcctType

from wisefn_mariadb.db_connection import MariaDBConnection
from wisefn_mariadb.wisefn2mariadb_schema import WiseFNSchema
from wisefn_mariadb.db_reader import MariaDBReader
from wisefn_mariadb.config import get_config
from logging.handlers import RotatingFileHandler
from preprocessing.clickhouse_financial.clickhouse_helper import ClickHouseConnection

logger = logging.getLogger('annual_secfin')


def write_clickhouse(con, table_name, term, data):
    cols = con.get_columns(table_name)
    data.loc[:,'TERM_TYP'] = term
    data = data.rename_axis('YEAR').reset_index()
    str_cols = data.select_dtypes(exclude='float').columns
    data = data.reindex(cols, axis='columns')
    data['_ts'] = datetime.datetime.now()
    data.loc[:, str_cols] = data.loc[:, str_cols].fillna('')

    try:
        n = con.get_client().insert_dataframe(f'INSERT INTO {table_name} VALUES', data)
        if n == 0:
            logger.error(f'0 rows written: {table_name}')        
    except Exception as e:
        logger.error(f'{e}')
    logger.debug(f'{data}')

def write_csv(path, sec_cd, data):
    s = data.to_csv(None, quoting=csv.QUOTE_NONNUMERIC, na_rep='NA').replace('"NA"', 'NA')
    target_path = path + '/' + '99YEARLY'
    logger.info(f"path - {target_path}/{sec_cd}.csv")
    logger.debug(f"data - \n{s}")
    if not os.path.exists(target_path):
        os.makedirs(target_path)
    if True:
        with open(target_path + "/" +  sec_cd + ".csv1", 'w') as f:
            f.write(s)

def generate_annual_sec_fin(conf, to_csv=True, to_clickhouse=True):
    con_ = ClickHouseConnection(
    host=conf['ClickHouse']['host'],
    user=conf['ClickHouse']['user'],
    password=conf['ClickHouse']['password'],
    database='financial', )  

    path = conf['Path']['by_sector_sec_fin_path']
    termaccts = [c.value for c in TermAcctDir]

    target_path = path + '/' + termaccts[0]
    if not os.path.exists(target_path):
        logger.error(f'path does not exist: {target_path}')
        return
    sep_secs_csv = os.listdir(target_path)
    secs = sorted([f.replace('.csv', '') for f in sep_secs_csv if '.csv' in f and 'csv1' not in f])
    secs = np.unique(secs)
    # db = MariaDBReader(conf['MariaDB']['connection_uri'] + '/' + conf['MariaDB']['db_name'])
    # sector = db.read_financial(table_name='TC_SECTOR', cond="WHERE SEC_TYP='W'").fillna('').astype('str')
    # secs = sorted(sector.loc[:,'SEC_CD'].unique())

    for sec in secs[:]:
        try:
            if not os.path.exists(target_path + '/' + sec + '.csv1'):
                continue
            sep = pd.read_csv(target_path + '/' + sec + '.csv1', encoding='euc-kr', dtype='str')
            sep.set_index(sep.columns[0], inplace=True)
            sep.index.rename('YYMM', inplace=True)
            sep = sep.loc[[i for i in sep.index if i[-2:] == '12'],:]
            sep.index = [i[:4] for i in sep.index]

            data = sep
            if len(data) == 0:
                logger.warning(f'empty {termaccts[0]}, {sec}')
                continue
            if 'Total_Stockholders._Equity.' in data.columns:
                logger.info('error correction')
                data = data.drop('Total_Stockholders._Equity.', axis='columns')
            
            data[data.columns[2:]] = data[data.columns[2:]].astype('float')
            if to_csv:
                write_csv(path, sec, data)
            if to_clickhouse:
                write_clickhouse(con_, 'annual_secfin', 'YEARLY', data)

        except Exception as e:
            logger.error(f'{e}')

def generate_annual_sec_fin_ch(conf, to_clickhouse=True):
    con_ = ClickHouseConnection(
        host=conf['ClickHouse']['host'],
        user=conf['ClickHouse']['user'],
        password=conf['ClickHouse']['password'],
        database='financial', )  

    termaccts = [c.value for c in TermAcctMethod]

    db = MariaDBReader(conf['MariaDB']['connection_uri'] + '/' + conf['MariaDB']['db_name'])
    sector = db.read_financial(table_name='TC_SECTOR', cond="WHERE SEC_TYP='W'").fillna('').astype('str')
    secs = sorted(sector.loc[:,'SEC_CD'].unique())

    for sec in secs[:]:
        try:
            table_name = 'secfin_bysec'
            sep = con_.get_client().query_dataframe(f"SELECT * FROM {table_name} WHERE SEC_CD = '{sec}' AND TERM_TYP='{termaccts[0]}'")
            if len(sep) == 0:
                continue
            sep = sep.sort_values(['YYMM', '_ts']).drop_duplicates(subset=['YYMM'], keep='last').set_index('YYMM')
            sep = sep.loc[[i for i in sep.index if i[-2:] == '12'],:]
            sep.index = [i[:4] for i in sep.index]
            sep.rename_axis('YEAR')

            data = sep
            if len(data) == 0:
                logger.warning(f'empty {termaccts[0]}, {sec}')
                continue

            data[data.columns[4:]] = data[data.columns[4:]].astype('float')            
            if to_clickhouse:
                write_clickhouse(con_, 'annual_secfin', 'YEARLY', data)

        except Exception as e:
            logger.error(f'{e}')


if __name__ == '__main__':
    config_file = '/home/mining/PycharmProjects/wisefn_data_processing/db3.config'
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    conf = get_config(config_file)

    logging.basicConfig(level=eval(conf['Logging']['level']),
        handlers=[eval(conf['Logging']['handler'])],
        format=conf['Logging']['format'],)

    # generate_annual_sec_fin(conf)
    generate_annual_sec_fin_ch(conf)

