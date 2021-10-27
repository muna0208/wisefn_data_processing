import csv
import datetime
import logging
import pandas as pd
import numpy as np
import os
import sys
import re

from preprocessing.config import config
from preprocessing.defines import TermAcctMethod, TermAcctDir, FinAcctType

from wisefn_mariadb.db_connection import MariaDBConnection
from wisefn_mariadb.wisefn2mariadb_schema import WiseFNSchema
from wisefn_mariadb.db_reader import MariaDBReader
from wisefn_mariadb.config import get_config
from logging.handlers import RotatingFileHandler
from preprocessing.clickhouse_financial.clickhouse_helper import ClickHouseConnection


logger = logging.getLogger('quarterly_seccns')


def write_clickhouse(con, table_name, term, sec_cd, data):
    cols = con.get_columns(table_name)
    data['SEC_CD'] = sec_cd
    data['TERM_TYP'] = term
    data = data.rename_axis('YYMM').reset_index()
    data['_ts'] = datetime.datetime.now()
    data = data.reindex(cols, axis='columns')
    data.YYMM = data.YYMM.astype(str)

    try:
        n = con.get_client().insert_dataframe(f'INSERT INTO {table_name} VALUES', data)
        if n == 0:
            logger.error(f'0 rows written: {table_name}')
    except Exception as e:
        logger.error(f'{e}')
    logger.debug(f'data - {data}')

def write_csv(path, sec_cd, dt, data):
    s = data.to_csv(None, quoting=csv.QUOTE_NONNUMERIC, na_rep='NA').replace('"NA"', 'NA')
    target_path = path + '/' + dt + '/' + '99QUARTER_NET'
    logger.info(f"path - {target_path}/{sec_cd}.csv")
    logger.debug(f"data - \n{s}")
    if not os.path.exists(target_path):
        os.makedirs(target_path)
    if True:
        with open(target_path + '/'  + sec_cd + ".csv1", 'w') as f:
            f.write(s)

def generate_quarterly_sec_cns(conf, start_dt, end_dt, to_csv=True, to_clickhouse=True):
    con_ = ClickHouseConnection(
        host=conf['ClickHouse']['host'],
        user=conf['ClickHouse']['user'],
        password=conf['ClickHouse']['password'],
        database='financial', )   

    path = conf['Path']['by_sector_sec_cns_path']
    termaccts = [c.value for c in TermAcctDir]

    db = MariaDBReader(conf['MariaDB']['connection_uri'] + '/' + conf['MariaDB']['db_name'])
    sector = db.read_financial(table_name='TC_SECTOR', cond="WHERE SEC_TYP='W'").fillna('').astype('str')
    secs = sorted(sector.loc[:,'SEC_CD'].unique())

    if not os.path.exists(path):
        logger.error(f'path does not exists: {path}')
        return
    dates = os.listdir(path)
    dates = [d for d in sorted(dates) if re.match(r'\d{8}', d)]
    
    dates = [d for d in dates if start_dt <= d <= end_dt]
    date_list = dates

    # for each cns_date
    for sec in secs[:]:
        logger.info(f'processing quarterly_seccns: {sec}')
        for dt in date_list[:]:
            # use SEP_NET
            target_path = path + '/' + dt + '/' +  termaccts[1]
            try:
                if not os.path.exists(target_path + '/' + sec + '.csv1'):
                    continue
                logger.debug('input ' +  target_path + '/' + sec + '.csv')
                sep = pd.read_csv(target_path + '/' + sec + '.csv1', dtype='str')
                sep = sep.set_index(sep.columns[0])
                # '.0' is not expected
                sep.index = [idx[:-2] + '.1' if idx[-2:] == '03'  
                        else  idx[:-2] + '.2' if idx[-2:] == '06' 
                        else  idx[:-2] + '.3' if idx[-2:] == '09' 
                        else  idx[:-2] + '.4' if idx[-2:] == '12' 
                        else  idx[:-2] + '.0' for idx in sep.index] 
                data = sep
                if len(data) == 0:
                    logger.warning(f'empty {dt}, {sec}')                        
                    continue

                data[data.columns[2:]] = data[data.columns[2:]].astype('float')
                if to_csv:
                    write_csv(path, sec, dt, data)
                if to_clickhouse:
                    write_clickhouse(con_, 'quarterly_seccns', 'QUARTER_NET', sec, data)

            except Exception as e:
                logger.error(f'{e}')

def generate_quarterly_sec_cns_ch(conf, to_clickhouse=True):
    con_ = ClickHouseConnection(
        host=conf['ClickHouse']['host'],
        user=conf['ClickHouse']['user'],
        password=conf['ClickHouse']['password'],
        database='financial', )    

    termaccts = [c.value for c in TermAcctMethod]

    db = MariaDBReader(conf['MariaDB']['connection_uri'] + '/' + conf['MariaDB']['db_name'])
    sector = db.read_financial(table_name='TC_SECTOR', cond="WHERE SEC_TYP='W'").fillna('').astype('str')
    secs = sorted(sector.loc[:,'SEC_CD'].unique())

    # for each cns_date
    for sec in secs[:]:
        logger.info(f'processing quarterly_seccns: {sec}')
        try:
            table_name = 'seccns_bysec'
            sep = con_.get_client().query_dataframe(f"SELECT * FROM {table_name} WHERE SEC_CD = '{sec}' AND TERM_TYP='{termaccts[1]}' ")
            if len(sep) == 0:
                continue
            sep = sep.set_index('YYMM')
            data = sep
            data = data[(data.TERM_TYP == termaccts[1])]
            data = data.reset_index().sort_values(['YYMM', '_ts']).drop_duplicates(subset=['YYMM'], keep='last').set_index('YYMM')

            # '.0' is not expected
            data.index = [idx[:-2] + '.1' if idx[-2:] == '03'  
                    else  idx[:-2] + '.2' if idx[-2:] == '06' 
                    else  idx[:-2] + '.3' if idx[-2:] == '09' 
                    else  idx[:-2] + '.4' if idx[-2:] == '12' 
                    else  idx[:-2] + '.0' for idx in data.index] 
            if len(data) == 0:
                logger.warning(f'empty {sec}')                        
                continue
            
            data[data.columns[5:]] = data[data.columns[5:]].astype('float')
            if to_clickhouse:
                write_clickhouse(con_, 'quarterly_seccns', 'QUARTER_NET', sec, data)

        except Exception as e:
            logger.error(f'{e}')


if __name__ == '__main__':
    config_file = '/home/mining/systemtrading/python_projects/wisefn_data_processing/db51.config'
    start_dt = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
    end_dt = datetime.datetime.now().strftime('%Y%m%d')

    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    if len(sys.argv) > 2:
        start_dt = sys.argv[2]
    if len(sys.argv) > 3:
        end_dt = sys.argv[3]

    conf = get_config(config_file)
    logging.basicConfig(level=eval(conf['Logging']['level']),
        handlers=[eval(conf['Logging']['handler'])],
        format=conf['Logging']['format'], )

    #FIXME
    start_dt = '20000101'
    end_dt = '20210907'

    # generate_quarterly_sec_cns(conf, start_dt, end_dt)
    generate_quarterly_sec_cns_ch(conf)
    


