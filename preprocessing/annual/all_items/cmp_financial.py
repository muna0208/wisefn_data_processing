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
from preprocessing.clickhouse_financial.shared_columns import column_map

logger = logging.getLogger('annual_cmpfin_all')


def write_clickhouse(con_, table_name, term, data):
    cols = con_.get_columns(table_name)
    cols = [column_map[c] if c in column_map else c for c in cols]
    data.loc[:,'TERM_TYP'] = term
    data = data.rename_axis('YEAR').reset_index()
    str_cols = data.select_dtypes(exclude='float').columns
    
    data = data.reindex(cols, axis='columns')
    data['_ts'] = datetime.datetime.now()
    data.loc[:, str_cols] = data.loc[:, str_cols].fillna('').astype(str)

    try:
        logger.debug(f'INSERT INTO {table_name} VALUES')
        n = con_.get_client().insert_dataframe(f'INSERT INTO {table_name} VALUES', data)
        if n == 0:
            logger.error(f'0 rows written: {table_name}')        
    except Exception as e:
        logger.error(f'{e}')
    logger.debug(f'{data}')
    

def write_csv(path, cmp_cd, data):
    s = data.to_csv(None, quoting=csv.QUOTE_NONNUMERIC, na_rep='NA').replace('"NA"', 'NA')
    target_path = path + '/' + '99YEARLY'
    logger.info(f"path - {target_path}/A{cmp_cd}.csv")
    logger.debug(f"data - \n{s}")
    if not os.path.exists(target_path):
        os.makedirs(target_path)
    if True:
        with open(target_path + "/" + 'A' +  cmp_cd + ".csv1", 'w') as f:
            f.write(s)

def generate_annual_cmp_fin_all(conf, to_csv=True, to_clickhouse=True):
    con_ = ClickHouseConnection(
        host=conf['ClickHouse']['host'],
        user=conf['ClickHouse']['user'],
        password=conf['ClickHouse']['password'],
        database='financial', )

    path = conf['Path']['by_company_cmp_fin_all_path']
    termaccts = [c.value for c in TermAcctDir]

    sep_path = path + '/' + termaccts[0]
    con_path = path + '/' + termaccts[2]
    if os.path.exists(sep_path):
        sep_cmps_csv = os.listdir(sep_path)
    else:
        sep_cmps_csv = []
    if os.path.exists(con_path):
        con_cmps_csv = os.listdir(con_path)
    else:
        con_cmps_csv = []
    cmps = sorted([f.replace('.csv', '')[1:] for f in sep_cmps_csv + con_cmps_csv if '.csv' in f and 'csv1' not in f])
    cmps = np.unique(cmps)

    db = MariaDBReader(conf['MariaDB']['connection_uri'] + '/' + conf['MariaDB']['db_name'])

    # check master
    for cmp in cmps:
        logger.info(f'processing annual_cmpfin_all: {cmp}')
        try:
            cmpprd = db.read_financial(table_name='TF_CMP_FINPRD', cond=f"WHERE CMP_CD = '{cmp.replace('A','')}'").astype('str')
            cmpprd = cmpprd.loc[(cmpprd.FS_QTR == '4') & (cmpprd.FS_USE_YN == '1'), ["FS_YEAR","MASTER_CHK","FS_USE_YN"]]
            sep = pd.DataFrame()
            con = pd.DataFrame()
            if os.path.exists(sep_path + '/' + 'A' + cmp + '.csv1'):
                sep = pd.read_csv(sep_path + '/' + 'A' + cmp + '.csv1', encoding='euc-kr', dtype='str', index_col=0)
                sep = sep[sep.FS_USE_YN=='1']
                sep = sep[sep.FS_QTR=='4'].set_index('YEAR')
                sep = sep.loc[sep.index.intersection(cmpprd[cmpprd.MASTER_CHK=='P'].FS_YEAR),:]

            if os.path.exists(con_path + '/' + 'A' + cmp + '.csv1'):
                con = pd.read_csv(con_path + '/' + 'A' + cmp + '.csv1', encoding='euc-kr', dtype='str', index_col=0)
                con = con[con.FS_USE_YN=='1']
                con['YEAR'] = con['FS_YEAR']                
                con = con[con.FS_QTR=='4'].set_index('YEAR')
                con = con.loc[con.index.intersection(cmpprd[cmpprd.MASTER_CHK=='C'].FS_YEAR),:]

            # FIXME mixing 'P' and 'C' would be harmful for financial analysis
            data = pd.concat([sep,con])
            if len(data) == 0:
                logger.warning(f'empty {cmp}')
                continue

            data = data.rename_axis('YEAR')
            data = data.reset_index().drop_duplicates(subset='YEAR', keep='first')
            data = data.set_index('YEAR').sort_index()

            if 'Total_Stockholders._Equity.' in data.columns:
                data = data.drop('Total_Stockholders._Equity.', axis='columns')
            
            data[data.columns[22:]] = data[data.columns[22:]].astype('float')
            
            if to_csv:
                write_csv(path, cmp, data)
            if to_clickhouse:
                write_clickhouse(con_, 'annual_cmpfin_all', 'YEARLY',  data)

        except Exception as e:
            logger.error(f'{e}')


def generate_annual_cmp_fin_all_ch(conf, start_dt='00000000', end_dt='99999999', to_clickhouse=True):
    con_ = ClickHouseConnection(
        host=conf['ClickHouse']['host'],
        user=conf['ClickHouse']['user'],
        password=conf['ClickHouse']['password'],
        database='financial', )

    termaccts = [c.value for c in TermAcctMethod]
    db = MariaDBReader(conf['MariaDB']['connection_uri'] + '/' + conf['MariaDB']['db_name'])
    company = db.read_financial(table_name='TC_COMPANY', cond="WHERE LIST_YN=1 AND MASTER_CHK IS NOT NULL").astype('str')
    company = company.set_index('CMP_CD')

    cmps = con_.get_client().query_dataframe(f"SELECT DISTINCT(CMP_CD) FROM wisefn.TF_CMP_FINDATA WHERE DNDATE >= '{start_dt}' AND DNDATE <= '{end_dt}'")
    cmps = company.index.intersection(list(cmps.loc[:, 'CMP_CD']))
    cmps = sorted(list(cmps))    
    
    for cmp in cmps:
        logger.info(f'processing annual_cmpfin: {cmp}')
        try:
            cmpprd = db.read_financial(table_name='TF_CMP_FINPRD', cond=f"WHERE CMP_CD = '{cmp.replace('A','')}'").astype('str')
            cmpprd = cmpprd.loc[(cmpprd.FS_QTR == '4') & (cmpprd.FS_USE_YN == '1'), ["FS_YEAR","MASTER_CHK","FS_USE_YN"]]

            table_name = 'cmpfin_bycmp_all'
            sep = con_.get_client().query_dataframe(f"SELECT * FROM {table_name} WHERE CMP_CD = '{cmp}' AND TERM_TYP='{termaccts[0]}'")
            if len(sep) > 0:
                sep.pop('YYMM')
                sep = sep[sep.FS_USE_YN=='1']
                sep['YEAR'] = sep['FS_YEAR']
                sep = sep[sep.FS_QTR=='4'].set_index('YEAR')
                sep = sep.loc[sep.index.intersection(cmpprd[cmpprd.MASTER_CHK=='P'].FS_YEAR),:]
                sep = sep.reset_index().sort_values(['YEAR','_ts']).drop_duplicates(subset=['YEAR'], keep='last').set_index('YEAR')
            
            con = con_.get_client().query_dataframe(f"SELECT * FROM {table_name} WHERE CMP_CD = '{cmp}' AND TERM_TYP='{termaccts[2]}'")
            if len(con) > 0:
                con.pop('YYMM')
                con = con[con.FS_USE_YN=='1']
                con['YEAR'] = con['FS_YEAR']
                con = con[con.FS_QTR=='4'].set_index('YEAR')
                con = con.loc[con.index.intersection(cmpprd[cmpprd.MASTER_CHK=='C'].FS_YEAR),:]
                con = con.reset_index().sort_values(['YEAR','_ts']).drop_duplicates(subset=['YEAR'], keep='last').set_index('YEAR')

            data = pd.concat([sep,con])
            if len(data) == 0:
                logger.warning(f'empty {cmp}')
                continue
           
            data = data.rename_axis('YEAR').reset_index().drop_duplicates(subset=['YEAR'], keep='first').set_index('YEAR').sort_index()
            data.loc[:, data.columns[23:]] = data.loc[:, data.columns[23:]].astype('float')
            data = data.rename(columns=column_map)

            if to_clickhouse:
                write_clickhouse(con_, 'annual_cmpfin_all', 'YEARLY', data)

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

    # generate_annual_cmp_fin_all(conf)
    generate_annual_cmp_fin_all_ch(conf)
