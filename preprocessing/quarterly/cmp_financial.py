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

logger = logging.getLogger('generate_quarterly_cmp_fin')


def write_clickhouse(con, table_name, term, data):
    cols = con.get_columns(table_name)
    data.loc[:,'TERM_TYP'] = term
    data = data.reset_index()
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

def write_csv(path, cmp_cd, data):
    s = data.to_csv(None, quoting=csv.QUOTE_NONNUMERIC, na_rep='NA').replace('"NA"', 'NA')
    target_path = path + '/' + '99QUARTERLY'
    logger.info(f"path - {target_path}/A{cmp_cd}.csv")
    logger.debug(f"data - \n{s}")
    if not os.path.exists(target_path):
        os.makedirs(target_path)
    if True:
        with open(target_path + '/' + 'A' + cmp_cd + '.csv1', 'w') as f:
            f.write(s)

def generate_quarterly_cmp_fin(conf, to_csv=True, to_clickhouse=True):
    con_ = ClickHouseConnection(
        host=conf['ClickHouse']['host'],
        user=conf['ClickHouse']['user'],
        password=conf['ClickHouse']['password'],
        database='financial', )    

    termaccts = [c.value for c in TermAcctDir]
    path = conf['Path']['by_company_cmp_fin_path']

    sep_path = path + '/' + termaccts[1]
    sep_cmps_csv = []
    if os.path.exists(sep_path):
        sep_cmps_csv = os.listdir(sep_path)
    con_path = path + '/' + termaccts[3]
    con_cmps_csv = []
    if os.path.exists(con_path):
        con_cmps_csv = os.listdir(con_path)

    db = MariaDBReader(conf['MariaDB']['connection_uri'] + '/' + conf['MariaDB']['db_name'])
    # company = db.read_financial(table_name='TC_COMPANY', cond="WHERE LIST_YN=1 AND MASTER_CHK IS NOT NULL").astype('str')
    # cmps = company.set_index('CMP_CD')
    # cmps = np.unique(cmps.index)
    cmps = sorted([f.replace('.csv', '')[1:] for f in sep_cmps_csv + con_cmps_csv if '.csv' in f and 'csv1' not in f])
    cmps = np.unique(cmps)

    for cmp in cmps[:]:
        logger.info(f'processing quarterly_cmpfin: {cmp}')
        try:
            cmpprd = db.read_financial(table_name='TF_CMP_FINPRD', cond=f"WHERE CMP_CD = '{cmp.replace('A','')}'").astype('str')
            cmpprd = cmpprd.loc[cmpprd.FS_USE_YN == '1', ["FS_YEAR","FS_QTR","MASTER_CHK","FS_USE_YN"]]
            sep_path = path + '/' + termaccts[1] 
            con_path = path + '/' + termaccts[3]

            sep = pd.DataFrame()
            con = pd.DataFrame()
            if os.path.exists(sep_path + '/' + 'A' + cmp + '.csv'):
                sep = pd.read_csv(sep_path + '/' + 'A' + cmp + '.csv1', encoding='euc-kr', dtype='str', index_col=0)
                indices = sep.FS_YEAR + '.' + sep.FS_QTR
                sep = sep.set_index(indices)
                sep = sep.loc[sep.MASTER_CHK == 'P',:]
            if os.path.exists(con_path + '/' + 'A' + cmp + '.csv'):
                con = pd.read_csv(con_path + '/' + 'A' + cmp + '.csv1', encoding='euc-kr', dtype='str', index_col=0)
                indices = con.FS_YEAR + '.' + con.FS_QTR
                con = con.set_index(indices)
                con = con.loc[con.MASTER_CHK == 'C',:]

            df = pd.concat([sep,con])
            if len(df) == 0:
                logger.warning(f'empty {termaccts[1]}, {cmp}')
                continue
            df = df.rename_axis('YYMM').reset_index().drop_duplicates(subset=['YYMM'], keep='first').set_index('YYMM')
            df[df.columns[22:]] = df[df.columns[22:]].astype('float')
            if to_csv:
                write_csv(path, cmp, df)
            if to_clickhouse:
                write_clickhouse(con_, 'quarterly_cmpfin', 'QUARTERLY', df)

        except Exception as e:
            logger.error(f'{e}')

def generate_quarterly_cmp_fin_ch(conf, start_dt='00000000', end_dt='99999999', to_clickhouse=True):
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

    for cmp in cmps[:]:
        logger.info(f'processing quarterly_cmpfin: {cmp}')
        try:
            table_name = 'cmpfin_bycmp'
            sep = con_.get_client().query_dataframe(f"SELECT * FROM {table_name} WHERE CMP_CD = '{cmp}' AND TERM_TYP='{termaccts[1]}'")
            if len(sep) > 0:
                indices = sep.FS_YEAR + '.' + sep.FS_QTR
                sep = sep.set_index(indices)
                sep = sep.loc[sep.QTR_MASTER == 'P',:]
                sep = sep.sort_values(['YYMM', '_ts']).drop_duplicates('YYMM', keep='last')

            con = con_.get_client().query_dataframe(f"SELECT * FROM {table_name} WHERE CMP_CD = '{cmp}' AND TERM_TYP='{termaccts[3]}'")
            if len(con) > 0:
                indices = con.FS_YEAR + '.' + con.FS_QTR
                con = con.set_index(indices)
                con = con.loc[con.QTR_MASTER == 'C',:]
                con = con.sort_values(['YYMM', '_ts']).drop_duplicates('YYMM', keep='last')

            data = pd.concat([sep,con])
            if len(data) == 0:
                logger.warning(f'empty {termaccts[1]}, {cmp}')
                continue
            data = data.drop_duplicates(subset=['YYMM'], keep='first')
            data.pop('YYMM')
            data = data.rename_axis('YYMM')
            data[data.columns[23:]] = data[data.columns[23:]].astype('float')
            if to_clickhouse:
                write_clickhouse(con_, 'quarterly_cmpfin', 'QUARTERLY', data)

        except Exception as e:
            logger.error(f'{e}')


if __name__ == '__main__':
    config_file = '/home/mining/projects/wisefn_data_processing/db2513.config'
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    conf = get_config(config_file)

    logging.basicConfig(level=eval(conf['Logging']['level']),
        handlers=[eval(conf['Logging']['handler'])],
        format=conf['Logging']['format'],
        )

    # generate_quarterly_cmp_fin(conf)
    generate_quarterly_cmp_fin_ch(conf)
