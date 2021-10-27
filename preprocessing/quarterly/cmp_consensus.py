import csv
import datetime
import logging
import pandas as pd
import numpy as np
import os
import sys
import re

from pandas.core.groupby.generic import DataFrameGroupBy

from preprocessing.config import config
from preprocessing.paths import FIN_quarterly_path, FIN_yearly_path, filepath, by_ITEM_path, \
    SEC_filepath, by_SEC_ITEM_path, INPUTPATH, ALLINPUTPATH, MAINPATH
from preprocessing.defines import TermAcctMethod, TermAcctDir, FinAcctType

from wisefn_mariadb.db_connection import MariaDBConnection
from wisefn_mariadb.wisefn2mariadb_schema import WiseFNSchema
from wisefn_mariadb.db_reader import MariaDBReader
from wisefn_mariadb.config import get_config
from logging.handlers import RotatingFileHandler
from preprocessing.clickhouse_financial.clickhouse_helper import ClickHouseConnection

logger = logging.getLogger('generate_quarterly_cmp_cns')


def write_clickhouse(con, table_name, term, cmp_cd, data):
    cols = con.get_columns(table_name)
    data['CMP_CD'] = cmp_cd
    data['TERM_TYP'] = term
    data = data.reset_index()
    str_cols = data.select_dtypes(exclude='float').columns
    data.loc[:, str_cols] = data.loc[:, str_cols].fillna('')
    data['_ts'] = datetime.datetime.now()    
    data = data.reindex(cols, axis='columns')

    try:
        n = con.get_client().insert_dataframe(f'INSERT INTO {table_name} VALUES', data)
        if n == 0:
            logger.error(f'0 rows written: {table_name}')
    except Exception as e:
        logger.error(f'{e}')
    logger.debug(f'data - {data}')

def write_csv(path, cmp_cd, dt, data):
    s = data.to_csv(None, quoting=csv.QUOTE_NONNUMERIC, na_rep='NA').replace('"NA"', 'NA')
    target_path = path + '/' + dt + '/' + '99QUARTER_NET'
    logger.info(f"path - {target_path}/A{cmp_cd}.csv")
    logger.debug(f"data - \n{s}")
    if not os.path.exists(target_path):
        os.makedirs(target_path)
    if True:
        with open(target_path + '/' +  'A' + cmp_cd + ".csv1", 'w') as f:
            f.write(s)

def calcuate_terms(df, settle_month):
    indices = np.array(df.index.astype(int))
    ind = indices.copy()
    if settle_month == 6:
        indices[indices%100 > 6]  +=  100
        indices = (indices // 100)*10 + ((indices%100 + 3) // 3 % 4 + 1)
    elif settle_month == 9:
        indices[indices%100 > 9]  +=  100
        indices =  (indices // 100)*10 + ((indices%100 + 0) // 3 % 4 + 1)
    elif settle_month == 12:
        indices = (indices // 100)*10 + ((indices%100 - 3) // 3 % 4 + 1)
    elif settle_month == 3:
        indices[indices%100 == 3]  -=  100
        indices = (indices // 100)*10 + ((indices%100 + 6) // 3 % 4 + 1)
    else:
        indices = (indices // 100)*10 
    return ind, indices / 10

def generate_quarterly_cmp_cns(conf, start_dt, end_dt, to_csv=True, to_clickhouse=True):
    con_ = ClickHouseConnection(
        host=conf['ClickHouse']['host'],
        user=conf['ClickHouse']['user'],
        password=conf['ClickHouse']['password'],
        database='financial', )    

    path = conf['Path']['by_company_cmp_cns_path']
    termaccts = [c.value for c in TermAcctDir]

    db = MariaDBReader(conf['MariaDB']['connection_uri'] + '/' + conf['MariaDB']['db_name'])
    est = db.read_financial(table_name='TT_EST_MASTER')
    cmps = est.CMP_CD.unique()

    company = db.read_financial(table_name='TC_COMPANY')
    cols = company.select_dtypes(include='float').columns
    company.loc[:, cols] = company.loc[:, cols].astype(pd.Int64Dtype())
    company.loc[:, cols].sort_values('FYE_MN')

    if not os.path.exists(path):
        logger.error(f'path does not exists: {path}')
        return
    dates = os.listdir(path)
    dates = [d for d in sorted(dates) if re.match(r'\d{8}', d)]
    dates = [d for d in dates if start_dt <= d <= end_dt]
    date_list = dates

    for cmp in cmps[:]:
        logger.info(f'processing quarterly_cmpcns: {cmp}')
        for dt in date_list[:]:
            # use SEP_NET first. if it fails, use CON_NET
            try:
                sep = pd.DataFrame()
                con = pd.DataFrame()
                sep_path = path + '/' + dt + '/' +  termaccts[1]
                con_path = path + '/' + dt + '/' +  termaccts[3]
                if os.path.exists(sep_path + '/' + 'A'+ cmp + '.csv1'):
                    logger.debug('reading: ' + sep_path + '/' + 'A'+ cmp + '.csv')
                    sep = pd.read_csv(sep_path + '/' + 'A'+ cmp + '.csv1', dtype='str')
                    sep =sep.set_index(sep.columns[0])
                if os.path.exists(con_path + '/' + 'A'+ cmp + '.csv1'):
                    logger.debug('reading: ' + con_path + '/' + 'A'+ cmp + '.csv')
                    con = pd.read_csv(con_path + '/' + 'A'+ cmp + '.csv1', dtype='str')
                    con =con.set_index(con.columns[0])
                df = pd.concat([sep, con])
                df = df.rename_axis('YYMM').reset_index().drop_duplicates(subset='YYMM', keep='first').set_index('YYMM')

                if len(df) == 0:
                    logger.warning(f'empty {dt}, {cmp}')
                    continue
                cmp_info = company[company.CMP_CD == cmp]
                if cmp_info.iloc[0].FYE_MN % 3 != 0:
                    logger.warning(f'Not quarter settlement month stock code: {cmp}, settlement month: {cmp_info.FYE_MN}')
                    continue
                _, indices = calcuate_terms(df, settle_month = cmp_info.iloc[0].FYE_MN)
                
                df = df.reset_index() 
                df['YYMM'] = [str(i) for i in indices]
                df = df.set_index('YYMM')
                df[df.columns[5:]] = df[df.columns[5:]].astype('float')

                if to_csv:
                    write_csv(path, cmp, dt, df)
                if to_clickhouse:
                    write_clickhouse(con_, 'quarterly_cmpcns', 'QUARTER_NET', cmp, dt, df)
            
            except Exception as e:
                logger.error(f'{e}')


def generate_quarterly_cmp_cns_ch(conf, start_dt=None, end_dt=None, to_clickhouse=True):
    con_ = ClickHouseConnection(
        host=conf['ClickHouse']['host'],
        user=conf['ClickHouse']['user'],
        password=conf['ClickHouse']['password'],
        database='financial', )    

    termaccts = [c.value for c in TermAcctMethod]

    db = MariaDBReader(conf['MariaDB']['connection_uri'] + '/' + conf['MariaDB']['db_name'])
    est = db.read_financial(table_name='TT_EST_MASTER')
    cmps = est.CMP_CD.unique()

    company = db.read_financial(table_name='TC_COMPANY')
    cols = company.select_dtypes(include='float').columns
    company.loc[:, cols] = company.loc[:, cols].astype(pd.Int64Dtype())
    # company.loc[:, cols].sort_values('FYE_MN')

    cns_cmps = con_.get_client().query_dataframe(f"SELECT DISTINCT(CMP_CD) FROM wisefn.TT_CMP_CNS_DATA WHERE DNDATE >= '{start_dt}' AND DNDATE <= '{end_dt}'")
    cmps = set(cns_cmps.iloc[:,0]).intersection(set(cmps))
    cmps = sorted(list(cmps))

    table_name = 'cmpcns_bycmp'
    
    for cmp in cmps[:]:
        logger.info(f'processing quarterly_cmpcns: {cmp}')
        df = con_.get_client().query_dataframe(f"SELECT * FROM {table_name} WHERE CMP_CD = '{cmp}' ")
        if len(df) == 0:
            continue

        try:
            sep = df[(df.TERM_TYP == termaccts[1])]
            sep = sep[sep.CONS_YN == '0']
            sep = sep.sort_values(['YYMM', '_ts']).drop_duplicates('YYMM', keep='last')

            con = df[(df.TERM_TYP == termaccts[3])]
            con = con[con.CONS_YN == '1']
            con = con.sort_values(['YYMM', '_ts']).drop_duplicates('YYMM', keep='last')

            data = pd.concat([sep, con])
            if len(data) == 0:
                logger.debug(f'empty {cmp}')
                continue

            data = data.drop_duplicates(subset='YYMM', keep='first').set_index('YYMM')

            if len(data) == 0:
                logger.warning(f'empty {cmp}')
                continue

            cmp_info = company[company.CMP_CD == cmp]
            if cmp_info.iloc[0].FYE_MN % 3 != 0:
                logger.warning(f'Not quarter settlement month stock code: {cmp}, settlement month: {cmp_info.FYE_MN}')
                continue
            _, indices = calcuate_terms(data, settle_month = cmp_info.iloc[0].FYE_MN)
            
            data = data.reset_index() 
            data['YYMM'] = [str(i) for i in indices]
            data = data.set_index('YYMM')
            data[data.columns[9:]] = data[data.columns[9:]].astype('float')

            if to_clickhouse:
                write_clickhouse(con_, 'quarterly_cmpcns', 'QUARTER_NET', cmp, data)
        
        except Exception as e:
            logger.error(f'{e}')



if __name__ == '__main__':
    config_file = '/home/mining/PycharmProjects/wisefn_data_processing/db3.config'
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
        format=conf['Logging']['format'], 
        )

    #FIX ME
    start_dt = '20210401'
    end_dt = '20210401'
    # generate_quarterly_cmp_cns(conf, start_dt, end_dt)
    generate_quarterly_cmp_cns_ch(conf, start_dt, end_dt)


