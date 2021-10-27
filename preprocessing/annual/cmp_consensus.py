import csv
import datetime
import logging
import pandas as pd
import numpy as np
import os
import sys
import re

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


logger = logging.getLogger('annual_cmpcns')


def write_clickhouse(con, table_name, term, cmp_cd, data):
    cols = con.get_columns(table_name)
    data['CMP_CD'] = cmp_cd
    data['TERM_TYP'] = term
    data = data.reset_index()
    str_cols = data.select_dtypes(exclude='float').columns
    data.loc[:, str_cols] = data.loc[:, str_cols].fillna('')
    data = data.reindex(cols, axis='columns')
    data['_ts'] = datetime.datetime.now()

    try:
        n = con.get_client().insert_dataframe(f'INSERT INTO {table_name} VALUES', data)
        if n == 0:
            logger.error(f'0 rows written: {table_name}')
    except Exception as e:
        logger.error(f'{e}')
    logger.debug(f'data - {data}')

def write_csv(path, cmp_cd, dt, data):
    s = data.to_csv(None, quoting=csv.QUOTE_NONNUMERIC, na_rep='NA').replace('"NA"', 'NA')
    target_path = path + '/' + dt + '/' + '99ALL_CUM'
    logger.info(f"path - {target_path}/A{cmp_cd}.csv")
    logger.debug(f"data - \n{s}")
    if not os.path.exists(target_path):
        os.makedirs(target_path)
    if True:
        with open(target_path + '/' + 'A' + cmp_cd + ".csv1", 'w') as f:
            f.write(s)
    
def generate_annual_cmp_cns(conf, start_cns_dt='00000000', end_cns_dt='99999999', to_csv=True, to_clickhouse=True):
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

    if not os.path.exists(path):
        logger.error(f'path does not exists: {path}')
        return
    dates = os.listdir(path)
    dates = [d for d in sorted(dates) if re.match(r'\d{6}', d)]
    dates = [d for d in dates if start_cns_dt <= d <= end_cns_dt]
    date_list = dates

    # TODO apply company.FYE_MN
    # company = db.read_financial(table_name='TC_COMPANY')
    # cols = company.select_dtypes(include='float').columns
    # company.loc[:, cols] = company.loc[:, cols].astype(pd.Int64Dtype())
    
    for cmp in cmps[:]:
        logger.info(f'processing annual_cmpcns: {cmp}')
        for dt in date_list[:]:
            try:
                # settlement month '12' and other settlement months are treated differently
                # use SEP_CUM first. if that fails, use CON_CUM
                sep = pd.DataFrame()
                con = pd.DataFrame()
                sep_path = path + '/' + dt + '/' +  termaccts[0]
                con_path = path + '/' + dt + '/' +  termaccts[2]
                if os.path.exists(sep_path + '/' + 'A'+ cmp + '.csv'):
                    logger.debug(sep_path + '/' + 'A'+ cmp + '.csv')
                    sep = pd.read_csv(sep_path + '/' + 'A'+ cmp + '.csv', dtype='str')
                    sep = sep.set_index(sep.columns[0])
                    sep = sep[sep.CONS_YN == '0']
                    sep.index = [idx[:-2] if idx[-2:] == '12' else str(int(idx[:-2]) - 1) for idx in sep.index]
                
                if os.path.exists(con_path + '/' + 'A'+ cmp + '.csv'):
                    logger.debug(con_path + '/' + 'A'+ cmp + '.csv')
                    con = pd.read_csv(con_path + '/' + 'A'+ cmp + '.csv', dtype='str')
                    con = con.set_index(con.columns[0])
                    con = con[con.CONS_YN == '1']
                    con.index = [idx[:-2] if idx[-2:] == '12' else str(int(idx[:-2]) - 1) for idx in con.index]
                
                df = pd.concat([sep, con])
                if len(df) == 0:
                    logger.debug(f'empty {cmp}, {dt}')
                    continue

                df = df.rename_axis('YEAR').reset_index().drop_duplicates(subset='YEAR', keep='first').set_index('YEAR').sort_index()
                df = df.loc[df.index.drop_duplicates(keep='first'),:]
                df[df.columns[5:]] = df[df.columns[5:]].astype('float') 
                if to_csv:
                    write_csv(path, cmp, dt, df)
                if to_clickhouse:
                    write_clickhouse(con_, 'annual_cmpcns', 'ALL_CUM', cmp, dt, df)
            
            except Exception as e:
                logger.error(f'{e}')

def generate_annual_cmp_cns_ch(conf, start_dt='00000000', end_dt='99999999', to_clickhouse=True):
    con_ = ClickHouseConnection(
        host=conf['ClickHouse']['host'],
        user=conf['ClickHouse']['user'],
        password=conf['ClickHouse']['password'],
        database='financial', )    

    termaccts = [c.value for c in TermAcctMethod]
    table_name = 'cmpcns_bycmp'

    db = MariaDBReader(conf['MariaDB']['connection_uri'] + '/' + conf['MariaDB']['db_name'])
    est = db.read_financial(table_name='TT_EST_MASTER')
    cmps = est.CMP_CD.unique()
    company = db.read_financial(table_name='TC_COMPANY')
    cols = company.select_dtypes(include='float').columns
    company.loc[:, cols] = company.loc[:, cols].astype(pd.Int64Dtype())
    
    cns_cmps = con_.get_client().query_dataframe(f"SELECT DISTINCT(CMP_CD) FROM wisefn.TT_CMP_CNS_DATA WHERE DNDATE >= '{start_dt}' AND DNDATE <= '{end_dt}'")
    cmps = set(cns_cmps.iloc[:,0]).intersection(set(cmps))
    cmps = sorted(list(cmps))

    for cmp in cmps[:]:
        logger.info(f'processing annual_cmpcns: {cmp}')
        data = con_.get_client().query_dataframe(f"SELECT * FROM {table_name} WHERE CMP_CD = '{cmp}' ")
        if len(data) == 0:
            continue
        
        if isinstance(company[company.CMP_CD == cmp].FYE_MN.iloc[0], np.number):
            fyemn = str(company[company.CMP_CD == cmp].FYE_MN.iloc[0])
        else:
            fyemn = ''

        try:
            # settlement month '12' and other settlement months are treated differently
            # use SEP_CUM first. if that fails, use CON_CUM
            sep = data[(data.TERM_TYP == termaccts[0])]
            sep = sep[sep.CONS_YN == '0']
            sep['YEAR'] = [idx[:-2] if idx[-2:] == fyemn and fyemn == '12' else str(int(idx[:-2]) - 1) if idx[-2:] == fyemn else np.nan for idx in sep.YYMM]
            sep.pop('YYMM')
            sep = sep.dropna(subset=['YEAR'])
            sep = sep.sort_values(['YEAR', '_ts']).drop_duplicates(subset=['YEAR'], keep='last')

            con = data[(data.TERM_TYP == termaccts[2])]
            con = con[con.CONS_YN == '1']
            con['YEAR'] = [idx[:-2] if idx[-2:] == fyemn and fyemn == '12' else str(int(idx[:-2]) - 1) if idx[-2:] == fyemn else np.nan for idx in con.YYMM]
            con.pop('YYMM')
            con = con.dropna(subset=['YEAR'])
            con = con.sort_values(['YEAR', '_ts']).drop_duplicates(subset=['YEAR'], keep='last')

            df = pd.concat([sep, con])
            if len(df) == 0:
                logger.debug(f'empty {cmp}')
                continue
            # print(cmp, dt, df)
            df = df.drop_duplicates(subset=['YEAR'], keep='first').set_index('YEAR').sort_index()
            df[df.columns[9:]] = df[df.columns[9:]].astype('float') 
            if to_clickhouse:
                write_clickhouse(con_, 'annual_cmpcns', 'ALL_CUM', cmp, df)
        
        except Exception as e:
            logger.error(f'{e}')


if __name__ == '__main__':
    config_file = '/home/mining/PycharmProjects/wisefn_data_processing/db3.config'
    end_cns_dt = datetime.datetime.now().strftime('%Y%m%d')
    
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    if len(sys.argv) > 2:
        start_dt = sys.argv[2]
    if len(sys.argv) > 3:
        end_dt = sys.argv[3]

    conf = get_config(config_file)
    logging.basicConfig(level=eval(conf['Logging']['level']),
        # handlers=[eval(conf['Logging']['handler'])],
        format=conf['Logging']['format'],)
    
    #FIXME
    start_dt = '00000000'
    end_dt = '99999999'

    # generate_annual_cmp_cns(conf, start_dt=start_dt, end_dt=end_dt)
    generate_annual_cmp_cns_ch(conf, start_dt=start_dt, end_dt=end_dt)