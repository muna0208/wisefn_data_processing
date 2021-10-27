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
from collections import namedtuple
from preprocessing.defines import cns_need2
from logging.handlers import RotatingFileHandler
from preprocessing.clickhouse_financial.clickhouse_helper import ClickHouseConnection


logger = logging.getLogger('by_item_cmp_cns')


def write_clickhouse(con, table_name, term_acct, item_name, data):
    cols = con.get_columns(table_name)
    new_cols = data.columns.difference(cols)
    
    for col in new_cols:
        logger.info(f'new column added for table {table_name}: {col} Float64')
        con.add_column(table_name, col, 'Float64')
    if len(new_cols) > 0:
        cols = con.get_columns(table_name)

    for cns_dt in data.index.get_level_values(0).unique():
        try:
            df = data.loc[[cns_dt],:,:].rename_axis(['CNS_DT', 'YYMM']).reset_index(level=[0,1])
            df['TERM_TYP'] = term_acct.value
            df['ITEM_NM'] = item_name
            df = df.reindex(cols, axis='columns')
            df['_ts'] = datetime.datetime.now()
            
            n = con.get_client().insert_dataframe(f"INSERT INTO {table_name} ({','.join(df.columns)}) VALUES", df)
            if n == 0:
                logger.error(f'0 rows written: {table_name}')
        except Exception as e:
            logger.error(f'{e}')


def write_csv(path, term_acct, item_name, data):
    # check if CNS_DT is less than YYMM
    # data = data[(np.array([i//100 for i in data.index.get_level_values('CNS_DT').astype(int)]) - np.array([i for i in data.index.get_level_values('YYMM').astype(int)])) <= 0]
    for cns_dt in data.index.get_level_values(0).unique():
        target_path = path + "/" + cns_dt + "/" + TermAcctDir[term_acct.name].value
        if not os.path.exists(target_path):
            os.makedirs(target_path)
        df = data.loc[[cns_dt],:,:].rename_axis(['CNS_DT', '']).reset_index(level=[0]).drop('CNS_DT', axis='columns')
        df = df.sort_index()            
        s = df.to_csv(None, na_rep='NA', quoting=csv.QUOTE_NONNUMERIC).replace('"NA"', 'NA')
        logger.info(f"path - {target_path + '/' + item_name + '.csv'}")
        logger.debug(f"data - \n{s}")
        if True:
            with open(target_path + "/" + item_name + ".csv1", 'w') as f:
                f.write(s)

def generate_cmp_cns_by_item_mp(conf, start_dt, end_dt, item_cd, item_name, item_unit, to_csv, to_clickhouse):
    con_ = ClickHouseConnection(
        host=conf['ClickHouse']['host'],
        user=conf['ClickHouse']['user'],
        password=conf['ClickHouse']['password'],
        database='financial', )
    db = MariaDBReader(conf['MariaDB']['connection_uri'] + '/' + conf['MariaDB']['db_name'])
    path = conf['Path']['by_item_cmp_cns_path']

    try:
        logger.info(f'reading item_cd: {item_cd}')
        # cns_dts = con_.get_client().query_dataframe(f"SELECT DISTINCT(CNS_DT) FROM wisefn.TT_CMP_CNS_DATA WHERE DNDATE >= '{start_dt}' AND DNDATE <= '{end_dt}' AND ITEM_CD='{item_cd}'")
        # if len(cns_dts) == 0:
        #     return
        # cond = '( '+'OR '.join([f"CNS_DT = '{x}' " for x in list(cns_dts.loc[:, 'CNS_DT'])]) + ')'
        # cond = f"WHERE ITEM_CD = '{item_cd}'" + ' AND ' + cond
        # logger.debug(f'select condition: {cond}')

        min_cns_dt = con_.get_client().query_dataframe(f"SELECT MIN(CNS_DT) FROM wisefn.TT_CMP_CNS_DATA WHERE DNDATE >= '{start_dt}' AND DNDATE <= '{end_dt}' AND ITEM_CD='{item_cd}'")
        if len(min_cns_dt) == 0:
            return
        cond = f"WHERE ITEM_CD = '{item_cd}'" + ' AND ' + f"CNS_DT >= '{min_cns_dt.iloc[0, 0]}'"
        logger.debug(f'select condition: {cond}')

        findata = db.read_financial(table_name='TT_CMP_CNS_DATA', cond=cond)
        if len(findata) == 0:
            logger.info(f'empty item_cd: {item_cd}')
            return
        logger.info(f'read complete item_cd: {item_cd}')
        
        findata.loc[:, findata.columns[findata.columns!='VAL']] = findata.loc[:, findata.columns[findata.columns!='VAL']].astype('str')
        findata = pd.pivot_table(findata, values='VAL', index=['CNS_DT', 'YYMM', 'TERM_TYP'] , columns= ['CMP_CD'])
        findata.columns = ['A'+ c for c in findata.columns]

        for term_acct in TermAcctMethod:
            if term_acct.value not in findata.index.get_level_values(2):
                return
            findata_ = findata.loc[:,:,term_acct.value,:]
            if item_cd in item_unit.index:
                findata_.loc[:, :, :] = findata_.loc[:, :, :] * 1000
            if to_csv:
                write_csv(path, term_acct, item_name, findata_)
            if to_clickhouse:
                write_clickhouse(con_, 'cmpcns_byitem', term_acct, item_name, findata_)    
    
    except Exception as e:
        logger.error(f'{e}')


def generate_cmp_cns_by_item(conf, start_dt='00000000', end_dt='99999999', to_csv=False, to_clickhouse=True):
    db = MariaDBReader(conf['MariaDB']['connection_uri'] + '/' + conf['MariaDB']['db_name'])

    items = db.read_financial(table_name='TZ_ITEM', cond="WHERE ITEM_TYP='E'").fillna('').astype('str')
    items = items.iloc[:,:7]
    items.loc[:, 'ITEM_NM_ENG'] = items.loc[:,'ITEM_NM_ENG'].apply(
                                                                lambda x: x.strip()
                                                                    .replace('(', '_')
                                                                    .replace(')','')
                                                                    .replace('/','')
                                                                    .replace('-','_')
                                                                    .replace('.','')
                                                                    .replace(',','')
                                                                    .replace("'",'')
                                                                    .replace(' ','_'))

    item_unit = items[['ITEM_CD', 'UNT_TYP']].drop_duplicates().set_index('ITEM_CD')
    item_unit = item_unit.loc[item_unit.UNT_TYP == '천원']

    company = db.read_financial(table_name='TC_COMPANY', cond="WHERE LIST_YN=1 AND MASTER_CHK IS NOT NULL").astype('str')
    company = company.set_index('CMP_CD')

    estimate_master = db.read_financial(table_name='TT_EST_MASTER').astype('str')
    estimate_master= estimate_master.set_index('CMP_CD')
            
    import multiprocessing as mp
    proc_map = {}
    for item_cd, item_name in cns_need2[:]:
        proc_map[item_cd] = mp.Process(target=generate_cmp_cns_by_item_mp, args=(conf, start_dt, end_dt, item_cd, item_name, item_unit, to_csv, to_clickhouse))
        proc_map[item_cd].start()   


    for kk in proc_map.keys():
        proc_map[kk].join()   


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
        # handlers=[eval(conf['Logging']['handler'])],
        format=conf['Logging']['format'],
        )

    #FIXME
    start_dt = '20210402'
    end_dt = '20210402'
    generate_cmp_cns_by_item(conf, start_dt=start_dt, end_dt=end_dt)
