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


logger = logging.getLogger('by_item_sec_cns')


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
            
            n = con.get_client().insert_dataframe(f'INSERT INTO {table_name} VALUES', df)
            if n == 0:
                logger.error(f'0 rows written: {table_name}')                 
        except Exception as e:
            logger.error(f'{e}')


def write_csv(path, term_acct, item_name, data):
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


def generate_sec_cns_by_item(conf, start_dt=None, end_dt=None, to_csv=False, to_clickhouse=True):
    con_ = ClickHouseConnection(
        host=conf['ClickHouse']['host'],
        user=conf['ClickHouse']['user'],
        password=conf['ClickHouse']['password'],
        database='financial', )

    db = MariaDBReader(conf['MariaDB']['connection_uri'] + '/' + conf['MariaDB']['db_name'])
    path = conf['Path']['by_item_sec_cns_path']

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

    # for every group name of type 'W' and 'G'
    sector = db.read_financial(table_name='TC_SECTOR').fillna('').astype('str')
    sector = sector.set_index('SEC_CD')

    for item_cd, item_name in cns_need2[:]:
        try:
            logger.info(f'reading item_cd: {item_cd}')
            # cns_dts = con_.get_client().query_dataframe(f"SELECT DISTINCT(CNS_DT) FROM wisefn.TT_SEC_CNS_DATA WHERE DNDATE >= '{start_dt}' AND DNDATE <= '{end_dt}' AND ITEM_CD='{item_cd}'")
            # if len(cns_dts) == 0:
            #     continue
            # cond = '( '+'OR '.join([f"CNS_DT = '{x}' " for x in list(cns_dts.loc[:, 'CNS_DT'])]) + ')'
            # cond = f"WHERE ITEM_CD = '{item_cd}'" + ' AND ' + cond
            # logger.debug(f'select condition: {cond}')

            min_cns_dt = con_.get_client().query_dataframe(f"SELECT MIN(CNS_DT) FROM wisefn.TT_SEC_CNS_DATA WHERE DNDATE >= '{start_dt}' AND DNDATE <= '{end_dt}' AND ITEM_CD='{item_cd}'")
            if len(min_cns_dt) == 0:
                return
            cond = f"WHERE ITEM_CD = '{item_cd}'" + ' AND ' + f"CNS_DT >= '{min_cns_dt.iloc[0, 0]}'"
            logger.debug(f'select condition: {cond}')

            findata = db.read_financial(table_name='TT_SEC_CNS_DATA', cond=cond)
            if len(findata) == 0:
                logger.info(f'empty item_cd: {item_cd}')
                continue
            logger.info(f'read complete item_cd: {item_cd}')

            findata.loc[:, findata.columns[findata.columns!='VAL']] = findata.loc[:, findata.columns[findata.columns!='VAL']].astype('str')
            findata = pd.pivot_table(findata, values='VAL', index=['CNS_DT', 'YYMM', 'TERM_TYP'] , columns= ['SEC_CD'])
            findata = findata.loc[:, findata.columns.intersection(sector.index)]

            for term_acct in TermAcctMethod:
                if term_acct.value not in findata.index.get_level_values(2):
                    continue
                findata_ = findata.loc[:,:,term_acct.value,:]
                if item_cd in item_unit.index:
                    findata_ = findata_ * 1000
                if to_csv:
                    write_csv(path, term_acct, item_name, findata_)
                if to_clickhouse:
                    write_clickhouse(con_, 'seccns_byitem', term_acct, item_name, findata_)

        except Exception as e:
            logger.error(f'{e}')


if __name__ == '__main__':
    config_file = '/home/mining/PycharmProjects/wisefn_data_processing/db3.config'
    start_dt = '20000101'
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

    #FIXME
    # start_dt = '20000101'
    # end_dt = None
    generate_sec_cns_by_item(conf, start_dt=start_dt, end_dt=end_dt)
