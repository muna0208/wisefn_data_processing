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
from preprocessing.defines import fin_need2
from logging.handlers import RotatingFileHandler
from preprocessing.clickhouse_financial.clickhouse_helper import ClickHouseConnection


logger = logging.getLogger('by_item_cmp_fin')

 
def write_clickhouse(con, table_name, term_acct, item_name, data):
    cols = con.get_columns(table_name)
    new_cols = data.columns.difference(cols)
    
    for col in new_cols:
        logger.info(f'new column added for table {table_name}: {col} Float64')
        con.add_column(table_name, col, 'Float64')
    if len(new_cols) > 0:
        cols = con.get_columns(table_name)
    
    data = data.reset_index()
    data['TERM_TYP'] = term_acct.value
    data['ITEM_NM'] = item_name
    str_cols = data.select_dtypes(exclude='float').columns
    data.loc[:,str_cols] = data.loc[:,str_cols].fillna('')
    data = data.reindex(cols, axis='columns')
    data['_ts'] = datetime.datetime.now()

    try:
        n = con.get_client().insert_dataframe(f'INSERT INTO {table_name} VALUES', data)
        if n == 0:
            logger.error(f'0 rows written: {table_name}')             
    except Exception as e:
        logger.error(f'{e}')

def write_csv(path, term_acct, item_name, data):
    target_path = path + '/' + TermAcctDir[term_acct.name].value
    if not os.path.exists(target_path):
        os.makedirs(target_path)
    s = data.to_csv(None, na_rep='NA', quoting=csv.QUOTE_NONNUMERIC).replace('"NA"', 'NA')
    logger.info(f"path - {target_path + '/' + item_name + '.csv'}")
    logger.debug(f"data - \n{s}")
    if True:
        with open(target_path + '/' + item_name + '.csv1', 'w') as f:
            f.write(s)

def generate_cmp_fin_by_item(conf, to_csv=True, to_clickhouse=True):
    con_ = ClickHouseConnection(
        host=conf['ClickHouse']['host'],
        user=conf['ClickHouse']['user'],
        password=conf['ClickHouse']['password'],
        database='financial', )

    db = MariaDBReader(conf['MariaDB']['connection_uri'] + '/' + conf['MariaDB']['db_name'])
    path = conf['Path']['by_item_cmp_fin_path'] 

    items = db.read_financial(table_name='TZ_ITEM').astype('str')
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
    item_unit = items[['ITEM_CD', 'ITEM_TYP', 'UNT_TYP']]\
        .drop_duplicates().set_index(['ITEM_CD', 'ITEM_TYP'])
    item_unit = item_unit.loc[item_unit.UNT_TYP == '천원']

    company = db.read_financial(table_name='TC_COMPANY', cond="WHERE LIST_YN=1 AND MASTER_CHK IS NOT NULL").astype('str')
    company = company.set_index('CMP_CD')

    for item_cd, item_name in fin_need2[:]:
        try:
            logger.info(f'reading item_cd: {item_cd}')
            findata = db.read_financial(table_name='TF_CMP_FINDATA', cond=f"WHERE ITEM_CD='{item_cd}' AND ITEM_TYP='M'")
            if len(findata) == 0:
                logger.info(f'emtpy item_cd: {item_cd}')
                continue
            logger.info(f'read compelete item_cd: {item_cd}')
            findata = findata.loc[:, findata.columns[findata.columns != 'ITEM_TYP']]
            findata.loc[:, findata.columns[findata.columns!='VAL']] = findata.loc[:, findata.columns[findata.columns!='VAL']].astype('str')
            findata = pd.pivot_table(findata, values='VAL', index=['YYMM', 'TERM_TYP'] , columns= ['CMP_CD'])
            findata.columns = ['A' + c for c in findata.columns]

            for term_acct in TermAcctMethod:
                if term_acct.value not in findata.index.get_level_values(1):   
                    continue
                findata_ = findata.loc[:,term_acct.value,:]
                if pd.IndexSlice[item_cd, 'M'] in item_unit.index and len(item_unit.loc[(item_cd, 'M'), :]) > 0:
                    findata_ = findata_ * 1000
                findata_ = findata_.dropna(how='all').dropna(how='all', axis=1)
                if to_csv:
                    write_csv(path, term_acct, item_name, findata_)
                if to_clickhouse:
                    write_clickhouse(con_, 'cmpfin_byitem', term_acct, item_name, findata_)
        
        except Exception as e:
            logger.error(f'{e}')


if __name__ == '__main__':
    config_file = '/home/mining/PycharmProjects/wisefn_data_processing/db3.config'

    if len(sys.argv) > 1:
        config_file = sys.argv[1]

    conf = get_config(config_file)
    logging.basicConfig(level=eval(conf['Logging']['level']),
        handlers=[eval(conf['Logging']['handler'])],
        format=conf['Logging']['format'],
        )

    generate_cmp_fin_by_item(conf)
