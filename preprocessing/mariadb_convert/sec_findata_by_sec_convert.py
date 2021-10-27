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

logger = logging.getLogger('by_sector_sec_fin')


def write_clickhouse(con, table_name, term_acct, sec_cd, data, sector):
    cols = con.get_columns(table_name)
    secinfo = pd.DataFrame(index=data.index, columns=['SEC_CD', 'MKT_TYP'])
    secinfo.loc[:, 'TERM_TYP'] = term_acct.value
    secinfo.loc[:, 'SEC_CD'] = sec_cd
    secinfo.loc[:, 'MKT_TYP'] = sector.loc[sec_cd, 'MKT_TYP']
    
    df = pd.concat([secinfo, data], axis='columns')
    df = df.rename_axis('YYMM').reset_index()
    str_cols = df.select_dtypes(exclude='float').columns
    df.loc[:, str_cols] = df.loc[:, str_cols].fillna('')
    df = df.reindex(cols, axis='columns')
    df['_ts'] = datetime.datetime.now()

    try:
        n = con.get_client().insert_dataframe(f'INSERT INTO {table_name} VALUES', df)
        if n == 0:
            logger.error(f'0 rows written: {table_name}')            
    except Exception as e:
        logger.error(f'{e}')

def write_csv(path, term_acct, sec_cd, data, sector):
    target_path = path + '/' + TermAcctDir[term_acct.name].value 
    secinfo = pd.DataFrame(index=data.index, columns=['SEC_CD', 'MKT_TYP'])
    secinfo.loc[:, 'SEC_CD'] = sec_cd
    secinfo.loc[:, 'MKT_TYP'] = sector.loc[sec_cd, 'MKT_TYP']
    df = pd.concat([secinfo, data], axis='columns')
    # FIXME is it necessary to remove index name?
    # df.index.rename('', inplace=True)
    s = df.to_csv(None, quoting=csv.QUOTE_NONNUMERIC, na_rep='NA').replace('"NA"', 'NA')
    logger.info(f'path - {target_path}/{sec_cd}.csv')
    logger.debug(f'data - \n{s}')
    if not os.path.exists(target_path):
        os.makedirs(target_path)
    with open(target_path +  '/' + sec_cd + '.csv1', 'w') as f:
        f.write(s)

def generate_sec_fin_by_sec(conf, to_csv=True, to_clickhouse=True):
    con_ = ClickHouseConnection(
        host=conf['ClickHouse']['host'],
        user=conf['ClickHouse']['user'],
        password=conf['ClickHouse']['password'],
        database='financial', )

    db = MariaDBReader(conf['MariaDB']['connection_uri'] + '/' + conf['MariaDB']['db_name'])
    path = conf['Path']['by_sector_sec_fin_path']

    import data
    orginal_field_path = data.__path__._path[0] + '/' + 'original_fields.csv'
    org_items = pd.read_csv(orginal_field_path, dtype='str', index_col=0)

    items = db.read_financial(table_name='TZ_ITEM').astype('str')
    org_items.columns = items.columns[:7]
    org_items = org_items[(org_items.FINACC_TYP == '0') | (org_items.FINACC_TYP == '1')]
    org_items = org_items[(org_items.ITEM_TYP=='A') | (org_items.ITEM_TYP=='M')]
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
    item_unit = items[['ITEM_NM_ENG', 'UNT_TYP']].drop_duplicates().set_index('ITEM_NM_ENG')
    item_unit = item_unit.loc[item_unit.UNT_TYP == '천원']

    sector = db.read_financial(table_name='TC_SECTOR', cond="WHERE SEC_TYP='W'").fillna('').astype('str')
    sector = sector.set_index('SEC_CD')

    for sec_cd in sector.index[:].sort_values():
        try:
            logger.info(f'reading sec_cd: {sec_cd}')
            findata = db.read_financial(table_name='TF_SEC_FINDATA', cond=f"WHERE SEC_CD = '{sec_cd}'")
            if len(findata) == 0:
                logger.info(f'emtpy sec_cd: {sec_cd}')
                continue
            logger.info(f'read complete sec_cd: {sec_cd}')
            findata.loc[:, findata.columns[findata.columns!='VAL']] = findata.loc[:, findata.columns[findata.columns!='VAL']].astype('str')
            findata = pd.pivot_table(findata, values='VAL', index=['YYMM', 'TERM_TYP'] , columns= ['ITEM_CD', 'ITEM_TYP'])
            findata.columns = findata.columns.to_flat_index()
            colname_map = dict(zip(list(zip(org_items.ITEM_CD, org_items.ITEM_TYP)), org_items.ITEM_NM_ENG))
            cols = findata.columns.intersection(list(zip(org_items.ITEM_CD, org_items.ITEM_TYP)))
            findata = findata.loc[pd.IndexSlice[:,:], cols].rename(columns=colname_map).reindex(list(org_items.ITEM_NM_ENG), axis='columns')
        
            for term_acct in TermAcctMethod:
                if term_acct.value in findata.index.get_level_values(1):
                    findata_ = findata.loc[:,term_acct.value,:].copy()
                else:
                    continue
                cols1000 = item_unit.index.intersection(findata_.columns)
                findata_.loc[:, cols1000] = findata_.loc[:, cols1000] * 1000
                if to_csv:
                    write_csv(path, term_acct, sec_cd, findata_, sector)
                if to_clickhouse:
                    write_clickhouse(con_, 'secfin_bysec', term_acct, sec_cd, findata_, sector)
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

    generate_sec_fin_by_sec(conf)
