from wisefn_clickhouse.clickhouse_connection import ClickHouseConnection
from wisefn_clickhouse.wisefn2clickhouse_schema import ClickHouseSchema
import pandas as pd
import logging
import os
import gc
from wisefn_clickhouse.config import get_config
import sys
from logging.handlers import RotatingFileHandler
from wisefn_clickhouse.clickhouse_reader import ClickHouseReader

from preprocessing.config import config
from preprocessing.defines import TermAcctMethod, TermAcctDir, FinAcctType

from wisefn_mariadb.db_connection import MariaDBConnection
from wisefn_mariadb.wisefn2mariadb_schema import WiseFNSchema
from wisefn_mariadb.db_reader import MariaDBReader
from wisefn_mariadb.config import get_config
from logging.handlers import RotatingFileHandler
import datetime
import numpy as np

config_file = '/home/mining/PycharmProjects/wisefn_data_processing/db3.config'
if len(sys.argv) > 1:
    config_file = sys.argv[1]
conf = get_config(config_file)

db = ClickHouseReader(database='wisefn')
d = db.read_financial(table_name='TF_CMP_FINDATA', cond=" WHERE CMP_CD = '000020' AND DNDATE >= '20210225'")

db = ClickHouseReader(database='financial')
e = db.read_financial(table_name='financial.cmpfin_bycmp', cond=" WHERE CMP_CD = '000020'")

logger = logging.getLogger('by_company_cmp_cns_path')
con = ClickHouseConnection(
    host=conf['ClickHouse']['host'],
    user=conf['ClickHouse']['user'],
    password=conf['ClickHouse']['password'],
    database='financial', )

db = MariaDBReader(conf['MariaDB']['connection_uri'] + '/' + conf['MariaDB']['db_name'])
path = conf['Path']['by_company_cmp_fin_path']

if config['use_original_fields']:
    ORIGINAL_FIELDS = pd.read_csv(conf['Path']['base_path'] + '/' + 'original_fields.csv', dtype='str', index_col=0)

items = db.read_financial(table_name='TZ_ITEM').astype('str')
ORIGINAL_FIELDS.columns = items.columns[:7]
ORIGINAL_FIELDS = ORIGINAL_FIELDS[(ORIGINAL_FIELDS.FINACC_TYP == '0') | (ORIGINAL_FIELDS.FINACC_TYP == '1')]
ORIGINAL_FIELDS = ORIGINAL_FIELDS[(ORIGINAL_FIELDS.ITEM_TYP=='A') | (ORIGINAL_FIELDS.ITEM_TYP=='M')]
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

company = db.read_financial(table_name='TC_COMPANY', cond="WHERE LIST_YN=1 AND MASTER_CHK IS NOT NULL").astype('str')
company = company.set_index('CMP_CD')

for cmp_cd in ['000020']: #[company.index[:].sort_values()[:]:]
    try:
        logger.info(f'reading cmp_cd: {cmp_cd}')
        findata = db.read_financial(table_name='TF_CMP_FINDATA', cond=f"WHERE CMP_CD = '{cmp_cd}'")
        if len(findata) == 0:
            logger.info(f'empty cmp_cd: {cmp_cd}')
            continue
        logger.info(f'read complete cmp_cd: {cmp_cd} size {len(findata)}')

        findata.loc[:, findata.columns[findata.columns!='VAL']] = findata.loc[:, findata.columns[findata.columns!='VAL']].astype('str')
        findata = pd.pivot_table(findata, values='VAL', index=['YYMM', 'TERM_TYP'] , columns= ['ITEM_CD', 'ITEM_TYP'])
        findata.columns = findata.columns.to_flat_index()
        colname_map = dict(zip(list(zip(ORIGINAL_FIELDS.ITEM_CD, ORIGINAL_FIELDS.ITEM_TYP)), ORIGINAL_FIELDS.ITEM_NM_ENG))
        findata_cols = findata.columns.intersection(list(zip(ORIGINAL_FIELDS.ITEM_CD, ORIGINAL_FIELDS.ITEM_TYP)))
        findata = findata.loc[pd.IndexSlice[:,:], findata_cols].rename(columns=colname_map).reindex(list(ORIGINAL_FIELDS.ITEM_NM_ENG), axis='columns')
        
        fininfo = db.read_financial(table_name='TF_CMP_FINDATA_INFO', cond=f"WHERE CMP_CD = '{cmp_cd}'")
        float_cols = fininfo.select_dtypes(include='float').columns
        if len(float_cols) > 0:
            # fininfo.loc[:, float_cols] = fininfo.loc[:, float_cols].astype(pd.Int64Dtype())
            fininfo.loc[:, float_cols] = fininfo.loc[:, float_cols].applymap(lambda x: str(int(x)) if x==x else '').astype(str)
        fininfo = fininfo.fillna('').astype(str).set_index(['YYMM', 'TERM_TYP']).drop('CMP_CD', axis='columns')

        pub_dt = fininfo['FST_PUB_DT'].apply(lambda x: datetime.datetime.strptime(x, '%Y%m%d' ) if len(x) > 1 else np.datetime64('NaT'))
        end_dt = fininfo['END_DT'].apply(lambda x: datetime.datetime.strptime(x, '%Y%m%d' ) if len(x) > 1 else np.datetime64('NaT'))
        fininfo.loc[pub_dt - end_dt  > datetime.timedelta(days=360), 'FST_PUB_DT'] = np.datetime64('NaT')

        finprd = db.read_financial(table_name='TF_CMP_FINPRD', cond=f"WHERE CMP_CD = '{cmp_cd}'")
        cols = finprd.select_dtypes(include='float').columns
        if len(cols) > 0:
            finprd.loc[:, cols] = finprd.loc[:, cols].applymap(lambda x: str(int(x)) if x==x else '').astype(str)
        finprd = finprd.fillna('').astype(str).set_index('YYMM')
        finprd = finprd.drop('CMP_CD', axis='columns')

        for term_acct in TermAcctMethod:
            if term_acct.value not in findata.index.get_level_values(1):
                continue
            findata_ = findata.loc[:,term_acct.value,:].copy()
            cols1000 = item_unit.index.intersection(findata_.columns)
            findata_.loc[:, cols1000] = findata_.loc[:, cols1000] * 1000
            fininfo_ = fininfo.loc[:,[term_acct.value],:].reset_index(level=[1])

    except Exception as e:
        logger.error(f'{e}')