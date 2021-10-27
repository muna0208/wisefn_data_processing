import csv
import datetime
import logging
import pandas as pd
import numpy as np
import os
import sys

from preprocessing.config import config
from preprocessing.paths import FIN_quarterly_path, FIN_yearly_path, filepath, by_ITEM_path, \
    SEC_filepath, by_SEC_ITEM_path, INPUTPATH, ALLINPUTPATH, MAINPATH
from preprocessing.defines import TermAcctMethod, TermAcctDir, FinAcctType, ITEMNAME_NEED_2, ITEMCODE_NEED_2

from wisefn_mariadb.db_connection import MariaDBConnection
from wisefn_mariadb.wisefn2mariadb_schema import WiseFNSchema
from wisefn_mariadb.db_reader import MariaDBReader
from wisefn_mariadb.config import get_config
from logging.handlers import RotatingFileHandler

config_file = '/home/mining/PycharmProjects/wisefn_data_processing/db3.config'
conf = get_config(config_file)

ORIGINAL_FIELDS = pd.read_csv(conf['Path']['base_path'] + '/' + 'original_fields.csv', dtype='str', index_col=0)


db = MariaDBReader(conf['MariaDB']['connection_uri'] + '/' + conf['MariaDB']['db_name'])
findata = db.read_financial(table_name='TF_SEC_FINDATA', cond=f"WHERE SEC_CD = 'WI000'")
findata.loc[:, findata.columns[findata.columns!='VAL']] = findata.loc[:, findata.columns[findata.columns!='VAL']].astype('str')
findata = pd.pivot_table(findata, values='VAL', index=['YYMM', 'TERM_TYP'] , columns= ['ITEM_CD', 'ITEM_TYP'])
findata.columns = findata.columns.to_flat_index()


from preprocessing.defines import FINITEMNAME_NEED_2

# fininfo = db.read_financial(table_name='TF_CMP_FINDATA_INFO', cond=f"WHERE CMP_CD = '000020'")
# fininfo = fininfo.fillna('').astype(str).set_index(['YYMM', 'TERM_TYP']).drop('CMP_CD', axis='columns')
path = conf['Path']['by_item_sec_fin_path']


for itm in FINITEMNAME_NEED_2:
    if not os.path.exists(path + '/' + TermAcctDir[TermAcctMethod.SEP_CUM.name].value + "/" + f'{itm}' + ".csv"):
        continue
    org_ = pd.read_csv(path + '/' + TermAcctDir[TermAcctMethod.SEP_CUM.name].value + "/" + f'{itm}' + ".csv", 
            index_col=0, 
            dtype='str')
    if not os.path.exists(path + '/' + TermAcctDir[TermAcctMethod.SEP_CUM.name].value + "/" + f'{itm}' + ".csv1"):
        continue
    new_ = pd.read_csv(path + '/' + TermAcctDir[TermAcctMethod.SEP_CUM.name].value + "/" + f'{itm}' + ".csv1", 
            index_col=0, 
            dtype='str')

    common = org_.index.intersection(new_.index)

    org = org_.loc[common, org_.columns.intersection(new_.columns)] 
    new = new_.loc[common, org_.columns.intersection(new_.columns)] 

    for r in org.index: 
        for c in org.columns:
            if isinstance(org.loc[r, c], str):
                if float(org.loc[r, c]) != float(new.loc[r,c]):
                    if (new.loc[r,c] !=  new.loc[r,c]):
                        print(r, c, org.loc[r, c], new.loc[r,c])


    from preprocessing.clickhouse_financial.clickhouse_helper import ClickHouseConnection
    con_ = ClickHouseConnection(
        host=conf['ClickHouse']['host'],
        user=conf['ClickHouse']['user'],
        password=conf['ClickHouse']['password'],
        database='financial', )
    table_name = 'secfin_byitem'
    click = con_.get_client().query_dataframe(f"SELECT * FROM {table_name} WHERE ITEM_NM = '{itm}' AND TERM_TYP='{TermAcctMethod.SEP_CUM.value}'")
    click = click.drop_duplicates(subset=['YYMM'],keep='last')

    # new.columns.difference(click.columns)
    # new.dropna(how='all', axis='columns')

    click.YYMM  = click.YYMM.astype(int)
    new = click.set_index('YYMM').loc[org.index, org.columns.intersection(click.columns)]
    org = org[new.columns.intersection(org.columns)]
    for r in org.index: 
        for c in org.columns:
            if isinstance(org.loc[r, c], str):
                if float(org.loc[r, c]) != float(new.loc[r,c]):
                    if (new.loc[r,c] !=  new.loc[r,c]):
                        print(r, c, org.loc[r, c], new.loc[r,c])

    print(itm, org.shape, new.shape)

    