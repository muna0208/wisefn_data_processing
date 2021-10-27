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

path = conf['Path']['by_company_cmp_fin_all_path'] 

db = MariaDBReader(conf['MariaDB']['connection_uri'] + '/' + conf['MariaDB']['db_name'])
company = db.read_financial(table_name='TC_COMPANY', cond="WHERE LIST_YN=1 AND MASTER_CHK IS NOT NULL").astype('str')
company = company.set_index('CMP_CD')
cmps = company.index[:].sort_values()[:]

for cmp_cd in cmps[np.random.choice(len(cmps), size=100, replace=False)]:
    import time
    
    start = time.time()
    acct = TermAcctMethod.CON_CUM
    org_ = pd.read_csv(path + '/' + TermAcctDir[acct.name].value + "/" + "A" + f'{cmp_cd}' + ".csv", 
            index_col=0, 
            dtype='str')
    print('reading csv took :', time.time() - start)

    new_ = pd.read_csv(path + '/' + TermAcctDir[acct.name].value + "/" + "A" + f'{cmp_cd}' + ".csv1", 
            index_col=0, 
            dtype='str')
    from preprocessing.clickhouse_financial.shared_columns import column_map
    new_ = new_.rename(columns=column_map)

    common = org_.index.intersection(new_.index)

    org = org_.loc[common, org_.columns.intersection(new_.columns)] 
    new = new_.loc[common, org_.columns.intersection(new_.columns)] 

    for r in org.index: 
        for c in org.columns[22:]:
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
    table_name = 'cmpfin_bycmp_all'

    import time
    start = time.time()
    click = con_.get_client().query_dataframe(f"SELECT * FROM {table_name} WHERE CMP_CD = '{cmp_cd}' AND TERM_TYP='{acct.value}'")
    print('reading clickhouse took :', time.time() - start, click.shape)
    click = click.drop_duplicates(subset=['CMP_CD', 'YYMM'],keep='last')

    from preprocessing.clickhouse_financial.shared_columns import column_map
    click = click.rename(columns=column_map)
    click.YYMM  = click.YYMM.astype(int)

    # org = org_.loc[common, org_.columns.intersection(click.columns)] 
    # new = click.loc[common, org_.columns.intersection(click.columns)] 

    new = click.set_index('YYMM').loc[org_.index, org_.columns.intersection(click.columns)]
    org = org_[new.columns.intersection(org_.columns)]
    for r in org.index: 
        for c in org.columns[22:]:
            if isinstance(org.loc[r, c], str):
                if float(org.loc[r, c]) != float(new.loc[r,c]):
                    if (new.loc[r,c] !=  new.loc[r,c]):
                        print(r, c, org.loc[r, c], new.loc[r,c])
    print(cmp_cd, org.shape, new.shape)


