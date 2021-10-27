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
path = conf['Path']['by_company_cmp_fin_path']

db = MariaDBReader(conf['MariaDB']['connection_uri'] + '/' + conf['MariaDB']['db_name'])
company = db.read_financial(table_name='TC_COMPANY', cond="WHERE LIST_YN=1 AND MASTER_CHK IS NOT NULL").astype('str')
company = company.set_index('CMP_CD')
cmps = company.index[:].sort_values()[:]

for cmp in cmps[:300]: #np.random.choice(len(cmps), size=300, replace=False):

    if not os.path.exists(path + '/' + '99QUARTERLY' + "/" + f'A{cmp}' + ".csv"):
        continue

    org_ = pd.read_csv(path + '/' + '99QUARTERLY' + "/" + f'A{cmp}' + ".csv", 
            index_col=0, 
            dtype='str')

    # new_ = pd.read_csv(path +  '/' +  '99QUARTERLY' + "/" + f'A{cmp}' + ".csv1", 
    #         index_col=0, 
    #         dtype='str')

    # common = org_.index.intersection(new_.index)

    # org = org_.loc[common, org_.columns.intersection(new_.columns)] 
    # new = new_.loc[common, org_.columns.intersection(new_.columns)] 

    # for r in org.index: 
    #     for c in org.columns[22:]:
    #         if isinstance(org.loc[r, c], str):
    #             if float(org.loc[r, c]) != float(new.loc[r,c]):
    #                 if (new.loc[r,c] !=  new.loc[r,c]):
    #                     print(r, c, org.loc[r, c], new.loc[r,c])


    from preprocessing.clickhouse_financial.clickhouse_helper import ClickHouseConnection
    con_ = ClickHouseConnection(
        host=conf['ClickHouse']['host'],
        user=conf['ClickHouse']['user'],
        password=conf['ClickHouse']['password'],
        database='financial', )
    table_name = 'quarterly_cmpfin'
    click = con_.get_client().query_dataframe(f"SELECT * FROM {table_name} WHERE CMP_CD = '{cmp}' AND TERM_TYP='QUARTERLY'")
    click = click.sort_values(['CMP_CD', 'YYMM','_ts']).drop_duplicates(subset=['CMP_CD', 'YYMM'],keep='last')


    click.YYMM = click.YYMM.apply(float)
    org = org_
    new = click.set_index('YYMM')
    new = new.loc[org.index.intersection(new.index), org.columns.intersection(new.columns)]
    org = org[new.columns.intersection(org.columns)]

    for r in org.index: 
        for c in org.columns[22:]:
            if isinstance(org.loc[r, c], str):
                if float(org.loc[r, c]) != float(new.loc[r,c]):
                    if (new.loc[r,c] !=  new.loc[r,c]):
                        print(r, c, org.loc[r, c], new.loc[r,c])

    print(cmp, org.shape, new.shape)

# 2018.4 Sales_Growth_YoY 0.12764 nan
# 2018.4 Operating_Profit_Growth_YoY 24.89836 nan
# 2018.4 Net_Profit_Growth_YoY -0.91325 nan
# 000020 (108, 65) (104, 65)
