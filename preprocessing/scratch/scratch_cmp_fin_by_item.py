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


# db = MariaDBReader(conf['MariaDB']['connection_uri'] + '/' + conf['MariaDB']['db_name'])
# findata = db.read_financial(table_name='TF_CMP_FINDATA', cond=f"WHERE CMP_CD = '000020'")
# findata.loc[:, findata.columns[findata.columns!='VAL']] = findata.loc[:, findata.columns[findata.columns!='VAL']].astype('str')
# findata = pd.pivot_table(findata, values='VAL', index=['YYMM', 'TERM_TYP'] , columns= ['ITEM_CD', 'ITEM_TYP'])
# findata.columns = findata.columns.to_flat_index()
# item = db.read_financial(table_name='TZ_ITEM')
# item[(item.ITEM_CD == '111100') & (item.ITEM_TYP == 'M')].iloc[:,:7]

# item_unit = item[['ITEM_CD', 'ITEM_TYP', 'UNT_TYP']]\
#         .drop_duplicates().set_index(['ITEM_CD', 'ITEM_TYP'])
# item_unit = item_unit.loc[item_unit.UNT_TYP == '천원']
# len(item_unit.loc[('111100', 'M'), :])
# pd.IndexSlice['111100', 'M'] in item_unit.index

from preprocessing.clickhouse_financial.clickhouse_helper import ClickHouseConnection
con_ = ClickHouseConnection(
    host=conf['ClickHouse']['host'],
    user=conf['ClickHouse']['user'],
    password=conf['ClickHouse']['password'],
    database='financial', )

path = conf['Path']['by_item_cmp_fin_path']
item_list = os.listdir(path + '/' + TermAcctDir[TermAcctMethod.SEP_CUM.name].value)

item_list = [i.split('.')[0] for i in item_list]


db = MariaDBReader(conf['MariaDB']['connection_uri'] + '/' + conf['MariaDB']['db_name'])
company = db.read_financial(table_name='TC_COMPANY', cond="WHERE LIST_YN=1 AND MASTER_CHK IS NOT NULL").astype('str')
company = company.set_index('CMP_CD')
cmps = company.index[:].sort_values()[:]

cmps = ['A'+ c for c in cmps]

for itm in item_list:
    if not os.path.exists(path + '/' + TermAcctDir[TermAcctMethod.SEP_CUM.name].value + "/" + f'{itm}' + ".csv"):
        continue
    org = pd.read_csv(path + '/' + TermAcctDir[TermAcctMethod.SEP_CUM.name].value + "/" + f'{itm}' + ".csv", 
            index_col=0, 
            dtype='str')
            
    new = pd.read_csv(path + '/' + TermAcctDir[TermAcctMethod.SEP_CUM.name].value + "/" + f'{itm}' + ".csv1", 
            index_col=0, 
            dtype='str')

    common = org.index.intersection(new.index)

    org = org.loc[common, org.columns.intersection(new.columns).intersection(cmps)] 
    new = new.loc[common, org.columns.intersection(new.columns).intersection(cmps)] 

    # for r in org.index: 
    #     for c in org.columns[:]:
    #         if isinstance(org.loc[r, c], str):
    #             if float(org.loc[r, c]) != float(new.loc[r,c]):
    #                 print(r, c, org.loc[r, c], new.loc[r,c])


    # need to debug

    table_name = 'cmpfin_byitem'

    click = con_.get_client().query_dataframe(f"SELECT * FROM {table_name} limit 1")
    click = con_.get_client().query_dataframe(f"SELECT * FROM {table_name} WHERE ITEM_NM = '{itm}' AND TERM_TYP='{TermAcctMethod.SEP_CUM.value}'")
    click = click.drop_duplicates(subset=['ITEM_NM', 'YYMM'],keep='last')

    click.YYMM  = click.YYMM.astype(int)
    new = click.set_index('YYMM').loc[org.index, org.columns.intersection(click.columns)]
    org = org[new.columns.intersection(org.columns)]
    for r in org.index: 
        for c in org.columns:
            if isinstance(org.loc[r, c], str):
                if float(org.loc[r, c]) != float(new.loc[r,c]):
                    if (new.loc[r,c] !=  new.loc[r,c]):
                        print(itm, r, c, org.loc[r, c], new.loc[r,c])

    print(itm, org.shape, new.shape) 

# 08TotalEquity 201706 A256630 13375601400 nan
# 08TotalEquity 201706 A256840 13263613470 nan
# 08TotalEquity 201706 A258790 6079046600 nan


# TermAcctMethod.SEP_CUM.value
# from preprocessing.defines import fin_need2
# item_cd='115000'
# item_name='08TotalEquity'

# company = db.read_financial(table_name='TC_COMPANY', cond="WHERE LIST_YN=1 AND MASTER_CHK IS NOT NULL").astype('str')
# company = company.set_index('CMP_CD')

# findata = db.read_financial(table_name='TF_CMP_FINDATA', cond=f"WHERE TERM_TYP='1' and CMP_CD='256630' and ITEM_CD='{item_cd}' AND ITEM_TYP='M'")
# findata = findata.loc[:, findata.columns[findata.columns != 'ITEM_TYP']]
# findata.loc[:, findata.columns[findata.columns!='VAL']] = findata.loc[:, findata.columns[findata.columns!='VAL']].astype('str')
# findata = pd.pivot_table(findata, values='VAL', index=['YYMM', 'TERM_TYP'] , columns= ['CMP_CD'])
# findata.columns = ['A' + c for c in findata.columns]


# con_ = ClickHouseConnection(
#     host=conf['ClickHouse']['host'],
#     user=conf['ClickHouse']['user'],
#     password=conf['ClickHouse']['password'],
#     database='wisefn', )

# d = con_.get_client().query_dataframe("select * from TF_CMP_FINDATA where CMP_CD='256630' and ITEM_CD='115000'")

# d[(d.TERM_TYP=='1') &(d.YYMM=='201706')]

