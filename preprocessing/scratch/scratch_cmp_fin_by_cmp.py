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

# ORIGINAL_FIELDS = pd.read_csv(conf['Path']['base_path'] + '/' + 'original_fields.csv', dtype='str', index_col=0)
# db = MariaDBReader(conf['MariaDB']['connection_uri'] + '/' + conf['MariaDB']['db_name'])
# findata = db.read_financial(table_name='TF_CMP_FINDATA', cond=f"WHERE CMP_CD = '000020'")
# findata.loc[:, findata.columns[findata.columns!='VAL']] = findata.loc[:, findata.columns[findata.columns!='VAL']].astype('str')
# findata = pd.pivot_table(findata, values='VAL', index=['YYMM', 'TERM_TYP'] , columns= ['ITEM_CD', 'ITEM_TYP'])
# findata.columns = findata.columns.to_flat_index()
# fininfo = db.read_financial(table_name='TF_CMP_FINDATA_INFO', cond=f"WHERE CMP_CD = '000020'")
# fininfo = fininfo.fillna('').astype(str).set_index(['YYMM', 'TERM_TYP']).drop('CMP_CD', axis='columns')


db = MariaDBReader(conf['MariaDB']['connection_uri'] + '/' + conf['MariaDB']['db_name'])
company = db.read_financial(table_name='TC_COMPANY', cond="WHERE LIST_YN=1 AND MASTER_CHK IS NOT NULL").astype('str')
company = company.set_index('CMP_CD')
cmps = company.index[:].sort_values()[:]

for cmp in cmps[np.random.choice(len(cmps), size=100, replace=False)]:
    org_ = pd.read_csv(filepath + TermAcctDir[TermAcctMethod.CON_CUM.name].value + "/" + "A" + f'{cmp}' + ".csv", 
            index_col=0, 
            dtype='str')
    new_ = pd.read_csv(filepath + TermAcctDir[TermAcctMethod.CON_CUM.name].value + "/" + "A" + f'{cmp}' + ".csv1", 
            index_col=0, 
            dtype='str')

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
    table_name = 'cmpfin_bycmp'
    click = con_.get_client().query_dataframe(f"SELECT * FROM {table_name} WHERE CMP_CD = '{cmp}' AND TERM_TYP='{TermAcctMethod.CON_CUM.value}'")
    click = click.drop_duplicates(subset=['CMP_CD', 'YYMM'],keep='last')

    click.YYMM  = click.YYMM.astype(int)
    new = click.set_index('YYMM').loc[org.index, org.columns.intersection(click.columns)]
    org = org[new.columns.intersection(org.columns)]
    for r in org.index: 
        for c in org.columns[22:]:
            if isinstance(org.loc[r, c], str):
                if float(org.loc[r, c]) != float(new.loc[r,c]):
                    if (new.loc[r,c] !=  new.loc[r,c]):
                        print(r, c, org.loc[r, c], new.loc[r,c])
    print(cmp, org.shape, new.shape)

# new.loc[202006, 'Common_Shares_FY_End']

# ORIGINAL_FIELDS[ORIGINAL_FIELDS['V6']=='Shares_All_FY_End']

# findata[(findata['ITEM_CD'] == '702110') & 
#  (findata['ITEM_TYP'] == 'M') & (findata['TERM_TYP'] == '1')].YYMM.sort_values().drop_duplicates()

# ORIGINAL_FIELDS[ORIGINAL_FIELDS['V6']=='Short_Term_Trading_Financial_Assets']

# findata[(findata['ITEM_CD'] == '113100') & 
#  (findata['ITEM_TYP'] == 'M') & (findata['TERM_TYP'] == '1')]

# c = org.loc[:, 'Short_Term_Trading_Financial_Assets']

# d = findata[(findata['ITEM_CD'] == '113100') & 
#  (findata['ITEM_TYP'] == 'M') & (findata['TERM_TYP'] == '1')][['YYMM', 'VAL']].set_index('YYMM')

# pd.concat([c.astype('float'), d.astype('float')], axis='columns')

# df = pd.DataFrame()
# for y in ['2015', '2016', '2017', '2018', '2019', '2020']:
#     f = pd.read_csv(
#         f'/home/mining/systemtrading/WiseFN_Down_DATA/02ALL_allitem/{y}' + '/' + 'TF_CMP_FINDATA.TXT',
#         names=("CMP_CD", "TERM_TYP", "YYMM", "ITEM_TYP", "ITEM_CD", "VAL"),
#         sep='|',
#         dtype={"CMP_CD":'str', "TERM_TYP":'str', "YYMM":'str', "ITEM_TYP":'str', "ITEM_CD":'str', "VAL":'float64'})
#     df = pd.concat([df, f]).drop_duplicates(subset=['YYMM', 'CMP_CD', 'ITEM_CD', 'ITEM_TYP', 'TERM_TYP'], keep='last')
#     print(y)

# e = df[(df.CMP_CD=='000020') & (df.ITEM_CD=='113100') &(df.TERM_TYP=='1')]
# g = findata[(findata.TERM_TYP == '1') & (findata.ITEM_CD=='113100')]

# pd.concat([c.astype('float'), d.astype('float')], axis='columns')

# e.loc[e.YYMM=='201909',:]
# g.loc[g.YYMM=='201909',:]
# org.loc[201909,]
# d.loc['201909',:]

# #200003 Short_Term_Trading_Financial_Assets 52459632000.0 650605000.0
# org.loc[200003,'Short_Term_Trading_Financial_Assets']
# org.loc[200003,'Current_Liabilities']

# new.loc[200003,'Short_Term_Trading_Financial_Assets']
# new.loc[200003,'Current_Liabilities']
# e.loc[e.YYMM=='200003',:]
# g.loc[g.YYMM=='200003',:]

# findata[(findata.TERM_TYP == '1') & (findata.ITEM_CD=='702110')]
# org.loc[202009,'Shares_All_FY_End']
# new.loc[202009,'Shares_All_FY_End']

# org.loc[200006,'Common_Shares_FY_End']
# new.loc[200006,'Common_Shares_FY_End']

# e = df[(df.CMP_CD=='000020') & (df.ITEM_CD=='702110') &(df.TERM_TYP=='1')]
# g = findata[(findata.TERM_TYP == '1') & (findata.ITEM_CD=='702110')]

# e.loc[e.YYMM=='202009',:]
# g.loc[g.YYMM=='202009',:]
