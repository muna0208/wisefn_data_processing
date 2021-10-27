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
findata = db.read_financial(table_name='TT_CMP_CNS_DATA', cond=f"WHERE CMP_CD = '000660'")
findata.loc[:, findata.columns[findata.columns!='VAL']] = findata.loc[:, findata.columns[findata.columns!='VAL']].astype('str')

findata = pd.pivot_table(findata, values='VAL', index=['YYMM', 'TERM_TYP'] , columns= ['ITEM_CD', 'ITEM_TYP'])
findata.columns = findata.columns.to_flat_index()


# fininfo = db.read_financial(table_name='TF_CMP_FINDATA_INFO', cond=f"WHERE CMP_CD = '000020'")
# fininfo = fininfo.fillna('').astype(str).set_index(['YYMM', 'TERM_TYP']).drop('CMP_CD', axis='columns')
path = conf['Path']['by_company_cmp_cns_path']

dates = sorted(os.listdir(path))
company = db.read_financial(table_name='TC_COMPANY', cond="WHERE LIST_YN=1 AND MASTER_CHK IS NOT NULL").astype('str')
company = company.set_index('CMP_CD')
cmps = company.index[:].sort_values()[:]
path = conf['Path']['by_company_cmp_cns_path']

for cmp in cmps: #np.array(cmps)[np.random.choice(len(cmps), size=500, replace=False)]:
    for dt in dates:
        if not os.path.exists(path + '/' + f'{dt}'  + '/' + '99ALL_CUM' + "/" + f'A{cmp}' + ".csv"):
            continue
        org_ = pd.read_csv(path + '/' + f'{dt}'  + '/' + '99ALL_CUM' + "/" + f'A{cmp}' + ".csv", 
                index_col=0, 
                dtype='str')
        # if not os.path.exists(path + '/' + f'{dt}'  + '/' + '99ALL_CUM' + "/" + f'{cmp}' + ".csv1"):
        #     continue
        # new_ = pd.read_csv(path +  '/' + f'{dt}'  + '/' +  '99ALL_CUM' + "/" + f'{cmp}' + ".csv1", 
        #         index_col=0, 
        #         dtype='str')

        # common = org_.index.intersection(new_.index)

        # org = org_.loc[common, org_.columns.intersection(new_.columns)] 
        # new = new_.loc[common, org_.columns.intersection(new_.columns)] 

        # for r in org.index: 
        #     for c in org.columns[5:]:
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
        table_name = 'annual_cmpcns'
        click = con_.get_client().query_dataframe(f"SELECT * FROM {table_name} WHERE CMP_CD = '{cmp}' AND TERM_TYP='ALL_CUM' AND CNS_DT='{dt}'")
        click = click.drop_duplicates(subset=['CMP_CD', 'YEAR'],keep='last')
        if len(click) == 0:
            continue
        click.YEAR = click.YEAR.astype(int)
        org = org_
        
        try:
            new = click.set_index('YEAR')
            new = new.loc[org.index, org.columns.intersection(new.columns)]
            org = org[new.columns.intersection(org.columns)]
        except:
            print(cmp, dt)
            print('error', new, org)
            continue

        for r in org.index: 
            for c in org.columns[5:]:
                if isinstance(org.loc[r, c], str):
                    if float(org.loc[r, c]) != float(new.loc[r,c]):
                        if (new.loc[r,c] !=  new.loc[r,c]):
                            with open('acmpcns.txt', 'a') as f:
                                f.write(f'{cmp}, {dt}, {r}, {c}, {org.loc[r, c]}, {new.loc[r,c]}\n')
                            print(cmp, dt, r, c, org.loc[r, c], new.loc[r,c])
                
        # print(cmp, dt, org.shape, new.shape)



# 2015 Pre_tax_Profit_from_Continuing_Operations 1.24e+10 nan
# 2015 ROE_Owners 0.14714 nan
# 2015 BPS_Owners 3811.90613 nan
# 2015 EVEBITDA 5.04365 nan
# 2015 PB 1.42882 nan
# 2015 DPS_Adj_Comm_Cash 53.02617 nan
# 082920 20141002 (2, 18) (2, 18)