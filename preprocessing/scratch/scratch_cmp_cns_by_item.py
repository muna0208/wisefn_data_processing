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


# fininfo = db.read_financial(table_name='TF_CMP_FINDATA_INFO', cond=f"WHERE CMP_CD = '000020'")
# fininfo = fininfo.fillna('').astype(str).set_index(['YYMM', 'TERM_TYP']).drop('CMP_CD', axis='columns')
path = conf['Path']['by_item_cmp_cns_path']
dates = sorted(os.listdir(path))

from preprocessing.defines import CNSITEMNAME_NEED_2            
for itm in  np.array(CNSITEMNAME_NEED_2)[np.random.choice(len(CNSITEMNAME_NEED_2), size=5, replace=False)]:
    for dt in np.array(dates)[np.random.choice(len(dates), size=50, replace=False)]:
        if not os.path.exists(path + '/' + dt + '/' + TermAcctDir[TermAcctMethod.SEP_CUM.name].value + "/" + f'{itm}' + ".csv"):
            continue
        org_ = pd.read_csv(path + '/' + dt + '/' + TermAcctDir[TermAcctMethod.SEP_CUM.name].value + "/" + f'{itm}' + ".csv", 
                index_col=0, 
                dtype='str')
                
        new_ = pd.read_csv(path +  '/' + dt + '/' +  TermAcctDir[TermAcctMethod.SEP_CUM.name].value + "/" + f'{itm}' + ".csv1", 
                index_col=0, 
                dtype='str')

        common = org_.index.intersection(new_.index)

        org = org_.loc[common, org_.columns.intersection(new_.columns)] 
        new = new_.loc[common, org_.columns.intersection(new_.columns)] 

        for r in org.index: 
            for c in org.columns[30:]:
                if isinstance(org.loc[r, c], str):
                    if float(org.loc[r, c]) != float(new.loc[r,c]):
                        if (new.loc[r,c] !=  new.loc[r,c]):
                            print(r, c, org.loc[r, c], new.loc[r,c])

        # clickhouse data test
        from preprocessing.clickhouse_financial.clickhouse_helper import ClickHouseConnection
        con_ = ClickHouseConnection(
            host=conf['ClickHouse']['host'],
            user=conf['ClickHouse']['user'],
            password=conf['ClickHouse']['password'],
            database='financial', )
        table_name = 'cmpcns_byitem'

        click = con_.get_client().query_dataframe(f"SELECT * FROM {table_name} WHERE ITEM_NM = '{itm}' AND CNS_DT= '{dt}' AND TERM_TYP='{TermAcctMethod.SEP_CUM.value}'")

        click = click[click.TERM_TYP == TermAcctMethod.SEP_CUM.value]
        click = click.drop_duplicates(subset=['CNS_DT', 'YYMM'],keep='last')
        click.YYMM = click.YYMM.astype(int)
        click = click.set_index('YYMM')
        common = click.index.intersection(org_.index)
        click = click.loc[common, org_.columns.intersection(click.columns)] 
        new = click.loc[org.index, org.columns.intersection(click.columns)]

        org = org[click.columns.intersection(org.columns)]
        for r in org.index: 
            for c in org.columns[22:]:
                if isinstance(org.loc[r, c], str):
                    if float(org.loc[r, c]) != float(click.loc[r,c]):
                        if (new.loc[r,c] !=  click.loc[r,c]):
                            if abs(new.loc[r,c] - click.loc[r,c]) < 0.1:
                                print(r, c, org.loc[r, c], click.loc[r,c])

        print(itm, dt, org.shape, new.shape)                    

