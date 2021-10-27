import csv
import datetime
import logging
import pandas as pd
import os

from pandas.core.frame import DataFrame
os.environ['NUMEXPR_MAX_THREADS'] = '8'
import numpy as np
import sys
import multiprocessing as mp

from preprocessing.config import config
from preprocessing.defines import TermAcctMethod, TermAcctDir, FinAcctType

from wisefn_mariadb.db_connection import MariaDBConnection
from wisefn_mariadb.wisefn2mariadb_schema import WiseFNSchema
from wisefn_mariadb.db_reader import MariaDBReader
from wisefn_mariadb.config import get_config
from logging.handlers import RotatingFileHandler
from preprocessing.clickhouse_financial.clickhouse_helper import ClickHouseConnection


logger = logging.getLogger('by_company_cmp_fin')


def write_clickhouse(con, table_name, term_acct, cmp_cd, findata, fininfo, finprd, company):
    cols = con.get_columns(table_name)
    findata = findata.dropna(how='all', axis='columns')
    df = pd.concat([finprd, fininfo, findata], axis='columns')
    cmpinfo = pd.DataFrame(index=df.index, columns=['CMP_CD', 'MKT_TYP','GICS_CD', 'WI26_CD'])
    df = pd.concat([cmpinfo, df], axis='columns')
    
    df.loc[:,'TERM_TYP'] = term_acct.value
    df.loc[:,'CMP_CD'] = cmp_cd
    df.loc[:,'MKT_TYP'] = company.loc[cmp_cd,'MKT_TYP']
    df.loc[:,'GICS_CD'] = company.loc[cmp_cd,'GICS_CD']
    df.loc[:,'WI26_CD'] = company.loc[cmp_cd,'WI26_CD']
    
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


def write_csv(path, term_acct, cmp_cd, findata, fininfo, finprd, company):
    findata = findata.dropna(how='all', axis='columns')
    df = pd.concat([finprd, fininfo, findata], axis='columns')
    cmpinfo = pd.DataFrame(index=df.index, columns=['CMP_CD', 'MKT_TYP','GICS_CD', 'WI26_CD'])
    df = pd.concat([cmpinfo, df], axis='columns')

    df.loc[:,'CMP_CD'] = cmp_cd
    df.loc[:,'MKT_TYP'] = company.loc[cmp_cd,'MKT_TYP']
    df.loc[:,'GICS_CD'] = company.loc[cmp_cd,'GICS_CD']
    df.loc[:,'WI26_CD'] = company.loc[cmp_cd,'WI26_CD']

    s = df.to_csv(None, quoting=csv.QUOTE_NONNUMERIC, na_rep='NA').replace('"NA"', 'NA')
    target_path = path + '/' + TermAcctDir[term_acct.name].value 
    logger.info(f"path - {target_path}/A{cmp_cd}.csv")
    logger.debug(f"data - \n{s}")
    if not os.path.exists(target_path):
        os.makedirs(target_path)
    if True:
        with open(path + '/' + TermAcctDir[term_acct.name].value +  '/' + 'A' + cmp_cd + '.csv1', 'w') as f:
            f.write(s)


def generate_cmp_fin_by_cmp_mp(conf, to_csv=True, to_clickhouse=True, cmps=[]):
    con_ = ClickHouseConnection(
        host=conf['ClickHouse']['host'],
        user=conf['ClickHouse']['user'],
        password=conf['ClickHouse']['password'],
        database='financial', )

    db = MariaDBReader(conf['MariaDB']['connection_uri'] + '/' + conf['MariaDB']['db_name'])
    path = conf['Path']['by_company_cmp_fin_path']

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

    company = db.read_financial(table_name='TC_COMPANY', cond="WHERE LIST_YN=1 AND MASTER_CHK IS NOT NULL").astype('str')
    company = company.set_index('CMP_CD')

    for cmp_cd in cmps:
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
            colname_map = dict(zip(list(zip(org_items.ITEM_CD, org_items.ITEM_TYP)), org_items.ITEM_NM_ENG))
            findata_cols = findata.columns.intersection(list(zip(org_items.ITEM_CD, org_items.ITEM_TYP)))
            findata = findata.loc[pd.IndexSlice[:,:], findata_cols].rename(columns=colname_map).reindex(list(org_items.ITEM_NM_ENG), axis='columns')
            
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
                if to_csv:
                    write_csv(path, term_acct, cmp_cd, findata_.copy(), fininfo_.copy(), finprd, company)
                if to_clickhouse:
                    write_clickhouse(con_, 'cmpfin_bycmp', term_acct, cmp_cd, findata_.copy(), fininfo_.copy(), finprd, company)

        except Exception as e:
            logger.error(f'{e}')


def generate_cmp_fin_by_cmp(conf, start_dt='200010101', end_dt='99999999', to_csv=True, to_clickhouse=True, num_procs=8):
    con_ = ClickHouseConnection(
        host=conf['ClickHouse']['host'],
        user=conf['ClickHouse']['user'],
        password=conf['ClickHouse']['password'],
        database='financial', )

    db = MariaDBReader(conf['MariaDB']['connection_uri'] + '/' + conf['MariaDB']['db_name'])

    company = db.read_financial(table_name='TC_COMPANY', cond="WHERE LIST_YN=1 AND MASTER_CHK IS NOT NULL").astype('str')
    company = company.set_index('CMP_CD')

    cmps = con_.get_client().query_dataframe(f"SELECT DISTINCT(CMP_CD) FROM wisefn.TF_CMP_FINDATA WHERE DNDATE >= '{start_dt}' AND DNDATE <= '{end_dt}'")
    cmps = company.index.intersection(list(cmps.loc[:, 'CMP_CD']))
    cmps = sorted(list(cmps))
    
    cmps_map = {}
    for n in range(num_procs):
        cmps_map[n] = []
    
    for cmp in cmps:
        m = abs(hash(cmp)) % num_procs
        cmps_map[m] += [cmp]

    proc_map = {}
    for k in cmps_map.keys():
        proc_map[k] = mp.Process(target=generate_cmp_fin_by_cmp_mp, args=(conf, to_csv, to_clickhouse, cmps_map[k]))
        proc_map[k].start()

    for kk in proc_map.keys():
        proc_map[kk].join()


if __name__ == '__main__':
    config_file = '/home/mining/systemtrading/python_projects/wisefn_data_processing/db51.config' 

    if len(sys.argv) > 1:
        config_file = sys.argv[1]

    conf = get_config(config_file)
    logging.basicConfig(level=eval(conf['Logging']['level']),
        handlers=[eval(conf['Logging']['handler'])],
        format=conf['Logging']['format'],
        )

    generate_cmp_fin_by_cmp(conf, to_csv=False, to_clickhouse=True)

    
    