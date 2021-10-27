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

logger = logging.getLogger('by_company_cmp_cns')


def write_clickhouse(con, table_name, term_acct, cmp_cd, data):
    cols = con.get_columns(table_name)
    for cns_dt in data.index.get_level_values(0).unique():
        try:
            df = data.loc[[cns_dt],:,:].rename_axis(['CNS_DT', 'YYMM']).reset_index(level=[0,1])
            df['CMP_CD'] = cmp_cd
            df['TERM_TYP'] = term_acct.value
            df = df.reindex(cols, axis='columns')
            df['_ts'] = datetime.datetime.now()
            
            n = con.get_client().insert_dataframe(f'INSERT INTO {table_name} VALUES', df)
            if n == 0:
                logger.error(f'0 rows written: {table_name}')
        except Exception as e:
            logger.error(f'{e}')
            logger.error(f'cns_dt {cns_dt}, data {data}')


def write_csv(path, term_acct, cmp_cd, data):
    # data = data[(np.array([i//100 for i in data.index.get_level_values(0).astype(int)]) - np.array([i for i in data.index.get_level_values(1).astype(int)])) <= 0]
    for cns_dt in data.index.get_level_values(0).unique():
        target_path = path + "/" + cns_dt + "/" + TermAcctDir[term_acct.name].value
        if not os.path.exists(target_path):
            os.makedirs(target_path)
        df = data.loc[[cns_dt],:,:].rename_axis(['CNS_DT', '']).reset_index(level=[0]).drop('CNS_DT', axis='columns')
        df = df.sort_index()
        s = df.to_csv(None, na_rep='NA', quoting=csv.QUOTE_NONNUMERIC).replace('"NA"', 'NA')
        logger.info(f'path - {target_path +  "/" + "A" + cmp_cd}.csv')
        logger.debug(f'data - \n{s}')
        if True:
            with open(target_path + '/' + 'A' + cmp_cd + '.csv1', 'w') as f:
                f.write(s)

def generate_cmp_cns_by_cmp(conf, start_dt='00000000', end_dt='99999999', to_csv=False, to_clickhouse=True):
    con_ = ClickHouseConnection(
        host=conf['ClickHouse']['host'],
        user=conf['ClickHouse']['user'],
        password=conf['ClickHouse']['password'],
        database='financial', )

    db = MariaDBReader(conf['MariaDB']['connection_uri'] + '/' + conf['MariaDB']['db_name'])
    path = conf['Path']['by_company_cmp_cns_path']
    
    # get estimates only
    items = db.read_financial(table_name='TZ_ITEM', cond="WHERE ITEM_TYP='E'").fillna('').astype('str')
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
    
    cmps = con_.get_client().query_dataframe(f"SELECT DISTINCT(CMP_CD) FROM wisefn.TT_CMP_CNS_DATA WHERE DNDATE >= '{start_dt}' AND DNDATE <= '{end_dt}'")
    cmps = company.index.intersection(list(cmps.loc[:, 'CMP_CD']))

    estimate_master = db.read_financial(table_name='TT_EST_MASTER').astype('str')
    estimate_master = estimate_master.set_index('CMP_CD')

    for cmp_cd in sorted(cmps): 
        try:
            logger.info(f'reading cmp_cd: {cmp_cd}')
            cns_dts = con_.get_client().query_dataframe(f"SELECT DISTINCT(CNS_DT) FROM wisefn.TT_CMP_CNS_DATA WHERE DNDATE >= '{start_dt}' AND DNDATE <= '{end_dt}' AND CMP_CD='{cmp_cd}'")
            cond = '( '+'OR '.join([f"CNS_DT = '{x}' " for x in list(cns_dts.loc[:, 'CNS_DT'])]) + ')'
            cond = f"WHERE CMP_CD = '{cmp_cd}'" + ' AND ' + cond

            logger.debug(f"select condition: {cond}")
            findata = db.read_financial(table_name='TT_CMP_CNS_DATA', cond=cond)
            if len(findata) == 0:
                logger.info(f'empty cmp_cd: {cmp_cd}')
                continue
            logger.info(f'read complete cmp_cd: {cmp_cd}')

            findata.loc[:, findata.columns[findata.columns!='VAL']] = findata.loc[:, findata.columns[findata.columns!='VAL']].astype('str')
            findata = pd.pivot_table(findata, values='VAL', index=['CNS_DT', 'YYMM', 'TERM_TYP'] , columns= ['ITEM_CD', 'ITEM_TYP'])
            findata.columns = findata.columns.to_flat_index()
            colname_map = dict(zip(list(zip(items.ITEM_CD, items.ITEM_TYP)), items.ITEM_NM_ENG))
            findata = findata.rename(colname_map, axis='columns')
            for term_acct in TermAcctMethod:
                if term_acct.value not in findata.index.get_level_values(2):
                    continue
                findata_ = findata.loc[:,:,term_acct.value,:]
                findata_ = findata_.rename(colname_map, axis='columns')
                cols1000 = item_unit.index.intersection(findata_.columns)
                findata_.loc[:, cols1000] = findata_.loc[:, cols1000] * 1000

                est_m = estimate_master.loc[[cmp_cd], :]
                for idx in range(est_m.shape[0]-1):
                    if (datetime.datetime.strptime(est_m.iloc[idx].END_DT, '%Y%m%d') - 
                        datetime.datetime.strptime(est_m.iloc[idx+1].START_DT, '%Y%m%d') >
                        datetime.timedelta(days=5)):
                        logger.warning(f'start date and end date mismatch - ' 
                            f"end {datetime.datetime.strptime(est_m.iloc[idx].END_DT, '%Y%m%d')} "
                            f"next start {datetime.datetime.strptime(est_m.iloc[idx+1].START_DT, '%Y%m%d')}")
                est_m = est_m.reset_index().drop(['CMP_CD', 'END_DT'], axis='columns').set_index(['START_DT'])

                est = pd.concat([est_m, findata_.reset_index(level=[1])]).sort_index()
                est.loc[:, ['NO_TYP','CONS_YN']] = est.loc[:, ['NO_TYP','CONS_YN']].ffill().fillna('')
                est = est[est.YYMM == est.YYMM]
                est = est.set_index([est.index, 'YYMM'])
                cmpinfo = pd.DataFrame(index=est.index, columns=['MKT_TYP', 'GICS_CD', 'WI26_CD'])
                cmpinfo.loc[:,'MKT_TYP'] = company.loc[cmp_cd, 'MKT_TYP']
                cmpinfo.loc[:,'GICS_CD'] = company.loc[cmp_cd, 'GICS_CD']
                cmpinfo.loc[:,'WI26_CD'] = company.loc[cmp_cd, 'WI26_CD']

                data = pd.concat([cmpinfo, est], axis=1)
                if to_csv:
                    write_csv(path, term_acct, cmp_cd, data)
                if to_clickhouse:
                    write_clickhouse(con_, 'cmpcns_bycmp', term_acct, cmp_cd, data)

        except Exception as e:
            logger.error(f'{e}')


if __name__ == '__main__':
    config_file = '/home/mining/PycharmProjects/wisefn_data_processing/db3.config'
    start_dt = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
    end_dt = datetime.datetime.now().strftime('%Y%m%d')

    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    if len(sys.argv) > 2:
        start_dt = sys.argv[2]
    if len(sys.argv) > 3:
        end_dt = sys.argv[3]

    conf = get_config(config_file)
    logging.basicConfig(level=eval(conf['Logging']['level']),
        # handlers=[eval(conf['Logging']['handler'])],
        format=conf['Logging']['format'],
        )

    #FIXME
    start_dt = '20210401'
    end_dt = '20210401'
    generate_cmp_cns_by_cmp(conf, start_dt=start_dt, end_dt=end_dt)

