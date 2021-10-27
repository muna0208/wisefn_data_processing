from sqlalchemy import create_engine
import pymysql
import pandas as pd
import numpy as np
import logging
import urllib.parse
from wisefn_mariadb.config import get_config


logger = logging.getLogger('wisefn2mariadb_schema')

'''
CREATE TABLE IF NOT EXISTS TC_COMPANY(CMP_CD char(06) NOT NULL,MKT_TYP tinyint,CMP_NM_KOR varchar(100),CMP_NM_ENG varchar(100),CMP_FUL_NM_KOR varchar(200),CMP_FUL_NM_ENG varchar(200),BIZ_REG_NO varchar(13),CORP_REG_NO varchar(15),FINACC_TYP tinyint,GICS_CD varchar(10),WI26_CD varchar(10),EST_DT char(08),LIST_DT char(08),DELIST_DT char(08),FYE_MN tinyint,CEO varchar(50),ADDR varchar(150),ZIP_CD varchar(07),TEL varchar(20),URL varchar(50),AUDITOR varchar(30),IFRS_YYMM varchar(06),IFRS_YN tinyint,MASTER_CHK varchar(1),QTR_MASTER varchar(1),LIST_YN tinyint,PRIMARY KEY(CMP_CD))
CREATE TABLE IF NOT EXISTS TC_SECTOR(SEC_CD varchar(10) NOT NULL,SEC_TYP char(01),P_SEC_CD varchar(10),SEC_NM_KOR varchar(200),SEC_NM_ENG varchar(200),MKT_TYP tinyint,PRIMARY KEY(SEC_CD))
CREATE TABLE IF NOT EXISTS TF_CMP_FINDATA_INFO(CMP_CD char(06) NOT NULL,YYMM char(06) NOT NULL,TERM_TYP tinyint NOT NULL,START_DT varchar(08),END_DT varchar(08),DATA_EXIST_YN char(01),FST_PUB_DT varchar(08),PF_DT varchar(08),LAA_YN tinyint,LAQ_YN tinyint,SEQ smallint,PRIMARY KEY(CMP_CD,YYMM,TERM_TYP))
CREATE TABLE IF NOT EXISTS TF_CMP_FINPRD(CMP_CD char(06) NOT NULL,YYMM char(06) NOT NULL,CAL_YEAR smallint,CAL_QTR tinyint,CAL_USE_YN tinyint,FS_YEAR smallint,FS_QTR tinyint,FS_USE_YN tinyint,IFRS_CHK tinyint,MASTER_CHK char(01),QTR_MASTER char(01),PRIMARY KEY(CMP_CD,YYMM))
CREATE TABLE IF NOT EXISTS TF_CMP_FINDATA(CMP_CD char(06) NOT NULL,TERM_TYP tinyint NOT NULL,YYMM char(06) NOT NULL,ITEM_TYP char(01) NOT NULL,ITEM_CD char(06) NOT NULL,VAL decimal(28,5),PRIMARY KEY(CMP_CD,TERM_TYP,YYMM,ITEM_TYP,ITEM_CD))
CREATE TABLE IF NOT EXISTS TF_SEC_FINDATA(SEC_CD varchar(10) NOT NULL,TERM_TYP tinyint NOT NULL,YYMM char(06) NOT NULL,ITEM_TYP char(01) NOT NULL,ITEM_CD char(06) NOT NULL,VAL decimal(28,5),PRIMARY KEY(SEC_CD,TERM_TYP,YYMM,ITEM_TYP,ITEM_CD))
CREATE TABLE IF NOT EXISTS TZ_WORK_HST(CMP_CD char(06) NOT NULL,YYMM char(06) NOT NULL,TERM_TYP tinyint NOT NULL,PRIMARY KEY(CMP_CD,YYMM,TERM_TYP))
CREATE TABLE IF NOT EXISTS TT_EST_MASTER(CMP_CD char(6) NOT NULL,NO_TYP tinyint,CONS_YN tinyint,START_DT char(8) NOT NULL,END_DT char(8),PRIMARY KEY(CMP_CD,START_DT))
CREATE TABLE IF NOT EXISTS TT_CMP_CNS_DATA(CMP_CD char(06) NOT NULL,CNS_DT char(08) NOT NULL,TERM_TYP tinyint NOT NULL,YYMM char(06) NOT NULL,ITEM_TYP char(01) NOT NULL,ITEM_CD char(06) NOT NULL,VAL decimal(28,5),PRIMARY KEY(CMP_CD,CNS_DT,TERM_TYP,YYMM,ITEM_TYP,ITEM_CD))
CREATE TABLE IF NOT EXISTS TT_SEC_CNS_DATA(SEC_CD varchar(10) NOT NULL,CNS_DT char(08) NOT NULL,TERM_TYP tinyint NOT NULL,YYMM char(06) NOT NULL,ITEM_TYP char(01) NOT NULL,ITEM_CD char(06) NOT NULL,VAL decimal(28,5),PRIMARY KEY(SEC_CD,CNS_DT,TERM_TYP,YYMM,ITEM_TYP,ITEM_CD))
CREATE TABLE IF NOT EXISTS TZ_ITEM(ITEM_CD char(06) NOT NULL,FINACC_TYP tinyint NOT NULL,ITEM_TYP char(01) NOT NULL,FS_TYP char(02),ITEM_NM_KOR varchar(200),ITEM_NM_ENG varchar(200),UNT_TYP varchar(20),DET_SEQ int,SUM_SEQ int,SEL_SEQ int, PRIMARY KEY(ITEM_CD,FINACC_TYP,ITEM_TYP))
CREATE TABLE IF NOT EXISTS CODE_INFO (CODE VARCHAR(100), FIN_TYPE VARCHAR(100), CODE_TYPE VARCHAR(100), FIN_STMT_TYPE VARCHAR(100), KOR_ITEM_NAME VARCHAR(100), ENG_ITEM_NAME VARCHAR(100), UNIT VARCHAR(100), PRIMARY KEY(CODE))
'''

class WiseFNSchema:
    def __init__(self, connection_uri='', db_name=''):
        logger.info(f'connection uri {connection_uri}')
        self.db_name = db_name
        self.engine = create_engine(connection_uri + '/' + db_name)

    def _create_database(self, db_name):
        try:
            self.engine.execute(f"CREATE DATABASE IF NOT EXISTS {db_name} " 
            f"DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
        except Exception as e:
            logger.error(f'{e}')

    def create_table(self, data):
        sql = "CREATE TABLE IF NOT EXISTS " + data.iloc[0, :].loc['파일명']  + "(" 
        for i, d in data[['컬럼명', 'DataType', 'Null',]].iterrows():
            sql += d['컬럼명'] + ' ' + d['DataType'] 
            if d['Null'] != 'Y':
                sql += ' NOT NULL,'
            else:
                sql += ','
        sql += 'PRIMARY KEY(' + ','.join(list(data[data['Key'] == 'PK']['컬럼명'])) + '))'
        logger.info(f'{sql}')
        try:
            self.engine.execute(sql)
        except Exception as e:
            logger.error(f'{e}')

        try: # for the legacy data, these should be nullable
            if data.iloc[0, :].loc['파일명'] == 'TZ_ITEM':
                sql = "ALTER TABLE TZ_ITEM MODIFY DET_SEQ INT"
                self.engine.execute(sql)
                sql = "ALTER TABLE TZ_ITEM MODIFY SUM_SEQ INT"
                self.engine.execute(sql)                
                sql = "ALTER TABLE TZ_ITEM MODIFY SEL_SEQ INT"
                self.engine.execute(sql)                    
        except Exception as e:
            logger.error(f'{e}')

        try: 
            if data.iloc[0, :].loc['파일명'] in {'TF_CMP_FINDATA', 'TF_SEC_FINDATA', 'TT_CMP_CNS_DATA', 'TT_SEC_CNS_DATA'}:
                query = f"CREATE INDEX item_cd_index ON {data.iloc[0, :].loc['파일명']}(ITEM_CD)"
                logger.info(f'{query}')
                self.engine.execute(query)
        except Exception as e:
            logger.error(f'{e}')


    def create_index(self, index_name, table_name, columns):
        sql = f"CREATE INDEX {index_name} ON {table_name} ({columns})" 
        logger.info(f'{sql}')
        try:
            self.engine.execute(sql)
        except Exception as e:
            logger.error(f'{e}')      

    def create_table_from_data_frame(self, df):
        index_list = df[~df['파일명'].isna()].index
        index_list = index_list.append(df.index[-1:])
        intervals = list(zip(index_list[:-1], [df.index[i-1] if i != index_list[-1] else df.index[i] for i in index_list[1:]]))
        for intv in intervals:
            logger.info(f'intervals {intv}')
            try:
                self.create_table(df.loc[intv[0]:intv[1], :])
            except Exception as e:
                logger.error(f'{e}')

    def get_fields(self, table_name):
        sql = f'SHOW FIELDS FROM {table_name}'
        result = None
        try:
            result = self.engine.execute(sql)
            df = pd.DataFrame(result)
            df.columns = ['Field', 'Type', 'Null', 'Key',  'Default', 'Extra']
            return df
        except Exception as e:
            logger.error(f'{e}')
        return result


if __name__ == '__main__':
    import sys
    import data
    import pkg_resources
    excel_file = data.__path__[0] + '/' + 'DBSpec_DaumSoft_20210407.xlsx'
    logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(filename)s %(lineno)d %(levelname)s - %(message)s')

    cover = pd.read_excel(excel_file, sheet_name=0, skiprows=3)
    financial = pd.read_excel(excel_file, sheet_name=1, skiprows=2, index_col=None)
    concensus = pd.read_excel(excel_file, sheet_name=2, skiprows=2)
    items = pd.read_excel(excel_file, sheet_name=3, skiprows=2)
    code_info = pd.read_excel(excel_file, sheet_name=4, skiprows=1, dtype='str')

    config_file = '/home/mining/projects/wisefn_data_processing/db2513.config'
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    conf = get_config(config_file)

    if len(sys.argv) > 2 and sys.argv[2] == 'create_db':
        logger.info(f"create database {conf['MariaDB']['db_name']}")
        schema = WiseFNSchema(connection_uri=conf['MariaDB']['connection_uri'] , db_name='')
        schema._create_database(conf['MariaDB']['db_name'])

    schema = WiseFNSchema(connection_uri=conf['MariaDB']['connection_uri'] , db_name=conf['MariaDB']['db_name'])
    schema.create_table_from_data_frame(financial)
    schema.create_table_from_data_frame(concensus)
    schema.create_table_from_data_frame(items)

    code_info = code_info.iloc[:,1:]
    code_info.columns = ('CODE', 'FIN_TYPE', 'CODE_TYPE', 'FIN_STMT_TYPE', 'KOR_ITEM_NAME', 'ENG_ITEM_NAME', 'UNIT')
    sql = (
        "CREATE TABLE IF NOT EXISTS CODE_INFO (" +
        "CODE VARCHAR(100), " +
        "FIN_TYPE VARCHAR(100), " +
        "CODE_TYPE VARCHAR(100), " +
        "FIN_STMT_TYPE VARCHAR(100), " +
        "KOR_ITEM_NAME VARCHAR(100), " +
        "ENG_ITEM_NAME VARCHAR(100), " +
        "UNIT VARCHAR(100), " +
        "PRIMARY KEY(CODE))")
    schema.engine.execute(sql)
    code_info.to_sql('CODE_INFO', con=schema.engine, if_exists='replace', index=False)
    df = pd.read_sql_table('CODE_INFO', con=schema.engine)
    logger.info(df.tail())




