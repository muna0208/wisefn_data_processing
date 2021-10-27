from wisefn_mariadb.db_connection import MariaDBConnection
from wisefn_mariadb.wisefn2mariadb_schema import WiseFNSchema
import pandas as pd
import logging
import os
import re
import gc
from wisefn_mariadb.config import get_config
import sys
from logging.handlers import RotatingFileHandler

logger = logging.getLogger('dbwriter')


class MariaDBWriter:
    def __init__(self, connection_uri=None):
        self.connection = MariaDBConnection(connection_uri=connection_uri)
        self.engine = self.connection.get_alchemy_engine()
        self.schema = WiseFNSchema

    @staticmethod
    def mysql_replace_into(table, conn, keys, data_iter):
        from sqlalchemy.dialects.mysql import insert
        data = [dict(zip(keys, row)) for row in data_iter]
        stmt = insert(table.table).values(data)
        update_stmt = stmt.on_duplicate_key_update(**dict(zip(stmt.inserted.keys(), stmt.inserted.values())))
        conn.execute(update_stmt)

    def write_financial_csv_to_mariadb(self, conf, table_name, input_path, dates=[''], chunksize=50000):
        schema = WiseFNSchema(connection_uri=conf['MariaDB']['connection_uri'],
            db_name=conf['MariaDB']['db_name']).get_fields(table_name)
        names = list(schema.Field)

        for dt in dates:
            logger.info(f'date {dt} table {table_name}')
            try:
                if os.path.exists(input_path + '/' + dt + '/' + f'{table_name}.TXT'):
                    d = pd.read_csv(
                        input_path + '/' + dt + '/' + f'{table_name}.TXT',
                        encoding='euc-kr',
                        names=names,
                        dtype='str',
                        sep='|',
                    )
                    d.to_sql(name=table_name, con=self.engine, chunksize=chunksize, 
                    method=MariaDBWriter.mysql_replace_into, if_exists='append', index=False)
                    logger.info(f'{d.tail()}')
            except Exception as e:
                logger.error(f'{e}')

    def upsert_financial_csv_to_mariadb(self, conf, table_name, input_path):
        schema = WiseFNSchema(connection_uri=conf['MariaDB']['connection_uri'], 
                              db_name=conf['MariaDB']['db_name']).get_fields(table_name)
        names = list(schema.Field)
        logger.info(f'table {table_name}')
        try:
            if os.path.exists(input_path + '/' + f'{table_name}.TXT'):
                d = pd.read_csv(
                    input_path + '/' + f'{table_name}.TXT',
                    encoding='euc-kr',
                    names=names,
                    dtype='str',
                    sep='|',
                )
                d.to_sql('myTempTable', con=self.engine, if_exists ='replace', index=False)
                with self.engine.connect() as con:
                    con.execute(f"INSERT IGNORE INTO {table_name} SELECT * FROM myTempTable")
                    con.execute(f"DROP TABLE myTempTable")
                logger.info(f'{d.tail()}')       
        except Exception as e:
            logger.error(f'{e}')


    def write_annual_financial_csv_to_mariadb(self, conf, table_name, input_path, years, chunksize=50000):
        schema = WiseFNSchema(connection_uri=conf['MariaDB']['connection_uri'], 
                              db_name=conf['MariaDB']['db_name']).get_fields(table_name)
        names = list(schema.Field)
        for yr in years:
            logger.info(f'year {yr} table {table_name}')
            try:
                if os.path.exists(input_path + '/' + yr + '/' + f'{table_name}.TXT'):
                    d = pd.read_csv(
                        input_path + '/' + yr + '/' + f'{table_name}.TXT',
                        encoding='euc-kr',
                        names=names,
                        dtype='str',
                        sep='|',
                    )
                    d.to_sql(name=table_name, con=self.engine, chunksize=chunksize, 
                    method=MariaDBWriter.mysql_replace_into, if_exists='append', index=False)
                    logger.info(f'{d.tail()}')
            except Exception as e:
                logger.error(f'{e}')


    def bulk_write_financial_csv_to_mariadb(self, conf, table_name, input_path, dates, num_days=20, chunksize=50000):
        schema = WiseFNSchema(connection_uri=conf['MariaDB']['connection_uri'], 
                              db_name=conf['MariaDB']['db_name']).get_fields(table_name)
        names = list(schema.Field)
        '''
        types = ['str' if 'char' in s 
            else 'int64' if 'int' in s 
            else 'float64' if 'float' in s 
            else 'float64' if 'decimal' in s else 'str' 
            for s in list(schema.Type)]
        cols_dtype = dict(zip(names, types))
        '''
        keys = schema[schema.Key=='PRI'].Field

        for idx in range(0, len(dates), num_days):
            data_list = []
            try:
                for dt in dates[idx:idx+num_days]:
                    logger.info(f'date {dt} table {table_name}')
                    if os.path.exists(input_path + '/' + dt + '/' + f'{table_name}.TXT'):
                        d = pd.read_csv(
                            input_path + '/' + dt + '/' + f'{table_name}.TXT',
                            encoding='euc-kr',
                            names=names,
                            dtype='str',
                            sep='|',
                        )
                        data_list += [d]
                df = pd.concat(data_list)
                df = df.drop_duplicates(subset=list(keys), keep='last')
                df.to_sql(name=table_name, con=self.engine, chunksize=chunksize, 
                method=MariaDBWriter.mysql_replace_into, if_exists='append', index=False)
                logger.info(f'data {df.tail()}')    
            except Exception as e:
                logger.error(f'{e}')
    

if __name__ == '__main__':
    config_file = '/home/mining/PycharmProjects/wisefn_data_processing/db3.config'
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    conf = get_config(config_file)

    logging.basicConfig(level=eval(conf['Logging']['level']),
        handlers=[eval(conf['Logging']['handler'])],
        format=conf['Logging']['format'],
        )

    execel_file = '/home/mining/systemtrading/WiseFN_Down_DATA/DBSpec_DaumSoft_20210407.xlsx'
    excel_file = conf['Path']['excel_file'] 
    cover = pd.read_excel(excel_file, sheet_name=0, skiprows=3)
    table_names = list(cover['파일명'].dropna())
    
    db = MariaDBWriter(conf['MariaDB']['connection_uri'] + '/' + conf['MariaDB']['db_name'])
    if len(sys.argv) > 2 and  sys.argv[2] == 'annual':
        for table_name in table_names[:]:
            db.write_annual_financial_csv_to_mariadb(
                conf,
                table_name, 
                '/home/mining/systemtrading/WiseFN_Down_DATA/02ALL_allitem', 
                ['2015', '2016', '2017', '2018', '2019', '2020'])
    
    if len(sys.argv) > 2 and  sys.argv[2] == 'upsert':
        for table_name in table_names[:]:
            db.upsert_financial_csv_to_mariadb(
                conf,
                table_name,
                '/home/mining/systemtrading/WiseFN_Down_DATA/02ALL',
                )  
    input_path = conf['Path']['txt_path']
    start = -1
    if len(sys.argv) > 3 and sys.argv[2] == 'start':
        start = int(sys.argv[3])
    dates = sorted(os.listdir(input_path))[start:]
    
    if len(sys.argv) > 3 and re.search(r"[\d]{8}", sys.argv[2]) and re.search(r"[\d]{8}", sys.argv[3]) :
        start_dt = sys.argv[2]
        end_dt = sys.argv[3]
        dates = [d for d in sorted(os.listdir(input_path))[:] if start_dt <= d <=end_dt]

    logger.info(f'processing dates: {dates}')

    for table_name in table_names[:]:
        db.bulk_write_financial_csv_to_mariadb(conf, table_name, input_path, dates)

