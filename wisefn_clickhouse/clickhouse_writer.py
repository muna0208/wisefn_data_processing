from wisefn_clickhouse.clickhouse_connection import ClickHouseConnection
from wisefn_clickhouse.wisefn2clickhouse_schema import ClickHouseSchema
import pandas as pd
import logging
import os
import re
import gc
from wisefn_clickhouse.config import get_config
import sys
from logging.handlers import RotatingFileHandler


logger = logging.getLogger('clickhouse_writer')


class ClickHouseWriter:
    def __init__(self, **argv):
        self.connection = ClickHouseConnection(**argv)
        self.client = self.connection.get_client() 
        self.database = None
        if 'database' in argv:
            self.database = argv['database']
        else:
            logger.error(f'database error {self.database}')
        self.schema = ClickHouseSchema(**argv)


    def write_annual_financial_csv_to_clickhouse(self, table_name, input_path, years, chunksize=10000):
        self.schema.get_columns(table_name)
        field_types = self.schema.get_schema(table_name)
        types = []
        names = []
        for row in field_types:
            types += [(row[0], 'str' if 'String' in row[1] else 'float64' if 'Float' in row[1] else 'str')]
            names += [row[0]]
        logger.debug(f'{dict(types)}')

        for yr in years:
            logger.info(f'date {yr} table {table_name}')

            if os.path.exists(input_path + '/' + yr + '/' + f'{table_name}.TXT'):
                try:
                    df = pd.read_csv(
                        input_path + '/' + yr + '/' + f'{table_name}.TXT',
                        encoding='euc-kr',
                        names= names[1:],
                        dtype= dict(types[1:]),
                        sep='|',
                    )
                except Exception as e:
                    logger.error(f'{e}')
                df[names[0]] = yr + '1231'
                cols = df.select_dtypes(include=[object]).columns
                df.loc[:, cols] = df.loc[:, cols].fillna('')
                try:
                    logger.debug(f"INSERT INTO {table_name} ({','.join(names)}) VALUES")
                    for idx in range(0, df.shape[0], chunksize): 
                        n = self.client.insert_dataframe(f"INSERT INTO {table_name} VALUES", df.iloc[idx:idx+chunksize, :][names])
                        logger.debug(f'number of rows written: {n}')
                        if n == 0:
                            logger.error(f'0 rows written: {table_name}')
                except Exception as e:
                    logger.error(f'{e}')
                logger.info(f'{df.tail()}')


    def write_financial_csv_to_clickhouse(self, table_name, input_path, dates, chunksize=10000):
        self.schema.get_columns(table_name)
        field_types = self.schema.get_schema(table_name)
        types = []
        names = []
        for row in field_types:
            types += [(row[0], 'str' if 'String' in row[1] else 'float64' if 'Float' in row[1] else 'str')]
            names += [row[0]]
        logger.debug(f'{dict(types)}')

        for dt in dates:
            logger.info(f'date {dt} table {table_name}')

            if os.path.exists(input_path + '/' + dt + '/' + f'{table_name}.TXT'):
                try:
                    df = pd.read_csv(
                        input_path + '/' + dt + '/' + f'{table_name}.TXT',
                        encoding='euc-kr',
                        names= names[1:],
                        dtype= dict(types[1:]),
                        sep='|',
                    )
                except Exception as e:
                    logger.error(f'{e}')
                if dt == '':
                    df[names[0]] = '20141231'
                else:    
                    df[names[0]] = dt
                cols = df.select_dtypes(include=[object]).columns
                df.loc[:, cols] = df.loc[:, cols].fillna('')
                try:
                    logger.debug(f"INSERT INTO {table_name} ({','.join(names)}) VALUES")
                    for idx in range(0, df.shape[0], chunksize): 
                        n = self.client.insert_dataframe(f"INSERT INTO {table_name} VALUES", df.iloc[idx:idx+chunksize, :][names])
                        logger.debug(f'number of rows written: {n}')
                        if n == 0:
                            logger.error(f'0 rows written: {table_name}')
                except Exception as e:
                    logger.error(f'{e}')
                logger.info(f'{df[names].tail()}')


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

    db = ClickHouseWriter(host=conf['ClickHouse']['host'], 
        database=conf['ClickHouse']['database'], 
        user=conf['ClickHouse']['user'], 
        password=conf['ClickHouse']['password'])

    if len(sys.argv) > 2 and  sys.argv[2] == 'initial':
        for table_name in table_names[:]:
            db.write_financial_csv_to_clickhouse(
                table_name,
                '/home/mining/systemtrading/WiseFN_Down_DATA/02ALL',
                [''])  
    
    if len(sys.argv) > 2 and  sys.argv[2] == 'annual':
        for table_name in table_names[:]:
            db.write_annual_financial_csv_to_clickhouse(
                table_name, 
                '/home/mining/systemtrading/WiseFN_Down_DATA/02ALL_allitem', 
                ['2015', '2016', '2017', '2018', '2019', '2020'])

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
        db.write_financial_csv_to_clickhouse(
            table_name, 
            input_path, 
            dates)

    # python initial_write.py db.config initial annual
