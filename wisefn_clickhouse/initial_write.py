from wisefn_clickhouse.clickhouse_connection import ClickHouseConnection
from wisefn_clickhouse.wisefn2clickhouse_schema import ClickHouseSchema
from wisefn_clickhouse.clickhouse_writer import ClickHouseWriter
import pandas as pd
import logging
import os
import re
import gc
from wisefn_clickhouse.config import get_config
import sys
from logging.handlers import RotatingFileHandler


logger = logging.getLogger('initial_write')

if __name__ == '__main__':
    import data
    config_file = '/home/mining/projects/wisefn_data_processing/db2513.config'
    dbname = 'wisefn'

    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    conf = get_config(config_file)

    logging.basicConfig(level=eval(conf['Logging']['level']),
        handlers=[eval(conf['Logging']['handler'])],
        format=conf['Logging']['format'],
        )

    excel_file = data.__path__[0] + '/' + 'DBSpec_DaumSoft_20210407.xlsx'
    cover = pd.read_excel(excel_file, sheet_name=0, skiprows=3)
    table_names = list(cover['파일명'].dropna())

    writer = ClickHouseWriter(host=conf['ClickHouse']['host'], 
        database=dbname, 
        user=conf['ClickHouse']['user'], 
        password=conf['ClickHouse']['password'])

    dates = sorted(os.listdir(conf['Path']['base_path'] + '/01TXT_file'))
    for table_name in table_names[:]:
        writer.write_financial_csv_to_clickhouse(
            table_name,
            conf['Path']['base_path'] + '/02ALL',
            [''])  
            
        writer.write_annual_financial_csv_to_clickhouse(
            table_name, 
            conf['Path']['base_path'] + '/02ALL_allitem', 
            ['2015', '2016', '2017', '2018', '2019', '2020'])
        
        writer.write_financial_csv_to_clickhouse(
            table_name, 
            conf['Path']['base_path'] + '/01TXT_file', 
            dates)
