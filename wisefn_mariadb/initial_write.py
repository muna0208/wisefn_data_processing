
from wisefn_mariadb.db_connection import MariaDBConnection
from wisefn_mariadb.wisefn2mariadb_schema import WiseFNSchema
from wisefn_mariadb.db_writer import MariaDBConnection, MariaDBWriter
import pandas as pd
import logging
import os
import re
import gc
from wisefn_mariadb.config import get_config
import sys
from logging.handlers import RotatingFileHandler


logger = logging.getLogger('initial_write')

if __name__ == '__main__':
    import data
    config_file = '/home/mining/PycharmProjects/wisefn_data_processing/db3.config'
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
    
    db = MariaDBWriter(conf['MariaDB']['connection_uri'] + '/' + dbname) 
    dates = sorted(os.listdir(conf['Path']['base_path'] + '/01TXT_file'))
    for table_name in table_names[:]:
        db.write_financial_csv_to_mariadb(
            conf,
            table_name,
            conf['Path']['base_path'] + '/02ALL',
            [''])

        db.write_annual_financial_csv_to_mariadb(
            conf,
            table_name, 
            conf['Path']['base_path'] + '/02ALL_allitem', 
            ['2015', '2016', '2017', '2018', '2019', '2020'])            
        
        db.bulk_write_financial_csv_to_mariadb(
            conf,
            table_name, 
            conf['Path']['base_path'] + '/01TXT_file', 
            dates)
