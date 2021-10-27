import csv
import datetime
import logging
import pandas as pd
import numpy as np
import os
import sys
import re

from preprocessing.config import config
from preprocessing.defines import TermAcctMethod, TermAcctDir, FinAcctType

from wisefn_clickhouse.clickhouse_connection import ClickHouseConnection
from wisefn_clickhouse.wisefn2clickhouse_schema import ClickHouseSchema
from wisefn_clickhouse.clickhouse_writer import ClickHouseWriter
from wisefn_clickhouse.config import get_config
from logging.handlers import RotatingFileHandler


logger = logging.getLogger('wisefn_clickhouse main')


def main(config_file, start_dt, end_dt, last_date_only=True, table_names=[]):
    import data
    conf = get_config(config_file)
   
    excel_file = list(data.__path__)[0] + '/' + 'DBSpec_DaumSoft_20210407.xlsx'
    cover = pd.read_excel(excel_file, sheet_name=0, skiprows=3)
    if len(table_names) == 0:
        table_names = list(cover['파일명'].dropna())
    
    conf['ClickHouse']['database'] = 'wisefn'
    writer = ClickHouseWriter(host=conf['ClickHouse']['host'], 
        database=conf['ClickHouse']['database'], 
        user=conf['ClickHouse']['user'], 
        password=conf['ClickHouse']['password'])
    
    dates = sorted(os.listdir(conf['Path']['txt_path']))
    dates = [d for d in dates if start_dt <= d and d <= end_dt]
    if last_date_only:
        dates = dates[-1:]
    
    logger.info(f'processing dates: {dates}')

    for table_name in table_names[:]:
        writer.write_financial_csv_to_clickhouse(
            table_name, 
            conf['Path']['txt_path'], 
            dates,
            chunksize=eval(conf['ClickHouse']['chunksize']))


if __name__ == '__main__':
    import sys, getopt
    config_file = '/home/mining/PycharmProjects/wisefn_data_processing/db3.config'
    start_dt = (datetime.datetime.now() - datetime.timedelta(days=80)).strftime('%Y%m%d')
    end_dt = datetime.datetime.now().strftime('%Y%m%d')
    to_csv = True
    to_clickhouse = True
    last_date_only = True
    table_names = []
    
    try:
        opts, args = getopt.getopt(sys.argv[1:],"hc:S:E:C:H:L:")
    except getopt.GetoptError:
        print(f'option error')
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print(
                'example: main.py -c conf.config (config_file) '
                '-S 20000101 (start_dt) '
                '-E 20251231 (end_dt) '
                '-C True (to_csv) '
                '-H False (to_clickhouse) '
                '-L True (last_date_only) '
                "-T [\\'TC_CMP_FINDATA\\', \\'TC_COMPANY\\'] (table_names)")
            sys.exit()
        elif opt in ("-c"):
            config_file = arg
        elif opt in ("-S"):
            start_dt = arg
        elif opt in ("-E"):
            end_dt = arg
        elif opt in ("-C"):
            to_csv = eval(arg)
        elif opt in ("-H"):
            to_clickhouse = eval(arg)
        elif opt in ("-L"):
            last_date_only = eval(arg)
        elif opt in ("-T"):
            table_names = eval(arg)

    conf = get_config(config_file)
    logging.basicConfig(level=eval(conf['Logging']['level']),
        handlers=[eval(conf['Logging']['handler'])],
        format=conf['Logging']['format'],)

    main(config_file, start_dt, end_dt, last_date_only, table_names)
