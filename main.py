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

from wisefn_mariadb.db_connection import MariaDBConnection
from wisefn_mariadb.wisefn2mariadb_schema import WiseFNSchema
from wisefn_mariadb.db_reader import MariaDBReader
from wisefn_mariadb.config import get_config
from logging.handlers import RotatingFileHandler
from preprocessing.clickhouse_financial.clickhouse_helper import ClickHouseConnection

import wisefn_mariadb.db_writer
import wisefn_clickhouse.clickhouse_writer
import preprocessing.mariadb_convert.main
import preprocessing.annual.main
import preprocessing.quarterly.main
import wisefn_mariadb.main
import wisefn_clickhouse.main
import multiprocessing as mp

logger = logging.getLogger('main')

def main(config_file, 
        start_dt=None, 
        end_dt=None, 
        to_csv=False, 
        to_clickhouse=True, 
        last_date_only=True, 
        table_names=[]):
    conf = get_config(config_file)
    path = conf['Path']['txt_path']
    dates = sorted(os.listdir(f'{path}'))
    excel_file = list(data.__path__)[0] + '/' + 'DBSpec_DaumSoft_20210407.xlsx'
    cover = pd.read_excel(excel_file, sheet_name=0, skiprows=3)
    if len(table_names) == 0:
        table_names = list(cover['파일명'].dropna())

    if start_dt is None or last_date_only:
        start_dt = dates[-1]
    if end_dt is None or last_date_only:
        end_dt = dates[-1]

    logger.info(f'start date {start_dt}, end date {end_dt}, to_csv: {to_csv}, to_clickhouse {to_clickhouse}')
    dates = [d for d in dates if start_dt <= d and d <= end_dt]

    logger.info('starting main...')
    try:
        logger.info('start wisefn_mariadb.main')
        wisefn_mariadb.main.main(config_file, dates[0], dates[-1], last_date_only, table_names)
    except Exception as e:
        logger.error(f'{e}')
    try:
        logger.info('start wisefn_clickhouse.main')
        wisefn_clickhouse.main.main(config_file, dates[0], dates[-1], last_date_only, table_names)
    except Exception as e:
        logger.error(f'{e}')
    try:
        logger.info('start preprocessing.mariadb_convert.main')
        preprocessing.mariadb_convert.main.main(config_file, dates[0], dates[-1], to_csv, to_clickhouse)
        logger.info('start preprocessing.annual.main')
        preprocessing.annual.main.main(config_file, dates[0], dates[-1], to_csv, to_clickhouse)
        logger.info('start preprocessing.quarterly.main')
        preprocessing.quarterly.main.main(config_file, dates[0], dates[-1], to_csv, to_clickhouse)
        logger.info('main compelete...')
    except Exception as e:
        logger.error(f'{e}')


if __name__ == '__main__':
    import sys, getopt
    import data
    config_file = '/home/mining/PycharmProjects/wisefn_data_processing/db3.config'
    start_dt = None
    end_dt = None
    to_csv = False
    to_clickhouse = True
    last_date_only = True
    table_names = []

    try:
        opts, args = getopt.getopt(sys.argv[1:],"hc:S:E:C:H:L:T:")
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
                "-T [\\'TF_CMP_FINDATA\\', \\'TC_COMPANY\\'] (table_names)")
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
    
    main(config_file, start_dt, end_dt, to_csv, to_clickhouse, last_date_only, table_names)


