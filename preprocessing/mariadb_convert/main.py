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
from preprocessing.mariadb_convert.cmp_consensus_by_cmp_convert import generate_cmp_cns_by_cmp
from preprocessing.mariadb_convert.cmp_consensus_by_item_convert import generate_cmp_cns_by_item
from preprocessing.mariadb_convert.cmp_findata_by_cmp_convert import generate_cmp_fin_by_cmp
from preprocessing.mariadb_convert.cmp_findata_by_item_convert import generate_cmp_fin_by_item
from preprocessing.mariadb_convert.sec_consensus_by_item_convert import generate_sec_cns_by_item
from preprocessing.mariadb_convert.sec_consensus_by_sec_convert import generate_sec_cns_by_sec
from preprocessing.mariadb_convert.sec_findata_by_item_convert import generate_sec_fin_by_item
from preprocessing.mariadb_convert.sec_findata_by_sec_convert import generate_sec_fin_by_sec
from preprocessing.mariadb_convert.all_items.cmp_findata_by_cmp_convert import generate_cmp_fin_by_cmp_all
from preprocessing.mariadb_convert.all_items.sec_findata_by_sec_convert import generate_sec_fin_by_sec_all

def main(config_file, start_dt, end_dt, to_csv, to_clickhouse):
    import multiprocessing as mp
    conf = get_config(config_file)
    conf['ClickHouse']['database'] ='financial'
    conf['MariaDB']['db_name'] = 'wisefn'

    #generate_cmp_cns_by_cmp(conf, start_dt=start_dt, end_dt=end_dt, to_csv=to_csv, to_clickhouse=to_clickhouse)
    #generate_cmp_cns_by_item(conf, start_dt=start_dt, end_dt=end_dt, to_csv=to_csv, to_clickhouse=to_clickhouse)
    #generate_cmp_fin_by_cmp(conf, to_csv=to_csv, to_clickhouse=to_clickhouse)
    #generate_cmp_fin_by_item(conf, to_csv=to_csv, to_clickhouse=to_clickhouse)
    #generate_sec_cns_by_item(conf, start_dt=start_dt, end_dt=end_dt, to_csv=to_csv, to_clickhouse=to_clickhouse)
    #generate_sec_cns_by_sec(conf, start_dt=start_dt, end_dt=end_dt, to_csv=to_csv, to_clickhouse=to_clickhouse)
    #generate_sec_fin_by_item(conf, to_csv=to_csv, to_clickhouse=to_clickhouse)
    #generate_sec_fin_by_sec(conf, to_csv=to_csv, to_clickhouse=to_clickhouse)
    #generate_cmp_fin_by_cmp_all(conf, to_csv=to_csv, to_clickhouse=to_clickhouse)
    #generate_sec_fin_by_sec_all(conf, to_csv=to_csv, to_clickhouse=to_clickhouse)

    # proc_list = []
    
    # p = mp.Process(target=generate_cmp_cns_by_item, args=(conf,), kwargs={'start_dt':start_dt, 'end_dt':end_dt, 'to_csv':to_csv, 'to_clickhouse':to_clickhouse})
    # proc_list += [p]
    # p.start()

    # p = mp.Process(target=generate_sec_cns_by_item, args=(conf,), kwargs={'start_dt':start_dt, 'end_dt':end_dt, 'to_csv':to_csv, 'to_clickhouse':to_clickhouse})
    # proc_list += [p]
    # p.start()

    # for e in proc_list:
    #     e.join()

    generate_cmp_cns_by_cmp(conf, start_dt=start_dt, end_dt=end_dt, to_csv=to_csv, to_clickhouse=to_clickhouse)
    generate_cmp_fin_by_cmp(conf, start_dt=start_dt, end_dt=end_dt, to_csv=to_csv, to_clickhouse=to_clickhouse, num_procs=3)
    generate_cmp_fin_by_item(conf, to_csv=to_csv, to_clickhouse=to_clickhouse)
    generate_sec_cns_by_sec(conf, start_dt=start_dt, end_dt=end_dt, to_csv=to_csv, to_clickhouse=to_clickhouse)
    generate_sec_fin_by_item(conf, to_csv=to_csv, to_clickhouse=to_clickhouse)
    generate_sec_fin_by_sec(conf, to_csv=to_csv, to_clickhouse=to_clickhouse)
    generate_cmp_fin_by_cmp_all(conf, start_dt=start_dt, end_dt=end_dt, to_csv=to_csv, to_clickhouse=to_clickhouse, num_procs=3)
    generate_sec_fin_by_sec_all(conf, to_csv=to_csv, to_clickhouse=to_clickhouse)
    generate_cmp_cns_by_item(conf, start_dt=start_dt, end_dt=end_dt, to_csv=to_csv, to_clickhouse=to_clickhouse)
    generate_sec_cns_by_item(conf, start_dt=start_dt, end_dt=end_dt, to_csv=to_csv, to_clickhouse=to_clickhouse)

  
if __name__ == '__main__':
    import sys, getopt
    config_file = '/home/mining/systemtrading/python_projects/wisefn_data_processing/db51.config'
    start_dt = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
    end_dt = datetime.datetime.now().strftime('%Y%m%d')
    to_csv = False
    to_clickhouse = True

    try:
        opts, args = getopt.getopt(sys.argv[1:],"hc:S:E:C:H:")
    except getopt.GetoptError:
        print(f'option error')
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print('example: main.py -c conf.config (config_file) -S 20000101 (start_dt) -E 20251231 (end_dt) -C True (to_csv) -H False (to_clickhouse)')
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

    conf = get_config(config_file)
    logging.basicConfig(level=eval(conf['Logging']['level']),
        handlers=[eval(conf['Logging']['handler'])],
        format=conf['Logging']['format'],)

    start_dt = '20201231'
    end_dt = '20210726'
    main(config_file, start_dt, end_dt, to_csv, to_clickhouse)


