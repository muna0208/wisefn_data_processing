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
from preprocessing.annual.cmp_consensus import generate_annual_cmp_cns, generate_annual_cmp_cns_ch
from preprocessing.annual.cmp_finanacial import generate_annual_cmp_fin, generate_annual_cmp_fin_ch
from preprocessing.annual.sec_consensus import generate_annual_sec_cns, generate_annual_sec_cns_ch
from preprocessing.annual.sec_finanacial import generate_annual_sec_fin, generate_annual_sec_fin_ch
from preprocessing.annual.all_items.cmp_financial import generate_annual_cmp_fin_all, generate_annual_cmp_fin_all_ch
from preprocessing.annual.all_items.sec_financial import generate_annual_sec_fin_all, generate_annual_sec_fin_all_ch

def main(config_file, start_dt, end_dt, to_csv, to_clickhouse):
    import multiprocessing as mp
    conf = get_config(config_file)
    conf['ClickHouse']['database'] ='financial'
    conf['MariaDB']['db_name'] = 'wisefn'
    
    # generate_annual_cmp_cns(conf, start_dt=start_dt, end_dt=end_dt, to_csv=to_csv, to_clickhouse=to_clickhouse)
    # generate_annual_cmp_fin(conf, to_csv=to_csv, to_clickhouse=to_clickhouse)
    # generate_annual_sec_cns(conf, start_dt=start_dt, end_dt=end_dt, to_csv=to_csv, to_clickhouse=to_clickhouse)
    # generate_annual_sec_fin(conf, to_csv=to_csv, to_clickhouse=to_clickhouse)
    # generate_annual_cmp_fin_all(conf, to_csv=to_csv, to_clickhouse=to_clickhouse)
    # generate_annual_sec_fin_all(conf, to_csv=to_csv, to_clickhouse=to_clickhouse)

    # proc_list = []
    # p = mp.Process(target=generate_annual_cmp_cns_ch, args=(conf,), kwargs={'start_dt':start_dt, 'end_dt':end_dt, 'to_clickhouse':to_clickhouse})
    # proc_list += [p]
    # p.start()

    # p = mp.Process(target=generate_annual_cmp_fin_ch, args=(conf,), kwargs={'start_dt':start_dt, 'end_dt':end_dt,'to_clickhouse':to_clickhouse})
    # proc_list += [p]
    # p.start()

    # p = mp.Process(target=generate_annual_sec_cns_ch, args=(conf,), kwargs={'to_clickhouse':to_clickhouse})
    # proc_list += [p]
    # p.start()

    # p = mp.Process(target=generate_annual_sec_fin_ch, args=(conf,), kwargs={'to_clickhouse':to_clickhouse})
    # proc_list += [p]
    # p.start()

    # p = mp.Process(target=generate_annual_cmp_fin_all_ch, args=(conf,), kwargs={'start_dt':start_dt, 'end_dt':end_dt, 'to_clickhouse':to_clickhouse})
    # proc_list += [p]
    # p.start()

    # p = mp.Process(target=generate_annual_sec_fin_all_ch, args=(conf,), kwargs={'to_clickhouse':to_clickhouse})
    # proc_list += [p]
    # p.start()

    # for e in proc_list:
    #     e.join()

    generate_annual_cmp_cns_ch(conf, start_dt=start_dt, end_dt=end_dt, to_clickhouse=to_clickhouse)
    generate_annual_cmp_fin_ch(conf, start_dt=start_dt, end_dt=end_dt, to_clickhouse=to_clickhouse)
    generate_annual_sec_cns_ch(conf, to_clickhouse=to_clickhouse)
    generate_annual_sec_fin_ch(conf, to_clickhouse=to_clickhouse)
    generate_annual_cmp_fin_all_ch(conf, start_dt=start_dt, end_dt=end_dt, to_clickhouse=to_clickhouse)
    generate_annual_sec_fin_all_ch(conf, to_clickhouse=to_clickhouse)



if __name__ == '__main__':
    import sys, getopt
    config_file = '/home/mining/PycharmProjects/wisefn_data_processing/db3.config'
    start_dt = '20000101'
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

    # start_dt = '20000101'
    # end_dt = '99990101'

    main(config_file, start_dt, end_dt, to_csv, to_clickhouse)

