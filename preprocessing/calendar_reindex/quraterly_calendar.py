import os
import pandas as pd
import logging
import sys
from preprocessing.config import get_config
from logging.handlers import RotatingFileHandler
import csv

logger = logging.getLogger('quraterly_calendar_convert')


def convert_quarterly_index(conf, to_csv=True):
    path = conf['Path']['by_company_cmp_fin_path']
    source_path = path + '/' + '99QUARTERLY'
    target_path = path + '/' + '99QUARTERLY_CAL'

    files = os.listdir(source_path)
    files = [f for f in files if 'csv1' not in f and 'csv' in f]
    # files = [f for f in files if 'csv1'in f]

    for fn in sorted(files[:]):
        df = pd.read_csv(source_path + '/' + fn, dtype='str')
        df = df[(df.CAL_USE_YN == '1') & (~df.CAL_YEAR.isna() & ~df.CAL_QTR.isna())]
        df = df.drop(df.columns[0], axis=1)
        df.index = df.CAL_YEAR + '.' + df.CAL_QTR
        df.iloc[:, 22:] = df.iloc[:, 22:].astype('float')
        s = df.to_csv(None, na_rep='NA', quoting=csv.QUOTE_NONNUMERIC).replace('"NA"', 'NA')
        logger.info(f'{target_path}' + '/' + f'{fn}')
        
        if to_csv:
            with open(target_path + '/' + f'{fn}1', 'w') as file:
                file.write(s)
            # logger.debug(f'{s}')
            logger.info(f'{target_path}' + '/' + f'{fn}1')

if __name__ == '__main__':
    config_file = '/home/mining/PycharmProjects/wisefn_data_processing/db3.config'

    if len(sys.argv) > 1:
        config_file = sys.argv[1]

    conf = get_config(config_file)
    logging.basicConfig(level=eval(conf['Logging']['level']),
        handlers=[eval(conf['Logging']['handler'])],
        format=conf['Logging']['format'], 
        )

    convert_quarterly_index(conf)