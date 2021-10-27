import csv
import pandas as pd
import os
import logging
import sys
from preprocessing.config import get_config

# adapted from 01change_ITEMname.R

logger = logging.getLogger('change_item_name')


def convert_item_name_to_csv(txt_path, date):
    try:
        df = pd.read_csv(txt_path + '/' + date + '/' + 'TZ_ITEM.TXT', 
                header=None, 
                dtype='str', 
                encoding='euc-kr', 
                sep='|')
        df.loc[:, 4] = df.loc[:,4].apply(lambda x: x.strip())
        df.loc[:, 5] = df.loc[:,5].apply(
            lambda x: x.strip()
                .replace('(', '_')
                .replace(')','')
                .replace('/','')
                .replace('-','_')
                .replace('.','')
                .replace(',','')
                .replace("'",'')
                .replace(' ','_'))
        logger.info(f'{df.tail()}')
        # make the same index, columns as R
        df.index = df.index + 1
        df.columns = ['V' + str(c+1) for c in df.columns]
        # write to csv
        df.to_csv(txt_path + '/' + date + '/' + 'TZ_ITEM.csv', quoting=csv.QUOTE_ALL)
    except Exception as e:
        logger.error(f'{e}')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(filename)s %(lineno)d %(levelname)s - %(message)s')
    config_file = '/home/mining/PycharmProjects/wisefn_data_processing/db3.config'
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    conf = get_config(config_file)
    conf['Path']['txt_path']
    txt_path = conf['Path']['txt_path']

    dates = sorted(os.listdir(txt_path))
    df = None
    for date in sorted(dates)[-1:]:
        if os.path.exists(txt_path + '/' + date + '/' + 'TZ_ITEM.TXT'):
            convert_item_name_to_csv(txt_path, date)
