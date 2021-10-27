import pandas as pd
import os
import logging

logger = logging.getLogger('preprocessing.util')

#Util_functions


def read_txt(filename):
    df = None
    if os.path.getsize(filename) > 0:
        try:
            df = pd.read_csv(filename, encoding='euc-kr', sep='|', header=None, dtype='str')
        except Exception as e:
            logger.error(f'{e}')
    return df

