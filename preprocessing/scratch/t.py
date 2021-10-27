import os
import csv
import datetime
import logging
import pandas as pd
import numpy as np
import os
import sys

from preprocessing.config import config
from preprocessing.paths import FIN_quarterly_path, FIN_yearly_path, filepath, by_ITEM_path, \
    SEC_filepath, by_SEC_ITEM_path, INPUTPATH, ALLINPUTPATH, MAINPATH


with open(MAINPATH + '/'+ '02ALL' +'/' + 'TF_CMP_FINDATA.TXT', 'r') as f:
    for line in f.readlines():
        if '263020' in line and '122700' in line and '201806' in line:
            print(ALLINPUTPATH + '/'+ '02ALL' + '/' + 'TF_CMP_FINDATA.TXT', line)


dirs = os.listdir(ALLINPUTPATH)
for d in [e for e in dirs if len(e)==4]:
    if os.path.exists(ALLINPUTPATH + '/'+d +'/' + 'TF_CMP_FINDATA.TXT'):
        with open(ALLINPUTPATH + '/'+d +'/' + 'TF_CMP_FINDATA.TXT', 'r') as f:
            for line in f.readlines():
                if '263020' in line and '122700' in line and '201806' in line:
                    print(ALLINPUTPATH + '/'+d + '/' + 'TF_CMP_FINDATA.TXT', line)


dirs = os.listdir(INPUTPATH)
for d in dirs:
    if os.path.exists(INPUTPATH + '/'+d +'/' + 'TF_CMP_FINDATA.TXT'):
        with open(INPUTPATH + '/'+d +'/' + 'TF_CMP_FINDATA.TXT', 'r') as f:
            for line in f.readlines():
                if '263020' in line and '122700' in line and '201806' in line:
                    print(INPUTPATH + '/'+d + '/' + 'TF_CMP_FINDATA.TXT', line)