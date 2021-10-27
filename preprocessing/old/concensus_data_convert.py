import csv
import datetime
import logging
import pandas as pd
import numpy as np
import os
from preprocessing.config import config
from preprocessing.paths import FIN_quarterly_path, FIN_yearly_path, filepath, by_ITEM_path, \
    SEC_filepath, by_SEC_ITEM_path, INPUTPATH, ALLINPUTPATH, MAINPATH
from preprocessing.defines import TermAcctMethod, TermAcctDir


logger = logging.getLogger('04makingDB')
logging.basicConfig(level=logging.ERROR, format='%(asctime)-15s %(filename)s %(lineno)d %(levelname)s - %(message)s')


# configuration
start_day = 0
if config['FeedALL']:
  allfolder = os.listdir(ALLINPUTPATH)
  start_day = 0
else:
  allfolder = os.listdir(INPUTPATH)
  allfolder = sorted(allfolder)
  if config['last_day_only']:
    start_day = len(allfolder) -1
  else:
    start_day = 0
if start_day > 0:
    start_day = start_day - 5 #

logger = logging.getLogger('concensus')

logging.basicConfig(level=logging.INFO)


last_day = allfolder[-2]
date_path = INPUTPATH +'/' + last_day + '/'

if not os.path.exists(date_path +"TC_SECTOR.TXT"):
    logger.error('error')
if not os.path.exists(date_path +"TC_COMPANY.TXT"):
    logger.error('error')


sector = pd.read_csv(
    date_path +"TC_SECTOR.TXT",
    encoding='euc-kr',
    names=("SEC_CD","SEC_TYP","P_SEC_CD","SEC_NM_KOR","SEC_NM_ENG","MKT_TYP"),
    sep='|',
    dtype='str')

sec_concensus = pd.read_csv(
    date_path + '/' + 'TT_SEC_CNS_DATA.TXT',
    encoding='euc-kr',
    names=("SEC_CD","CNS_DT","TERM_TYP","YYMM","ITEM_TYP","ITEM_CD","VAL"),
    sep='|',
    dtype={"SEC_CD":'str', "CNS_DT":'str', "TERM_TYP":'str', "YYMM":'str', "ITEM_TYP":'str', "ITEM_CD":'str', "VAL":'float64'})

estimate_master = pd.read_csv(
    date_path + '/' + 'TT_EST_MASTER.TXT',
    encoding='euc-kr',
    names=("CMP_CD","NO_TYP","CONS_YN","START_DT","END_DT"),
    sep='|',
    dtype='str'
)

comp_concensus = pd.read_csv(
    date_path + '/' + 'TT_CMP_CNS_DATA.TXT',
    encoding='euc-kr',
    names=("CMP_CD","CNS_DT","TERM_TYP","YYMM","ITEM_TYP","ITEM_CD","VAL"),
    sep='|',
    dtype={"CMP_CD":'str', "CNS_DT":'str', "TERM_TYP":'str', "YYMM":'str', "ITEM_TYP":'str', "ITEM_CD":'str', "VAL":'float64'})

company = pd.read_csv(
    date_path + '/' + "TC_COMPANY.TXT",
    encoding='euc-kr',
    names=("CMP_CD", "MKT_TYP", "CMP_NM_KOR", "CMP_NM_ENG", "CMP_FUL_NM_KOR", "CMP_FUL_NM_ENG",
           "BIZ_REG_NO", "CORP_REG_NO",
           "FINACC_TYP", "GICS_CD", "WI26_CD", "EST_DT", "LIST_DT", "DELIST_DT", "FYE_MN",
           "CEO", "ADDR", "ZIP_CD", "TEL", "URL", "AUDITOR", "IFRS_YYMM", "IFRS_YN", "MASTER_CHK",
           "QTR_MASTER", "LIST_YN"),
    sep='|',
    dtype='str'
)

item = pd.read_csv(
    date_path + '/' + "TZ_ITEM.csv",
    header=0,  # override header
    encoding='euc-kr',
    dtype='str',
    index_col=0)
names = ("ITEM_CD", "FINACC_TYP", "ITEM_TYP", "FS_TYP", "ITEM_NM_KOR", "ITEM_NM_ENG", "UNT_TYP")
item.rename(columns=dict(zip(item.columns[:len(names)], names)), inplace=True)
# Estimations
item = item[item['ITEM_TYP'] == "E"]

listed_comp = company.loc[company.LIST_YN == '1', ("CMP_CD","MKT_TYP","GICS_CD","WI26_CD")]

comp_columns = listed_comp.columns.append(estimate_master.columns[:2])
comp_concensus_dates =  comp_concensus['CNS_DT'].drop_duplicates().sort_values()

comp_columns = comp_columns.append(pd.Index(item["ITEM_NM_ENG"]))
comp_columns = comp_columns[comp_columns!='CMP_CD']
comp_rows = comp_concensus['YYMM'].drop_duplicates().sort_values()

empty_cns_mat = pd.DataFrame(index=comp_rows, columns=comp_columns)

for dt in comp_concensus_dates[-10:]:
    dt

def convert_com_concensus_by_company(dt):
    comp_concensus_by_acct = dict()

    for acct in TermAcctMethod:
        comp_concensus_by_acct[acct] = comp_concensus.loc[(comp_concensus.TERM_TYP == acct.value)
        & (comp_concensus.loc[:,"CNS_DT"]==dt), :]

    df = pd.DataFrame()
    for k, cns in comp_concensus_by_acct.items():
        df = pd.concat([df,cns])
    comp_codes = df.drop_duplicates(['CMP_CD'])['CMP_CD']

    cmp_cd_set = set(listed_comp.CMP_CD)
    for comp_code in comp_codes[:1]:
        mat = empty_cns_mat.copy()
        if comp_code in cmp_cd_set:
            mat.loc[:, "MKT_TYP"] = listed_comp[listed_comp.CMP_CD==comp_code]['MKT_TYP'].iloc[0]
            mat.loc[:, "GICS_CD"] = listed_comp[listed_comp.CMP_CD==comp_code]['GICS_CD'].iloc[0]
            mat.loc[:, "WI26_CD"] = listed_comp[listed_comp.CMP_CD==comp_code]['WI26_CD'].iloc[0]

        comp_estimate = estimate_master.loc[estimate_master.loc[:, "CMP_CD"] == comp_code, :]

        if any((comp_estimate["START_DT"] <= dt) & (comp_estimate["END_DT"] >= dt)):
            mat.loc[:, 'NO_TYP'] = comp_estimate.loc[(comp_estimate.START_DT <= dt) & (comp_estimate.END_DT >= dt), 'NO_TYP'].iloc[0]
            mat.loc[:, 'CONS_YN'] = comp_estimate.loc[(comp_estimate.START_DT <= dt) & (comp_estimate.END_DT >= dt), 'CONS_YN'].iloc[0]

        mat_by_acct = dict()
        for acct in TermAcctMethod:
            if os.path.exists("/home/mining/systemtrading/WiseFN_Down_DATA/DATA/01BY_COMPANY/01CMP/02CNS/" +
                              dt + "/" +
                              TermAcctDir[acct.name].value  +
                              "/" + "A" + comp_code +".csv"):
                mat = pd.read_csv("/home/mining/systemtrading/WiseFN_Down_DATA/DATA/01BY_COMPANY/01CMP/02CNS/" +
                              dt + "/" +
                              TermAcctDir[acct.name].value  +
                              "/" + "A" + comp_code +".csv",
                            dtype='str'
                            )
            mat_by_acct[acct] = mat

            one_comp_concensus = comp_concensus_by_acct[acct].loc[comp_concensus_by_acct[acct].loc[:, 'CMP_CD'] == comp_code, :]

            if len(one_comp_concensus) > 0:
                item_list = one_comp_concensus.loc[:, 'ITEM_CD']
                for itm in item_list:
                    item_data = one_comp_concensus.loc[one_comp_concensus.loc[:, 'ITEM_CD']==itm, :].copy()
                    item_name = item.loc[item.loc[:, 'ITEM_CD']==itm, 'ITEM_NM_ENG']
                    if item.loc[item.loc[:, 'ITEM_CD'] == itm, 'UNT_TYP'].unique() == "천원":
                        item_data.loc[:, 'VAL'] = 1000 * item_data.loc[:, 'VAL']

                    for idx in item_data.index:
                        date_ = item_data.loc[idx, "YYMM"]
                        if date_ not in mat_by_acct[acct].index:
                            mat_by_acct[acct].loc[date_, :] = np.nan
                        mat_by_acct[acct].loc[mat_by_acct[acct].index==date_, item_name] = item_data.loc[idx, "VAL"]

            if len(mat_by_acct[acct]) != 1:
                mat_by_acct[acct] = mat_by_acct[acct].dropna(how='all', axis='rows', subset=mat_by_acct[acct].columns[4:])
            if not os.path.exists('/home/mining/systemtrading/WiseFN_Down_DATA/DATA/01BY_COMPANY/01CMP/02CNS/'):
                p = '/home/mining/systemtrading/WiseFN_Down_DATA/DATA/01BY_COMPANY/01CMP/02CNS/'
                for acct in TermAcctDir:
                    print(p + acct.value)
                    # os.makedirs(p + acct.value)
            for acct in TermAcctMethod:
                print(
                    filepath + dt + "/" + TermAcctDir[acct.name].value + "/"
                    + "A" + comp_code + ".csv")
                # mat_by_acct[acct].to_csv(
                #     filepath + dt + "/" + AcctDir[acct.name].value + "/"
                #     + "A" +  comp_code + ".csv")

convert_com_concensus_by_company(dt)


def convert_com_concensus_by_item(dt):
    ITEMCODE_NEED_2 = ("113100", "113120", "113240", "113250", "121000", "121500", "122500", "122700", "122710", "211500",
                        "312000", "314000", "331000", "382100", "382500", "423900")

    ITEMNAME_NEED_2 = ("01Short_Term_Trading_Financial_Assets", "02Short_Term_Financial_Instruments",
                        "03Short_Term_Available_for_Sale_Financial_Assets",
                        "04Short_Term_Held_to_Maturity_Investments", "05Net_Sales", "06Operating_Profit",
                        "07Pre_tax_Profit_from_Continuing_Operations",
                        "08Net_Profit", "09Net_Profit(Owners)", "10ROE(Owners)", "11EPS(Owners)", "12BPS(Owners)",
                        "13EV_EBITDA",
                        "14P_E", "15P_B", "16DPS(Adj_Comm_Cash_FY_End)")
    comp_concensus_by_acct = dict()
    for acct in TermAcctMethod:
        comp_concensus_by_acct[acct] = comp_concensus.loc[(comp_concensus.TERM_TYP == acct.value)
                                                          & (comp_concensus.loc[:, "CNS_DT"] == dt), :]
    for idx in range(len(ITEMNAME_NEED_2)):
        #FIXME
        # idx = 5
        item_code = ITEMCODE_NEED_2[idx]
        item_name = ITEMNAME_NEED_2[idx]
        mat_by_acct = dict()
        for acct in TermAcctMethod:
            empty_mat = pd.DataFrame(index=comp_rows, columns='A' + comp_concensus_by_acct[acct]['CMP_CD'].drop_duplicates())
            mat = empty_mat.copy()
            one_item_concensus = comp_concensus_by_acct[acct].loc[comp_concensus_by_acct[acct]["ITEM_CD"] == item_code, :]
            if os.path.exists(
                    "/home/mining/systemtrading/WiseFN_Down_DATA/DATA/02BY_ITEM/03CNSITEM/" +
                    dt + "/" + TermAcctDir[acct.name].value + "/" + item_name + ".csv"):
                mat = pd.read_csv(
                    "/home/mining/systemtrading/WiseFN_Down_DATA/DATA/02BY_ITEM/03CNSITEM/" +
                    dt + "/" + TermAcctDir[acct.name].value + "/" + item_name + ".csv"
                )
            mat_by_acct[acct] = mat

            if len(one_item_concensus) == 0:
                continue
            date_list = one_item_concensus.loc[:, "YYMM"].drop_duplicates()
            for dtx in date_list:
                date_data = one_item_concensus.loc[one_item_concensus.loc[:, "YYMM"] == dtx, :]
                comp_codes = date_data.loc[:, "CMP_CD"]
                VAL = date_data.loc[:, "VAL"]
                print(item.loc[item.loc[:, 'ITEM_CD'] == item_code, "UNT_TYP"].drop_duplicates())
                if (item.loc[item.loc[:, 'ITEM_CD'] == item_code, "UNT_TYP"].drop_duplicates() == "천원").all():
                    values = 1000 * VAL

                for comp_code in comp_codes:
                    if 'A' + comp_code not in mat_by_acct[acct].columns:
                        logger.info(f'{comp_code} added')
                        mat_by_acct[acct].loc[:, comp_code] = np.nan

                if dtx in mat_by_acct[acct].index:
                    logger.info(f'{dtx} added')
                    mat_by_acct[acct].loc[dtx, :] = np.nan

                mat_by_acct[acct].loc[dtx, "A"+ comp_codes] = values

            if acct in mat_by_acct:
                mat_by_acct[acct] = mat_by_acct[acct].dropna(how='all', axis='rows')
                mat_by_acct[acct] = mat_by_acct[acct].dropna(how='all', axis='columns')

            p = '/home/mining/systemtrading/WiseFN_Down_DATA/DATA/02BY_ITEM/03CNSITEM/'
            if not os.path.exists(p):
                for acct in TermAcctDir:
                    print(p + acct.value)
                    # os.makedirs(p + acct.value)
            for acct in TermAcctMethod:
                print(
                    by_ITEM_path + dt + "/" + TermAcctDir[acct.name].value + "/"
                    + "A" + comp_code + ".csv")
                # mat_by_acct[acct].to_csv(
                #     by_ITEM_path + dt + "/" + AcctDir[acct.name].value + "/"
                #     + "A" +  comp_code + ".csv")


convert_com_concensus_by_item(dt)

# SECTOR information
sector = sector.loc[sector.loc[:, "SEC_TYP"] == "W", :]
sec_info = sector.loc[:, ("SEC_CD", "MKT_TYP")]
sec_columns = sec_info.columns.append(pd.Index(item.loc[:,'ITEM_NM_ENG']))
sec_columns = sec_columns.drop_duplicates()
sec_rows = sec_concensus.loc[:, "YYMM"].drop_duplicates().sort_values()

empty_cns_mat = pd.DataFrame(index=sec_rows, columns=sec_columns)
sec_concensus.loc[:, 'CNS_DT'].drop_duplicates()
sec_concensus_dates = sec_concensus['CNS_DT'].drop_duplicates().sort_values()
for dt in sec_concensus_dates:
    print(dt)

def convert_sec_concensus_by_sector(dt):
    sec_concensus_by_acct = dict()
    for acct in TermAcctMethod:
        sec_concensus_by_acct[acct] = sec_concensus.loc[(sec_concensus.TERM_TYP == acct.value)
                                                          & (sec_concensus.loc[:, "CNS_DT"] == dt), :]
    for idx in sec_info.index:
        mat = empty_cns_mat.copy()
        sec_code = sec_info.loc[idx, 'SEC_CD']
        mat.loc[:, "SEC_CD"] = sec_code
        mat.loc[:, "MKT_TYP"] = sec_info.loc[idx, "MKT_TYP"]

        mat_by_acct = dict()

        for acct in TermAcctMethod:
            #TODO FIX
            acct = TermAcctMethod.SEP_NET
            one_sec_concensus = sec_concensus_by_acct[acct].loc[sec_concensus_by_acct[acct].loc[:, "SEC_CD"] == sec_code, :]
            mat = empty_cns_mat.copy()
            if os.path.exists("/home/mining/systemtrading/WiseFN_Down_DATA/DATA/01BY_COMPANY/02SEC/02CNS/"
                    + dt + "/" + TermAcctDir[acct.name].value + "/" + sec_code+ ".csv"):
                mat = pd.read_csv("/home/mining/systemtrading/WiseFN_Down_DATA/DATA/01BY_COMPANY/02SEC/02CNS/"
                    + dt + "/" + TermAcctDir[acct.name].value + "/" + sec_code+ ".csv", dtype='str')
            mat_by_acct[acct] = mat

            if len(one_sec_concensus) == 0:
                continue
            item_list = one_sec_concensus.loc[:, "ITEM_CD"].drop_duplicates()
            for itm in item_list:
                item_data = one_sec_concensus.loc[one_sec_concensus.loc[:, "ITEM_CD"] == itm, :].copy()
                item_name = item.loc[item.loc[:, 'ITEM_CD'] == itm, "ITEM_NM_ENG"]
                if (item.loc[item.loc[:, "ITEM_CD"] == itm, "UNT_TYP"] == "천원").all():
                    item_data.loc[:, "VAL"] = 1000 * item_data.loc[:, "VAL"].astype('float64')

                for idi in item_data.index:
                    date_ = item_data.loc[idi, "YYMM"]
                    # TODO
                    date_ = '202003'

                    if date_ not in mat_by_acct[acct].index:
                        logger.info(f'{date_} added')
                        mat_by_acct[acct].loc[date_, :] = np.nan
                    mat_by_acct[acct].loc[date_, item_name] = item_data.loc[idi,"VAL"]

        if len(mat_by_acct[acct]) > 0:
            mat_by_acct[acct].dropna(how='all', subset=mat_by_acct[acct].columns[2:], axis='rows', inplace=True)

        p = "/home/mining/systemtrading/WiseFN_Down_DATA/DATA/01BY_COMPANY/02SEC/02CNS/"
        if not os.path.exists(p + dt):
            for acct in TermAcctDir:
                print(p + dt + '/' + acct.value)
                # os.makedirs(p + dt + '/' + acct.value)

        for acct in TermAcctMethod:
            if acct in mat_by_acct and len(mat_by_acct[acct]) > 0:
                print(SEC_filepath + dt + "/" + TermAcctDir[acct.name].value + "/" +  sec_code + ".csv")
                # mat_by_acct[acct].to_csv(SEC_filepath + dt + "/" + AcctDir[acct.name].value + "/" +  sec_code + ".csv", dtyhpe='str')


dt = '20210226'
convert_sec_concensus_by_sector(dt)


sector = sector.loc[sector.loc[:, "SEC_TYP"] == "W", :]
sec_info = sector.loc[:, ("SEC_CD", "MKT_TYP")]
sec_columns = sec_info.columns.append(pd.Index(item.loc[:,'ITEM_NM_ENG']))
sec_columns = sec_columns.drop_duplicates()
sec_rows = sec_concensus.loc[:, "YYMM"].drop_duplicates().sort_values()

ITEMCODE_NEED_2 = ("113100", "113120", "113240", "113250", "121000", "121500", "122500", "122700", "122710", "211500",
                   "312000", "314000", "331000", "382100", "382500", "423900")
ITEMNAME_NEED_2 = ("01Short_Term_Trading_Financial_Assets", "02Short_Term_Financial_Instruments",
                   "03Short_Term_Available_for_Sale_Financial_Assets",
                   "04Short_Term_Held_to_Maturity_Investments", "05Net_Sales", "06Operating_Profit",
                   "07Pre_tax_Profit_from_Continuing_Operations",
                   "08Net_Profit", "09Net_Profit(Owners)", "10ROE(Owners)", "11EPS(Owners)", "12BPS(Owners)",
                   "13EV_EBITDA",
                   "14P_E", "15P_B", "16DPS(Adj_Comm_Cash_FY_End)")

dates =  sec_concensus.loc[:, "CNS_DT"].drop_duplicates().sort_values()
dt = '20210226'

def convert_sec_concensus_by_item(dt):
    sec_concensus_by_acct = dict()
    for acct in TermAcctMethod:
        sec_concensus_by_acct[acct] = sec_concensus.loc[(sec_concensus.TERM_TYP == acct.value)
                                                        & (sec_concensus.loc[:, "CNS_DT"] == dt), :]

    for idx in range(len(ITEMNAME_NEED_2)):
        #TODO
        idx=5
        item_code = ITEMCODE_NEED_2[idx]
        item_name = ITEMNAME_NEED_2[idx]

        mat_by_acct = dict()
        for acct in [TermAcctMethod.SEP_NET]:
            sec_columns = sec_concensus_by_acct[acct].loc[:, "SEC_CD"].drop_duplicates().sort_values()
            empty_mat = pd.DataFrame(index=sec_rows, columns=sec_columns)
            one_item_concensus = sec_concensus_by_acct[acct].loc[sec_concensus_by_acct[acct].loc[:, 'ITEM_CD'] == item_code, :]

            if len(one_item_concensus) == 0:
                continue
            mat = empty_mat.copy()
            date_list = one_item_concensus.loc[:, "YYMM"].drop_duplicates().sort_values()

            print("/home/mining/systemtrading/WiseFN_Down_DATA/DATA/02BY_ITEM/04CNSSECITEM/" + dt + "/" + TermAcctDir[
                acct.name].value + "/" + item_name + ".csv")
            if os.path.exists(
                    "/home/mining/systemtrading/WiseFN_Down_DATA/DATA/02BY_ITEM/04CNSSECITEM/"
                    + dt + "/" +  TermAcctDir[acct.name].value + "/" + item_name + ".csv"):
                mat = pd.read_csv(
                    "/home/mining/systemtrading/WiseFN_Down_DATA/DATA/02BY_ITEM/04CNSSECITEM/"
                    + dt + "/" + TermAcctDir[acct.name].value + "/" + item_name + ".csv", dtype='str')
            mat_by_acct[acct] = mat

            for date_ in date_list:
                #TODO
                # date_='201912'
                date_data = one_item_concensus.loc[one_item_concensus.loc[:, "YYMM"]==date_, :]
                sec_codes = date_data.loc[:, "SEC_CD"]
                values = date_data.loc[:, "VAL"]

                if (item.loc[item.loc[:, 'ITEM_CD'] == item_code, "UNT_TYP"] == "천원").all():
                    values = 1000 * values

                # Find if there is new SEC_code has appeared
                for sec_code in sec_codes:
                    if sec_code not in mat_by_acct[acct].columns:
                        logger.info(f'{sec_code} added')
                        mat_by_acct[acct].loc[:, sec_code] = np.nan
                if date_ not in mat_by_acct[acct].index:
                    mat_by_acct[acct].loc[date_,:] = np.nan

                mat_by_acct[acct].loc[date_, sec_codes] = values

            if acct in mat_by_acct:
                mat_by_acct[acct].dropna(how='all', axis='rows', inplace=True)
                mat_by_acct[acct].dropna(how='all', axis='columns', inplace=True)

        if not os.path.exists(by_SEC_ITEM_path + '/' + dt):
            os.makedirs(by_SEC_ITEM_path + '/' + dt)
            for acctd in TermAcctDir:
                print(by_SEC_ITEM_path + '/' + dt + acctd.value)
                # os.makedirs(by_SEC_ITEM_path + '/' + dt + acctd.value)
        for acctm in TermAcctMethod:
            print(
                by_SEC_ITEM_path + dt + "/" + TermAcctDir[acctm.name].value + "/"
                + "A" + item_name + ".csv")
            # mat_by_acct[acct].to_csv(
            #     by_SEC_ITEM_path + dt + "/" + AcctDir[acctm.name].value + "/"
            #     + "A" +  comp_code + ".csv")

dt = '20210226'
convert_sec_concensus_by_item(dt)