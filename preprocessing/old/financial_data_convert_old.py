import csv
import datetime
import logging
import pandas as pd
import numpy as np
import os
import sys

# sys.path.append('/home/mining/PycharmProjects/fnguide_data_processing')

from preprocessing.config import config
from preprocessing.paths import FIN_quarterly_path, FIN_yearly_path, filepath, by_ITEM_path, \
    SEC_filepath, by_SEC_ITEM_path, INPUTPATH, ALLINPUTPATH, MAINPATH
from preprocessing.defines import TermAcctMethod, TermAcctDir, FinAcctType, ITEMNAME_NEED_2, ITEMCODE_NEED_2


logger = logging.getLogger('02makingDB')


# Use original_fields only
if config['use_original_fields']:
    ORIGINAL_FIELDS = pd.read_csv(MAINPATH+"original_fields.csv", dtype='str', index_col=0)


def get_company_financial_data(input_path, dates):
    data_list = []
    for dt in dates:
        logger.info(f'reading {dt}')
        # if there is not enough # of files.
        if len(os.listdir(input_path + '/' + dt)) < 9:
            logger.warning(f'{dt} is empty')
            continue
        try:
            d = pd.read_csv(
                input_path + '/' + dt + '/' + 'TF_CMP_FINDATA.TXT',
                names=("CMP_CD", "TERM_TYP", "YYMM", "ITEM_TYP", "ITEM_CD", "VAL"),
                sep='|',
                dtype={"CMP_CD":'str', "TERM_TYP":'str', "YYMM":'str', "ITEM_TYP":'str', "ITEM_CD":'str', "VAL":'float64'}
            )
        except Exception as e:
            logger.error(f'{e}')            
        d.columns = ("CMP_CD", "TERM_TYP", "YYMM", "ITEM_TYP", "ITEM_CD", "VAL")
        data_list += [d]

    result = pd.concat(data_list)
    result = result.drop_duplicates(
        subset=["CMP_CD", "TERM_TYP", "YYMM", "ITEM_TYP", "ITEM_CD"],
        keep='last')
    return result


def get_company_info(input_path, dates):
    data_list = []
    for dt in dates:
        if not os.path.exists(input_path + '/' + dt +'/' + "TC_COMPANY.TXT"):
            logger.warning(f"path not found: {input_path + '/' + dt +'/' + 'TC_COMPANY.TXT'}")
            continue
        try:
            d = pd.read_csv(
                input_path + '/' + dt +'/' + "TC_COMPANY.TXT",
                names=("CMP_CD", "MKT_TYP", "CMP_NM_KOR", "CMP_NM_ENG", "CMP_FUL_NM_KOR", "CMP_FUL_NM_ENG",
                    "BIZ_REG_NO", "CORP_REG_NO",
                    "FINACC_TYP", "GICS_CD", "WI26_CD", "EST_DT", "LIST_DT", "DELIST_DT", "FYE_MN",
                    "CEO", "ADDR", "ZIP_CD", "TEL", "URL", "AUDITOR", "IFRS_YYMM", "IFRS_YN", "MASTER_CHK",
                    "QTR_MASTER", "LIST_YN"),
                sep='|',
                encoding='euc-kr',
                dtype='str'
            )
        except Exception as e:
            logger.error(f'{e}')
        d['DATE'] = dt
        data_list += [d]

    result = pd.concat(data_list)
    return result


def get_daily_data(data_path, date_):
    input_data = dict()
    input_path = data_path + '/' + date_
    TC_SECTOR = pd.read_csv(
        input_path + '/' + "TC_SECTOR.TXT",
        names=("SEC_CD", "SEC_TYP", "P_SEC_CD", "SEC_NM_KOR", "SEC_NM_ENG", "MKT_TYP"),
        sep='|',
        encoding='euc-kr',
        dtype='str'
    )
    input_data['TC_SECTOR'] = TC_SECTOR

    TF_SEC_FINDATA = pd.read_csv(
        input_path + '/' + "TF_SEC_FINDATA.TXT",
        names=("SEC_CD", "TERM_TYP", "YYMM", "ITEM_TYP", "ITEM_CD", "VAL"),
        sep='|',
        dtype={"SEC_CD":'str', "TERM_TYP":'str', "YYMM":'str', "ITEM_TYP":'str', "ITEM_CD":'str', "VAL":'float64'}
    )
    input_data['TF_SEC_FINDATA'] = TF_SEC_FINDATA

    TC_COMPANY = pd.read_csv(
        input_path + '/' + "TC_COMPANY.TXT",
        names=("CMP_CD", "MKT_TYP", "CMP_NM_KOR", "CMP_NM_ENG", "CMP_FUL_NM_KOR", "CMP_FUL_NM_ENG",
               "BIZ_REG_NO", "CORP_REG_NO",
               "FINACC_TYP", "GICS_CD", "WI26_CD", "EST_DT", "LIST_DT", "DELIST_DT", "FYE_MN",
               "CEO", "ADDR", "ZIP_CD", "TEL", "URL", "AUDITOR", "IFRS_YYMM", "IFRS_YN", "MASTER_CHK",
               "QTR_MASTER", "LIST_YN"),
        sep='|',
        encoding='euc-kr',
        dtype='str'
    )
    input_data['TC_COMPANY'] = TC_COMPANY

    TF_CMP_FINDATA_INFO = pd.read_csv(
        input_path + '/' + "TF_CMP_FINDATA_INFO.TXT",
        names=("CMP_CD", "YYMM", "TERM_TYP", "START_DT", "END_DT", "DATA_EXIST_YN", "FST_PUB_DT",
               "PF_DT", "LAA_YN", "LAQ_YN", "SEQ"),
        sep='|',
        dtype='str'
    )
    input_data['TF_CMP_FINDATA_INFO'] = TF_CMP_FINDATA_INFO

    TF_CMP_FINPRD = pd.read_csv(
        input_path + '/' + "TF_CMP_FINPRD.TXT",
        names=("CMP_CD", "YYMM", "CAL_YEAR", "CAL_QTR", "CAL_USE_YN", "FS_YEAR", "FS_QTR", "FS_USE_YN",
               "IFRS_CHK", "MASTER_CHK", "QTR_MASTER"),
        sep='|',
        dtype='str'
    )
    input_data['TF_CMP_FINPRD'] = TF_CMP_FINPRD

    TZ_ITEM = pd.read_csv(
        input_path + '/' + "TZ_ITEM.csv",
        header=0, # override header
        dtype='str',
        index_col=0)
    names = ("ITEM_CD", "FINACC_TYP", "ITEM_TYP", "FS_TYP", "ITEM_NM_KOR", "ITEM_NM_ENG", "UNT_TYP")
    ren = dict(zip(TZ_ITEM.columns[:len(names)],names))
    TZ_ITEM.rename(columns=ren, inplace=True)
    input_data['TZ_ITEM'] = TZ_ITEM
    return input_data


def convert_comp_fin_data_by_company(comp_code, daily_data, comp_fin_data, comp_info):
    # company code, market, sector
    comp = daily_data['TC_COMPANY']
    # get listed company info
    listed_comp = comp.loc[comp['LIST_YN'] == '1', ["CMP_CD", "MKT_TYP", "GICS_CD", "WI26_CD"]]
    # for each accounting method
    if comp_code not in set(listed_comp['CMP_CD']):  # is not listed?
        logger.info(f'{comp_code} is not listed')
        return None
    fin_info = daily_data['TF_CMP_FINDATA_INFO']
    # company financial info
    fin_info_by_acct = dict()
    for acct in TermAcctMethod:
        fin_info_by_acct[acct] = fin_info.loc[fin_info.loc[:, "TERM_TYP"] == acct.value, :]
    # company info + financial periods + financial info
    comp_columns = listed_comp.columns.append(
        daily_data['TF_CMP_FINPRD'].columns).append(
        daily_data['TF_CMP_FINDATA_INFO'].columns)
    
    # CMPFINDATA -> TZ_ITEM_M -> ORIGINAL_FIELDS[which(ORIGINAL_FIELDS[,3]=="A"|ORIGINAL_FIELDS[,3]=="M"),]
    if config['use_original_fields']:
        comp_columns = comp_columns.append(
            pd.Index(ORIGINAL_FIELDS.loc[(ORIGINAL_FIELDS.iloc[:, 2] == "A")
                     | (ORIGINAL_FIELDS.iloc[:, 2] == "M")].iloc[:, 5]))

    comp_rows = comp_fin_data["YYMM"].drop_duplicates()

    fin_data_by_acct = dict()
    for acct in TermAcctMethod:
        fin_data_by_acct[acct] = comp_fin_data.loc[comp_fin_data.loc[:, "TERM_TYP"] == acct.value, :]

    #FINDATA_MAT#
    comp_columns = comp_columns.drop_duplicates()
    comp_columns = comp_columns[comp_columns != "YYMM"]

    empty_matrix = pd.DataFrame(index=comp_rows, columns=comp_columns)
    comp = daily_data['TC_COMPANY']
    fin_acct_type = comp.loc[comp.loc[:, "CMP_CD"] == comp_code, "FINACC_TYP"]
    if len(fin_acct_type) == 0:  # when financial account type is not found
        logger.error(f'financial account type for company {comp_code} not found')
        return None

    mat_by_acct = dict()
    for k in TermAcctMethod:
        # k = AcctMethod.SEP_CUM
        logger.info(filepath + TermAcctDir[k.name].value + "/" + "A" + comp_code + ".csv")
        mat = empty_matrix.copy()
        if os.path.exists(filepath + TermAcctDir[k.name].value + "/" + "A" + comp_code + ".csv"):
            mat = pd.read_csv(filepath + TermAcctDir[k.name].value + "/" + "A" + comp_code + ".csv", 
                    index_col=0, 
                    dtype='str')
            mat.index = mat.index.astype('str')
            cols = ['Short_Term_Trading_Financial_Assets',
                'Short_Term_Financial_Instruments',
                'Short_Term_Available_for_Sale_Financial_Assets',
                'Short_Term_Held_to_Maturity__Investments',
                'Financial_Assets_Measured_at_Fair_Value_Through_Profit_or_Loss',
                'Financial_Assets_Measured_at_Fair_Value_Through_Other_Comprehensive_Income',
                'Securities_at_Amortised_Cost', 'Financial_Assets_at_Amortised_Cost',
                'Total_Assets', 'Current_Assets', 'Cash_and_Cash_Equivalents',
                'Non_current_Assets', 'Total_Liabilities', 'Current_Liabilities',
                'Interest_barings_Debt', 'Net_Debt', 'Total_Stockholders_Equity',
                'Paid_in_Capital_Preferred', 'Sales', 'Gross_Profit',
                'Operating_Profit', 'Pre_tax_Profit_from_Continuing_Operations',
                'Income_Taxes', 'Net_Profit', 'Net_Profit_Owners_of_Parent_Equity',
                'EBIT', 'EBITDA2', 'Free_Cash_Flow2', 'Cash_Flows_from_Operatings',
                'Depreciation_Cash_Flow', 'Operating_ProfitSales', 'Gross_Margin',
                'ROE_Owners_of_Parent_Equity', 'EV', 'Total_DebtEquity',
                'Sales_Growth_YoY', 'Operating_Profit_Growth_YoY',
                'Net_Profit_Growth_YoY', 'EPS_Owners_of_Parent_Equity_Adj',
                'BPS_Owners_of_Parent_Equity_Adj', 'EVEBITDA2', 'PE_FY_End',
                'PE_Adj_FY_End', 'PB_FY_End', 'PB_Adj_FY_End',
                'DPS_Adj_Comm_Cash_FY_End', 'DPS_Adj_Comm_Cash',
                'Cash_Dividend_Comm_FY_End', 'Cash_Dividend_Comm',
                'Common_Shares_FY_End', 'Shares_All_FY_End']
            mat.loc[:,cols] = mat.loc[:,cols].astype('float')
        if len(mat) == 0:
            mat = empty_matrix.copy()

        # set company info in mat
        # MKT_TYP might change over time. e.g. Cacao
        one_comp_info = comp_info.loc[comp_info.loc[:, 'CMP_CD'] == comp_code]
        one_comp_info = one_comp_info.set_index(one_comp_info.DATE.apply(lambda x: datetime.datetime.strptime(x, '%Y%m%d')))
        one_comp_info = one_comp_info.resample('1Q').last()
        one_comp_info.index = pd.Series(one_comp_info.index).apply(lambda x: datetime.datetime.strftime(x, '%Y%m'))
        indices = one_comp_info.index.intersection(mat.index)
        if len(indices) > 0:
            mat.loc[indices, "MKT_TYP"] = one_comp_info.loc[indices, "MKT_TYP"]
            mat.loc[indices, "GICS_CD"] = one_comp_info.loc[indices, "GICS_CD"]
            mat.loc[indices, "WI26_CD"] = one_comp_info.loc[indices, "WI26_CD"]
            mat.loc[:, ['MKT_TYP','GICS_CD','WI26_CD']] = mat.loc[:, ['MKT_TYP','GICS_CD','WI26_CD']].bfill()

        # set financial periods in mat
        comp_fin_prd = daily_data['TF_CMP_FINPRD']
        one_comp_prd = comp_fin_prd.loc[comp_fin_prd["CMP_CD"] == comp_code, :]

        if len(one_comp_prd) == 0:
            logger.error(f'financial periods not available for {comp_code}')
            continue
        for idx in one_comp_prd.index:
            dt = one_comp_prd.loc[idx, "YYMM"]
            if dt not in mat.index:
                logger.info(f'financial periods not available for code {comp_code}, date {dt} added')
                logger.info(f'date {dt} added for {comp_code}')
                mat.loc[dt, :] = np.nan
            # copy periods info to matrix
            mat.loc[dt, one_comp_prd.columns[one_comp_prd.columns != "YYMM"]] = \
                one_comp_prd.loc[idx, one_comp_prd.columns[one_comp_prd.columns != "YYMM"]]

        # CMP_FINDATA_INFO from here, need to sort the term type
        mat_by_acct[k] = mat
        one_comp_fin_info = fin_info_by_acct[k].loc[fin_info_by_acct[k].loc[:, "CMP_CD"] == comp_code]

        # set financial info to mat
        if len(one_comp_fin_info) > 0:
            for idx in one_comp_fin_info.index:
                date_ = one_comp_fin_info.loc[idx, "YYMM"]
                ###FST_PUB_DT error check###
                if not pd.isna(one_comp_fin_info.loc[idx, "FST_PUB_DT"]):
                    pub = datetime.datetime.strptime(one_comp_fin_info.loc[idx, "FST_PUB_DT"], '%Y%m%d')
                    end = datetime.datetime.strptime(one_comp_fin_info.loc[idx, "END_DT"], '%Y%m%d')
                    if pub - end > datetime.timedelta(days=360):
                        one_comp_fin_info.at[idx, "FST_PUB_DT"] = np.nan
                        logger.warning(f'{comp_code} pub date {pub}, end date {end} difference is more than 360 days')
                # copy financial periods to mat
                mat_by_acct[k].loc[date_, one_comp_fin_info.columns[one_comp_fin_info.columns != "YYMM"]] = \
                    one_comp_fin_info.loc[idx, one_comp_fin_info.columns[one_comp_fin_info.columns != "YYMM"]]

        # set financial data to mat (CMP_FINDATA join TZ_ITEM)
        one_comp_fin_data = fin_data_by_acct[k].loc[fin_data_by_acct[k].loc[:, "CMP_CD"] == comp_code, :]
        items = daily_data['TZ_ITEM']
        # FINACC_TYP '1' for manufacturing industry, '0' is for all industry
        items_for_finacc = items.loc[
                          (items.loc[:, "FINACC_TYP"] == fin_acct_type.iloc[0]) |
                          (items.loc[:, "FINACC_TYP"] == FinAcctType.COMMON.value), :]
        # FIXME ?? use ITEM_TYP "M" only util "A" is proven to be acceptable
        if len(one_comp_fin_data) > 0:
            one_comp_items = one_comp_fin_data.drop_duplicates(
                ['ITEM_CD', 'ITEM_TYP'])[['ITEM_CD', 'ITEM_TYP']].reset_index(drop=True)
            for idx in one_comp_items.index:
                item_idx = items_for_finacc.index[
                    (items_for_finacc.loc[:, "ITEM_CD"] == one_comp_items.loc[idx, "ITEM_CD"]) &
                    (items_for_finacc.loc[:, "ITEM_TYP"] == one_comp_items.loc[idx, "ITEM_TYP"])]

                if len(item_idx) != 1:
                    logger.error(f'# of items should be 1: {comp_code} index {item_idx} for item_cd {one_comp_items.loc[idx, "ITEM_CD"]}'
                    f' item_type {one_comp_items.loc[idx, "ITEM_TYP"]}')
                    # error_itemcode
                    continue
                
                one_comp_item_data = one_comp_fin_data.loc[
                    (one_comp_fin_data.loc[:, "ITEM_CD"] == items_for_finacc.loc[item_idx, "ITEM_CD"].iloc[0]) &
                    (one_comp_fin_data.loc[:, "ITEM_TYP"] == items_for_finacc.loc[item_idx, "ITEM_TYP"].iloc[0]), :].copy()
                
                # ORIGNAL_FIELD_CHECK #
                if config['use_original_fields']:
                    matches = ((ORIGINAL_FIELDS.iloc[:, 0] == items_for_finacc.loc[item_idx, "ITEM_CD"].iloc[0]) &
                                   (ORIGINAL_FIELDS.iloc[:, 2] == items_for_finacc.loc[item_idx, "ITEM_TYP"].iloc[0]))
                    if not any(matches):
                        logger.debug(f'not original field - {items_for_finacc.loc[item_idx, "ITEM_CD"]}')
                        continue

                item_name = items_for_finacc.loc[item_idx, "ITEM_NM_ENG"]
                if items_for_finacc.loc[item_idx, "UNT_TYP"].iloc[0] == "천원":
                    one_comp_item_data.loc[:, 'VAL'] = 1000 * one_comp_item_data.loc[:, 'VAL'].values

                for itx in one_comp_item_data.index:
                    date_ = one_comp_item_data.loc[itx, "YYMM"]
                    mat_by_acct[k].loc[date_, item_name] = one_comp_item_data.loc[itx, "VAL"]

        if mat_by_acct[k].shape[0] > 0:
            mat_by_acct[k] = mat_by_acct[k].dropna(subset=mat_by_acct[k].columns[4:],how='all', axis=0)
            mat_by_acct[k] = mat_by_acct[k].sort_index()

    for acct in TermAcctMethod:
        logger.info(f"{filepath + TermAcctDir[acct.name].value + '/' + 'A' + comp_code + '.csv'}  {mat_by_acct[acct]}")
        if not os.path.exists(filepath + TermAcctDir[acct.name].value + '/py'):
            os.makedirs(filepath + TermAcctDir[acct.name].value + '/py' )
        mat_by_acct[acct].to_csv(filepath + TermAcctDir[acct.name].value + '/py' + "/" + "A" + comp_code + ".csv", quoting=csv.QUOTE_NONNUMERIC)


def convert_comp_fin_data_by_item(daily_data, comp_fin_data):
    fin_info = daily_data['TF_CMP_FINDATA_INFO']
    
    fin_info_by_acct = dict()
    for acct in TermAcctMethod:
        fin_info_by_acct[acct] = fin_info.loc[fin_info.loc[:, "TERM_TYP"] == acct.value, :]
    comp_rows = comp_fin_data["YYMM"].drop_duplicates()
    
    fin_data_by_acct = dict()
    for acct in TermAcctMethod:
        fin_data_by_acct[acct] = comp_fin_data.loc[comp_fin_data.loc[:, "TERM_TYP"] == acct.value, :]

    for i in range(len(ITEMNAME_NEED_2)):
        logger.info(f'ITEM: {i}/{len(ITEMNAME_NEED_2)}')
        logger.info(f'{ITEMNAME_NEED_2[i]}')

        item_code = ITEMCODE_NEED_2[i]
        item_name = ITEMNAME_NEED_2[i]

        mat_by_acct = dict()
        for acct in TermAcctMethod:
            empty_matrix = pd.DataFrame(
                index=comp_rows,
                columns=["A" + s for s in fin_data_by_acct[acct].loc[:, "CMP_CD"].unique() if len(s) > 0])
            if os.path.exists(by_ITEM_path + "/" + TermAcctDir[acct.name].value + "/" + item_name + ".csv"):
                mat = pd.read_csv(by_ITEM_path + "/" + TermAcctDir[acct.name].value + "/" + item_name + ".csv",
                        dtype='str', 
                        index_col=0)
            else:
                mat = empty_matrix
            mat.index = mat.index.astype('str')

            mat_by_acct[acct] = mat

            one_item_fin_data = fin_data_by_acct[acct].loc[fin_data_by_acct[acct].loc[:, "ITEM_CD"] == item_code, :]
            one_item_fin_data = one_item_fin_data.loc[one_item_fin_data.loc[:, "ITEM_TYP"] == "M", :]

            if len(one_item_fin_data) > 0:
                date_list = one_item_fin_data.loc[:, "YYMM"].sort_values().unique()
                for idx in range(len(date_list)):
                    #FIXME
                    # idx = 0
                    date_ = date_list[idx]
                    one_date_item_fin_data = one_item_fin_data.loc[one_item_fin_data.loc[:, "YYMM"] == date_, :]
                    one_date_item_fin_data = one_date_item_fin_data.drop_duplicates()
                    ##0510 CMP_code change
                    comp_codes = 'A' + one_date_item_fin_data.loc[:, 'CMP_CD']
                    val = one_date_item_fin_data.loc[:, ['VAL', 'CMP_CD']]
                    val = val.set_index('CMP_CD')
                    val.index = 'A' + val.index 

                    if daily_data['TZ_ITEM'].loc[
                        daily_data['TZ_ITEM'].iloc[:, 0] == item_code, 'UNT_TYP'].unique()[0] == '천원':
                        val = 1000 * val

                    # Find if there is new CMP_code has appeared
                    for comp_code in comp_codes:
                        if comp_code not in mat_by_acct[acct].columns:
                            logger.info(f'{comp_code} added')
                            mat_by_acct[acct][comp_code] = np.nan

                    # FIXME
                    if date_ not in mat_by_acct[acct].index:
                        logger.info(f'{date_} added in {acct}')
                        df = pd.DataFrame(index=[date_], columns=mat_by_acct[acct].columns)
                        mat_by_acct[acct] = mat_by_acct[acct].append(df)
                    logger.info(f'{mat_by_acct[acct].loc[date_, comp_codes]}, {val}')
                    mat_by_acct[acct].loc[date_, comp_codes] = val['VAL']

                mat_by_acct[acct] = mat_by_acct[acct].dropna(how='all', axis='rows')
                mat_by_acct[acct] = mat_by_acct[acct].dropna(how='all', axis='columns')

            if mat_by_acct[acct].shape[0] != 0:
                mat_by_acct[acct] = mat_by_acct[acct].sort_index()
            if not os.path.exists(by_ITEM_path + "/"+ TermAcctDir[acct.name].value):
                os.makedirs(by_ITEM_path + "/"+ TermAcctDir[acct.name].value)
            mat_by_acct[acct].to_csv(by_ITEM_path + "/"+ TermAcctDir[acct.name].value + "/"+ item_name+ ".csv1")
            logger.info(by_ITEM_path + "/" + TermAcctDir[acct.name].value + "/" + item_name + ".csv")


def convert_sec_fin_data_by_sector(daily_data):
    sec_fin_data = daily_data['TF_SEC_FINDATA']
    sector = daily_data['TC_SECTOR'].loc[daily_data['TC_SECTOR'].loc[:, "SEC_TYP"] == "W", :]
    sec_codes = set(sec_fin_data.loc[:, 'SEC_CD']).intersection(set(sector.loc[:, "SEC_CD"]))
    # SECTOR information
    sec_info = sector.loc[:, ("SEC_CD", "MKT_TYP")]
    sec_columns = sec_info.columns

    # SECFINDATA -> TZ_ITEM_M
    if config['use_original_fields']:
        sec_columns = sec_columns.append(pd.Index(
            ORIGINAL_FIELDS.loc[(ORIGINAL_FIELDS.iloc[:, 2] == "A") | (ORIGINAL_FIELDS.iloc[:, 3] == "M")].iloc[:, 5]))

    sec_rows = sec_fin_data.loc[:, "YYMM"].drop_duplicates()

    # FINDATA 
    # TODO factor out acct
    fin_data_by_acct = dict()
    for acct in TermAcctMethod:
        fin_data_by_acct[acct] = sec_fin_data.loc[sec_fin_data.loc[:, "TERM_TYP"] == acct.value, :]

    #####FINDATA_MAT###############
    sec_columns = sec_columns.drop_duplicates()
    empty_mat = pd.DataFrame(index=sec_rows, columns=sec_columns)

    ##BASE_INFO##
    for sec_code in sec_codes:
        mat_by_acct = dict()
        for acct in TermAcctMethod:
            logger.info(f'term_type : {acct}')
            # FIXME
            #acct = AcctMethod.SEP_NET

            mat = empty_mat
            if os.path.exists(SEC_filepath + TermAcctDir[acct.name].value + "/" + sec_code + ".csv"):
                mat = pd.read_csv(SEC_filepath + TermAcctDir[acct.name].value + "/" + sec_code + ".csv", index_col=0, dtype='str')
                mat.index = mat.index.astype('str')

            ## FIXME MKT_TYP may change
            mat.loc[:, "SEC_CD"] = sec_code
            mat.loc[:, "MKT_TYP"] = sec_info.loc[sec_info.loc[:, "SEC_CD"] == sec_code, "MKT_TYP"].iloc[0]
            ##

            # SECFINDATAINFO from here, need to sort the term type
            mat_by_acct[acct] = mat

            # SECFINDATA which is main point. connected to TZ_ITEM
            single_sec_fin_data = fin_data_by_acct[acct].loc[fin_data_by_acct[acct].loc[:, "SEC_CD"] == sec_code, :]
            if len(single_sec_fin_data) > 0:
                items_for_one_sector = single_sec_fin_data.loc[:, "ITEM_CD"].unique()
                for idx in range(len(items_for_one_sector)):
                    one_item_data = single_sec_fin_data.loc[single_sec_fin_data.loc[:, "ITEM_CD"] == items_for_one_sector[idx], :].copy()
                    if config['use_original_fields']:
                        if (ORIGINAL_FIELDS.iloc[:, 0] == items_for_one_sector[idx]).sum(axis=0) == 0:
                            logger.info(f'{idx}, {(ORIGINAL_FIELDS.iloc[:, 0] == items_for_one_sector[idx]).sum(axis=0)}')
                            continue

                        one_item_name = ORIGINAL_FIELDS.loc[
                            (ORIGINAL_FIELDS.iloc[:, 0] == items_for_one_sector[idx]) &
                            (ORIGINAL_FIELDS.iloc[:, 2] == "M")].iloc[0, 5]
                        if ORIGINAL_FIELDS.loc[
                            (ORIGINAL_FIELDS.iloc[:, 0] == items_for_one_sector[idx]) &
                            (ORIGINAL_FIELDS.iloc[:, 2] == "M")].iloc[0, 6] == "천원":
                            one_item_data.loc[:, "VAL"] = 1000 * one_item_data.loc[:, "VAL"]

                    for itx in one_item_data.index:
                        date_ = one_item_data.loc[itx, "YYMM"]
                        if date_ not in mat_by_acct[acct].index:
                            logger.info(f'{date_}  added')
                            mat_by_acct[acct].loc[date_, :] = np.nan
                        mat_by_acct[acct].loc[date_, one_item_name] = one_item_data.loc[itx, "VAL"]

            if len(mat_by_acct[acct]) > 1:
                cols = mat_by_acct[acct].columns.difference(["Common_Shares_FY_End", "Shares_All_FY_End"])
                mat_by_acct[acct] = mat_by_acct[acct].dropna(how='all', subset=cols, axis='rows')

            if len(mat_by_acct[acct]) > 0:
                mat_by_acct[acct] = mat_by_acct[acct].sort_index()

        for acct in TermAcctMethod:
            logger.info(SEC_filepath + TermAcctDir[acct.name].value + "/" + sec_code + ".csv")
            if not os.path.exists(SEC_filepath + TermAcctDir[acct.name].value):
                os.makedirs(SEC_filepath + TermAcctDir[acct.name].value)
            mat_by_acct[acct].to_csv(SEC_filepath + TermAcctDir[acct.name].value + "/" + sec_code + ".csv1")


def convert_sec_fin_data_by_item(daily_data):
    #FIXME why oneday?
    sec_fin_data = daily_data['TF_SEC_FINDATA']
    sector = daily_data['TC_SECTOR'].loc[daily_data['TC_SECTOR'].loc[:, "SEC_TYP"] == "W", :]
    sec_codes = set(sec_fin_data.loc[:, 'SEC_CD']).intersection(set(sector.loc[:, "SEC_CD"]))
    # SECTOR information
    sec_info = sector.loc[:, ("SEC_CD", "MKT_TYP")]
    sec_columns = sec_info.columns

    sec_fin_data = daily_data['TF_SEC_FINDATA']
    sector = daily_data['TC_SECTOR'].loc[daily_data['TC_SECTOR'].loc[:, "SEC_TYP"] == "W", :]
    sec_codes = set(sec_fin_data.iloc[:, 0]).intersection(set(sector.loc[:, "SEC_CD"]))

    # SECTOR information
    sec_info = sector.loc[:, ("SEC_CD", "MKT_TYP")]
    sec_columns = sec_info.columns

    # SECFINDATA -> TZ_ITEM_M
    if config['use_original_fields']:
        sec_columns = sec_columns.append(
            pd.Index(ORIGINAL_FIELDS.loc[(ORIGINAL_FIELDS.iloc[:, 2] == "A") | (ORIGINAL_FIELDS.iloc[:, 3] == "M")].iloc[:,5]))

    sec_rows = sec_fin_data.loc[:, "YYMM"].unique()

    # FINDATA
    fin_data_by_acct = dict()
    for acct in TermAcctMethod:
        fin_data_by_acct[acct] = sec_fin_data.loc[sec_fin_data.loc[:, "TERM_TYP"] == acct.value, :]

    for i in range(len(ITEMNAME_NEED_2)):
        logger.info(f'ITEM:{i}/{len(ITEMNAME_NEED_2)}')
        logger.info(f'{ITEMNAME_NEED_2[i]}')

        item_code = ITEMCODE_NEED_2[i]
        item_name = ITEMNAME_NEED_2[i]

        mat_by_acct = dict()
        for acct in TermAcctMethod:
            acct=TermAcctMethod.SEP_NET
            if len(fin_data_by_acct[acct].loc[:, "SEC_CD"].unique()) == 0:
                continue

            empty_mat = pd.DataFrame(index=sec_rows, columns=fin_data_by_acct[acct].loc[:, "SEC_CD"].drop_duplicates())

            if os.path.exists(by_SEC_ITEM_path + "/" + TermAcctDir[acct.name].value + "/" + item_name + ".csv"):
                mat = pd.read_csv(by_SEC_ITEM_path + "/" + TermAcctDir[acct.name].value + "/" + item_name + ".csv")
            else:
                mat = empty_mat

            mat_by_acct[acct] = mat
            single_item_fin_data = fin_data_by_acct[acct].loc[fin_data_by_acct[acct].loc[:, "ITEM_CD"] == item_code, :]
            single_item_fin_data = single_item_fin_data.loc[single_item_fin_data.loc[ :, "ITEM_TYP"] == "M", :]

            if single_item_fin_data.shape[0] != 0:
                date_list = single_item_fin_data.loc[:, "YYMM"].drop_duplicates()

                for idx in range(len(date_list)):
                    date_ = single_item_fin_data.iloc[idx]["YYMM"]
                    date_item_fin_data = single_item_fin_data.loc[single_item_fin_data.loc[ :, "YYMM"] == date_, :]
                    sec_codes = date_item_fin_data.loc[ :, "SEC_CD"]
                    values = date_item_fin_data.loc[ :, "VAL"]

                    # set value for each sector code
                    if daily_data['TZ_ITEM'].loc[daily_data['TZ_ITEM'].iloc[ :, 0] == item_code, "UNT_TYP"].drop_duplicates().iloc[0] == "천원":
                        values = 1000 * values.astype('float64')

                    # Find if there is new SEC_code has appeared
                    for sec_code in sec_codes:
                        if sec_code not in mat_by_acct[acct].columns:
                            mat_by_acct[acct][sec_code] = np.nan
                    ##rownames가 ???????경우 ??????????????

                    if date_ not in mat_by_acct[acct].index:
                        logger.info(f'{date_} added')
                        mat_by_acct[acct].loc[date_, :] = np.nan

                    mat_by_acct[acct].loc[date_, sec_codes] = values

                mat_by_acct[acct] = mat_by_acct[acct].dropna(how='all', axis=0)
                mat_by_acct[acct] = mat_by_acct[acct].dropna(how='all', axis=1)

            if mat_by_acct[acct].shape[0] > 0:
                mat_by_acct[acct] = mat_by_acct[acct].sort_index()
            if not os.path.exists(by_SEC_ITEM_path + "/" + TermAcctDir[acct.name].value):
                os.makedirs(by_SEC_ITEM_path + "/" + TermAcctDir[acct.name].value)
            mat_by_acct[acct].to_csv(by_SEC_ITEM_path + "/" + TermAcctDir[acct.name].value + "/" + item_name + ".csv1")
            logger.info(f"{by_SEC_ITEM_path}/{TermAcctDir[acct.name].value}/{item_name} + .csv")


def Data_conversion(data_path=INPUTPATH, date='20210201'):
    data_path = INPUTPATH
    date = '20210201'
    # read data files and make dataframes from them
    daily_data = get_daily_data(data_path, date)
    if config['FeedALL']:
        input_path = ALLINPUTPATH
    else:
        input_path = INPUTPATH

    # configuration
    start_day = 0
    if config['FeedALL']:
        allfolder = os.listdir(ALLINPUTPATH)
        start_day = 0
    else:
        allfolder = os.listdir(INPUTPATH)
        allfolder = sorted(allfolder)
        if config['last_day_only']:
            start_day = len(allfolder) - 1
        else:
            start_day = 0
    if start_day > 0:
        start_day = start_day - 50  #

    dates = allfolder[-50:]
    comp_fin_data = get_company_financial_data(input_path, dates)
    comp_info = get_company_info(input_path, dates)

    if len(comp_fin_data) == 0:
        logger.error(f'TF_CMP_FINDATA is empy')
        return
    # CMP_CD = TF_CMP_FINDATA.loc[:, "CMP_CD"].drop_duplicates()
    # company code, market, sector
    comp = daily_data['TC_COMPANY']
    # get listed company info
    listed_comp = comp.loc[comp['LIST_YN'] == '1', ["CMP_CD", "MKT_TYP", "GICS_CD", "WI26_CD"]]

    # company financial info
    fin_info_by_acct = dict()
    # for each accounting method
    fin_info = daily_data['TF_CMP_FINDATA_INFO']
    for acct in TermAcctMethod:
        fin_info_by_acct[acct] = fin_info.loc[fin_info.loc[:, "TERM_TYP"] == acct.value, :]
    # company info + financial periods + financial info
    comp_columns = listed_comp.columns.append(
        daily_data['TF_CMP_FINPRD'].columns).append(
        daily_data['TF_CMP_FINDATA_INFO'].columns)

    # CMPFINDATA -> TZ_ITEM_M -> ORIGINAL_FIELDS[which(ORIGINAL_FIELDS[,3]=="A"|ORIGINAL_FIELDS[,3]=="M"),]
    #TODO what is ORIGINAL_FIELDS?
    if config['use_original_fields']:
        comp_columns = comp_columns.append(
            pd.Index(ORIGINAL_FIELDS.loc[(ORIGINAL_FIELDS.iloc[:, 2] == "A")
                     | (ORIGINAL_FIELDS.iloc[:, 2] == "M")].iloc[:,5]))

    # FINDATA
    fin_data_by_acct = dict()
    for acct in TermAcctMethod:
        fin_data_by_acct[acct] = comp_fin_data.loc[comp_fin_data.loc[:, "TERM_TYP"] == acct.value, :]

    # convert company financial data
    comp_codes = comp_fin_data.loc[:, "CMP_CD"].drop_duplicates()

    for comp_code in comp_codes.sort_values(): #.iloc[:2]:
        # comp_code = '000080'
        convert_comp_fin_data_by_company(comp_code, daily_data, comp_fin_data, comp_info)
    convert_comp_fin_data_by_item(daily_data, comp_fin_data)

    # convert sector financial data
    if len(daily_data['TF_SEC_FINDATA']) > 0:
        convert_sec_fin_data_by_sector(daily_data)
        convert_sec_fin_data_by_item(daily_data)



if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(filename)s %(lineno)d %(levelname)s - %(message)s')

    Data_conversion(data_path=INPUTPATH, date='20210201')