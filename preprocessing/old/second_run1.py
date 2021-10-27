import csv
import pandas as pd
import os

# 02-1makingDB_second_run_ALLitem.R

##################################### WD ###############################################
from preprocessing.paths import  FIN_quarterly_path, FIN_yearly_path, filepath, by_ITEM_path, \
    SEC_filepath, by_SEC_ITEM_path

#################################### Functions ##########

last_day_only = "YES"
allfolder = os.listdir()

if last_day_only == "YES":
    start_day = len(allfolder) - 1
else:
    start_day = 0


# start_day:length(allfolder)
for kkkk in range(start_day, len(allfolder)):
    os.chdir("/home/mining/systemtrading/WiseFN_Down_DATA/01TXT_file")
    last_day = allfolder[kkkk]
    os.chdir(last_day)
    if not os.path.exists("TC_SECTOR.TXT"):
        continue
    if not os.path.exists("TC_COMPANY.TXT"):
        continue
    #############################New_data_check########################
    TC_SECTOR_new = pd.read_csv("TC_SECTOR.TXT", encoding = "EUC-KR", sep ="|", header = None, dtype='str')
    TF_SEC_FINDATA_new = pd.read_csv("TF_SEC_FINDATA.TXT", encoding = "EUC-KR", sep ="|", header = None)
    # TT_SEC_CNS_DATA_new = as.data.frame(read.table("TT_SEC_CNS_DATA.TXT", fileEncoding = "EUC-KR",sep ="|",header = F,stringsAsFactors = FALSE, strip.white = TRUE, colClasses = "character"))
    TT_EST_MASTER_new = pd.read_csv("TT_EST_MASTER.TXT", encoding = "EUC-KR", sep ="|", header = None, dtype='str')
    ##CoMPany data
    TC_COMPANY_new = pd.read_csv("TC_COMPANY.TXT", encoding =  "EUC-KR", sep= "|", header =None, dtype='str')
    TF_CMP_FINDATA_new = pd.read_csv("TF_CMP_FINDATA.TXT", encoding =  "EUC-KR", sep= "|", header=None, dtype='str')
    TF_CMP_FINDATA_INFO_new = pd.read_csv("TF_CMP_FINDATA_INFO.TXT", encoding =  "EUC-KR", sep= "|", header=None, dtype='str')
    TF_CMP_FINPRD_new = pd.read_csv("TF_CMP_FINPRD.TXT", encoding =  "EUC-KR", sep= "|", header=None, dtype='str')
    # TT_CMP_CNS_DATA_new = as.data.frame(read.table("TT_CMP_CNS_DATA.TXT", fileEncoding = "EUC-KR",sep ="|",header = F,stringsAsFactors = FALSE, strip.white = TRUE, colClasses = "character"))
    ##ETC
    # TZ_ITEM_new = as.data.frame(read.table("TZ_ITEM.TXT",quote = "" ,fileEncoding = "EUC-KR",sep ="|",header = F,fill = TRUE,stringsAsFactors = FALSE, strip.white = TRUE, colClasses = "character"))
    TZ_ITEM_new = pd.read_csv("TZ_ITEM.csv")
    TZ_ITEM_new = TZ_ITEM_new.drop(columns=TZ_ITEM_new.columns[0])

    if len(TF_SEC_FINDATA_new) == 1 :
        if TF_SEC_FINDATA_new == "remove":
            del (TF_SEC_FINDATA_new)
    if len(TF_CMP_FINDATA_new) == 1:
        if TF_CMP_FINDATA_new == "remove":
            del (TF_CMP_FINDATA_new)


##colnames
    TC_SECTOR_new.columns =  ("SEC_CD", "SEC_TYP", "P_SEC_CD", "SEC_NM_KOR", "SEC_NM_ENG", "MKT_TYP")
    TF_SEC_FINDATA_new.columns = ("SEC_CD", "TERM_TYP", "YYMM", "ITEM_TYP", "ITEM_CD", "VAL")
    TT_EST_MASTER_new.columns = ("CMP_CD", "NO_TYP", "CONS_YN", "START_DT", "END_DT")
    TC_COMPANY_new.columns = ("CMP_CD", "MKT_TYP", "CMP_NM_KOR", "CMP_NM_ENG", "CMP_FUL_NM_KOR", "CMP_FUL_NM_ENG", "BIZ_REG_NO", "CORP_REG_NO",
                             "FINACC_TYP", "GICS_CD", "WI26_CD", "EST_DT", "LIST_DT", "DELIST_DT", "FYE_MN",
                             "CEO", "ADDR", "ZIP_CD", "TEL", "URL", "AUDITOR", "IFRS_YYMM", "IFRS_YN", "MASTER_CHK", "QTR_MASTER", "LIST_YN")

    TF_SEC_FINDATA_new.columns = ("SEC_CD", "TERM_TYP", "YYMM", "ITEM_TYP", "ITEM_CD", "VAL")

    TF_CMP_FINDATA_new.columns = ("CMP_CD", "TERM_TYP", "YYMM", "ITEM_TYP", "ITEM_CD", "VAL")
    TF_CMP_FINDATA_INFO_new.columns = ("CMP_CD", "YYMM", "TERM_TYP", "START_DT", "END_DT", "DATA_EXIST_YN", "FST_PUB_DT", "PF_DT", "LAA_YN", "LAQ_YN", "SEQ")
    TF_CMP_FINPRD_new.columns = ("CMP_CD", "YYMM", "CAL_YEAR", "CAL_QTR", "CAL_USE_YN", "FS_YEAR", "FS_QTR", "FS_USE_YN", "IFRS_CHK", "MASTER_CHK", "QTR_MASTER")
    # colnames(TT_CMP_CNS_DATA_new) = c("CMP_CD","CNS_DT","TERM_TYP","YYMM","ITEM_TYP","ITEM_CD","VAL")
    TZ_ITEM_new.columns = ("ITEM_CD", "FINACC_TYP", "ITEM_TYP", "FS_TYP", "ITEM_NM_KOR", "ITEM_NM_ENG", "UNT_TYP")

    TZ_ITEM_M_new = TZ_ITEM_new.loc[(TZ_ITEM_new.iloc[:, 2] == "M") | (TZ_ITEM_new.iloc[:, 2] == "A"), :]
    TZ_ITEM_E_new = TZ_ITEM_new.loc[(TZ_ITEM_new.iloc[:, 2] == "E") | (TZ_ITEM_new.iloc[:, 2] == "A"), :]


    #########################################FINANCIAL DATA#####################################
    # check if NEW COMPANY has added
    CMP_CD = (TF_CMP_FINDATA_new.iloc[:, 0].unique())

    # check if #quarterly and #yearly name is same
    quarterly_CMP_CD = [s.replace('.csv', '') for s in os.listdir(FIN_quarterly_path)]
    quarterly_CMP_CD = [i.replace('A', '') for i in quarterly_CMP_CD]

    yearly_CMP_CD = [s.replace('.csv', '') for s in os.listdir(FIN_yearly_path)]
    yearly_CMP_CD = [i.replace('A', '') for i in yearly_CMP_CD]

    if quarterly_CMP_CD != yearly_CMP_CD:
        print("error, quarterly and yearly cmp_cd is different")
        break
    else:
        listed_CMP_CD = quarterly_CMP_CD
        del (quarterly_CMP_CD)
        del (yearly_CMP_CD)

    TC_COMPANY = TC_COMPANY_new
    TC_SECTOR = TC_SECTOR_new
    TZ_ITEM = TZ_ITEM_new
    TZ_ITEM_M = TZ_ITEM_M_new
    TZ_ITEM_E = TZ_ITEM_E_new

    ###############updatig CMP_FINPRD & CMP_FINDATA & CMP_FiNDTA_INFO
    if TF_CMP_FINDATA_new:
        TF_CMP_FINPRD = TF_CMP_FINPRD_new
        TF_CMP_FINDATA = TF_CMP_FINDATA_new
        TF_CMP_FINDATA_INFO = TF_CMP_FINDATA_INFO_new

        COLNAMES_CMP = None

        # Company code,market,sector
        CMPINFO = TC_COMPANY.loc[:, ("CMP_CD", "MKT_TYP", "GICS_CD", "WI26_CD")]
        COLNAMES_CMP = CMPINFO.columns
        CMPINFO = CMPINFO.loc[TC_COMPANY.loc[:, "LIST_YN"] == "1",: ]


        # CMPFINPRD
        COLNAMES_CMP = COLNAMES_CMP.union(TF_CMP_FINPRD.columns)

        # CMPFININFO
        term_type = (1, 2, 31, 32)
        term_type_folder = ("1_SEP_CUM", "2_SEP_NET", "31_CON_CUM", "32_CON_NET")
        CMPFININFO = dict()
        CMPFININFO[1] = TF_CMP_FINDATA_INFO.loc[TF_CMP_FINDATA_INFO.loc[:, "TERM_TYP"].astype(int) == 1, :]
        CMPFININFO[2] = TF_CMP_FINDATA_INFO.loc[TF_CMP_FINDATA_INFO.loc[:, "TERM_TYP"].astype(int) == 2, :]
        CMPFININFO[3] = TF_CMP_FINDATA_INFO.loc[TF_CMP_FINDATA_INFO.loc[:, "TERM_TYP"].astype(int) == 31, :]
        CMPFININFO[4] = TF_CMP_FINDATA_INFO.loc[TF_CMP_FINDATA_INFO.loc[:, "TERM_TYP"].astype(int) == 32, :]
        COLNAMES_CMP = COLNAMES_CMP.union(TF_CMP_FINDATA_INFO.columns)

        # CMPFINDATA -> TZ_ITEM_M
        COLNAMES_CMP = COLNAMES_CMP.union( TZ_ITEM_M.loc[:, "ITEM_NM_ENG"])
        ROWNAMES_CMP = TF_CMP_FINDATA["YYMM"].unique()

        # FINDATA
        CMPFINDATA = dict()
        CMPFINDATA[1] = TF_CMP_FINDATA.loc[TF_CMP_FINDATA.loc[:, "TERM_TYP"].astype(int) == 1, :]
        CMPFINDATA[2] = TF_CMP_FINDATA.loc[TF_CMP_FINDATA.loc[:, "TERM_TYP"].astype(int) == 2, :]
        CMPFINDATA[3] = TF_CMP_FINDATA.loc[TF_CMP_FINDATA.loc[:, "TERM_TYP"].astype(int) == 31, :]
        CMPFINDATA[4] = TF_CMP_FINDATA.loc[TF_CMP_FINDATA.loc[:, "TERM_TYP"].astype(int) == 32, :]

        #####FINDATA_MAT###############
        import numpy as np
        COLNAMES_CMP = np.unique(COLNAMES_CMP)
        COLNAMES_CMP = COLNAMES_CMP[COLNAMES_CMP!="YYMM"]
        FINDATAMAT = pd.DataFrame(index=ROWNAMES_CMP, columns=COLNAMES_CMP)

        for i in range(len(CMP_CD)):
            print("code:", i, "/", len(CMP_CD))
            print(CMP_CD[i])
            code = CMP_CD[i]
            if len(CMPINFO.iloc[:, 1] == code) == 0:
                continue

            mat_type = dict()
            for k in range(len(term_type)):
                print("term_type : ", k, "/", len(term_type))
                if os.path.exists(filepath + term_type_folder[k] + "/" + "A" + CMP_CD[i] + ".csv"):
                    mat = pd.read_csv(filepath + term_type_folder[k] + "/" + "A" + CMP_CD[i] + ".csv", dtype='str')
                    mat = mat.set_index(mat.columns[0])
                    if mat.shape[0] == 0:
                        mat = FINDATAMAT
                else:
                    mat = FINDATAMAT

                # CMPINFO
                mat.loc[:, "MKT_TYP"] = CMPINFO.loc[CMPINFO.iloc[:, 0] == code].iloc[:, 1].iloc[0]
                mat.loc[:, "GICS_CD"] = CMPINFO.loc[CMPINFO.iloc[:, 0] == code].iloc[:, 2].iloc[0]
                mat.loc[:, "WI26_CD"] = CMPINFO.loc[CMPINFO.loc[:, "CMP_CD"] == code]["WI26_CD"].iloc[0]

                # adding CMPFINPRD
                PRD = TF_CMP_FINPRD.loc[TF_CMP_FINPRD.loc[:, "CMP_CD"] == code,: ]

                if PRD.shape[0] != 0:
                    for p in range(PRD.shape[0]):
                        DATE = PRD.iloc[p]["YYMM"]
                        if DATE not in mat.index:
                            print(DATE, "added")
                            mat.loc[DATE,:] = np.nan

                        mat.loc[DATE,PRD.columns[PRD.columns != "YYMM"]] = PRD.iloc[p][PRD.columns[PRD.columns != "YYMM"]]

                # CMPFINDATAINFO from here, need to sort the term type
                mat_type[k] = mat
                INFO = CMPFININFO[k].loc[CMPFININFO[k].loc[:, "CMP_CD"] == code]

                if INFO.shape[0] != 0:
                    for j in range(INFO.shape[0]):
                        DATE = INFO.iloc[j]["YYMM"]
                        mat_type[k].loc[DATE, INFO.columns[INFO.columns != "YYMM"]] = INFO.iloc[j][INFO.columns[INFO.columns != "YYMM"]]

                # CMPFINDATA which is main point. connected to TZ_ITEM
                FINDATA = CMPFINDATA[k].loc[CMPFINDATA[k].loc[:, "CMP_CD"] == code, :]
                if FINDATA.shape[0] != 0:
                    ITEM_LIST = FINDATA.loc[:, "ITEM_CD"].unique()
                    for j in range(len(ITEM_LIST)):
                        ITEMDATA = FINDATA.loc[FINDATA.loc[:, "ITEM_CD"] == ITEM_LIST[j], :]
                        ITEMNAME = TZ_ITEM_M.loc[TZ_ITEM_M.iloc[:, 0].astype(str) == ITEM_LIST[j], "ITEM_NM_ENG"]

                        if np.unique(TZ_ITEM_M.loc[TZ_ITEM_M.iloc[:, 0].astype(str)== ITEM_LIST[j], "UNT_TYP"])[0] == "천원":
                            ITEMDATA.loc[:, "VAL"] = 1000 * ITEMDATA.loc[:, "VAL"].astype('float64')

                        for m in range(ITEMDATA.shape[0]):
                            DATE = ITEMDATA.iloc[m]["YYMM"]
                            mat_type[k].loc[DATE, ITEMNAME] = ITEMDATA.iloc[m]["VAL"]

                if mat_type[k].shape[0] != 1:
                    no = mat_type[k].iloc[:, [i  for i in range(len(mat_type[k].columns)) if i not in [1,2,3]]].apply(lambda x: all(x!=x), axis=1)
                    mat_type[k] = mat_type[k].loc[~no, :]

                # if mat_type[k].shape[0] == 0:
                #     mat_type[k] = mat_type[k].T
                #     mat_type[k].index = ['dt']

                if mat_type[k].shape[0] != 0:
                    mat_type[k] = mat_type[k].sort_index()

            for o in range(len(term_type)):
                mat_type[o].to_csv(filepath +term_type_folder[o]+ "/"+ "A"+ code+ ".csv")


        ##############DATA by item
        ITEMCODE_NEED_2 = ("111100", "113100", "122700", "121500", "121000",
                            "111000", "113000", "115000", "701200", "111300",
                            "113120", "115200", "122500", "121200", "132020",
                            "111600", "131000", "122600", "423400", "441600",
                            "331030", "123200", "123100", "213000")
        ITEMNAME_NEED_2 = ("01LiquidAsset", "02LiquidDebt", "03NetIncome", "04OperatingProfit",
                            "05SalesAccount", "06TotalAsset", "07TotalDebt", "08TotalEquity",
                            "09Shares", "10Cash", "11ShortFinanceAsset", "12PreferredEquity",
                            "13EarningBeforeTax", "14GrossProfit", "15DepreciationCost",
                            "16NonCurrentAsset", "17OperatingCashFlow", "18FirmTax", "19DividendPerSharesYears",
                            "20TotalDividendYears", "21EV_EBITDA", "22EBITDA", "23EBIT", "24EV")

        for i in range(len(ITEMNAME_NEED_2)):
            print("ITEM:", i, "/", len(ITEMNAME_NEED_2))
            print(ITEMNAME_NEED_2[i])
            item = ITEMCODE_NEED_2[i]
            itemname = ITEMNAME_NEED_2[i]

            mat_type = dict()
            for k in range(len(term_type)):
                ITEMDATAMAT = pd.DataFrame(index=ROWNAMES_CMP,
                            columns=["A" + s for s in CMPFINDATA[k].loc[:, "CMP_CD"].unique() if len(s) > 0])
                if os.path.exists(by_ITEM_path + "/"+ term_type_folder[k]+ "/"+ itemname +  ".csv"):
                    mat = pd.read_csv(by_ITEM_path + "/"+ term_type_folder[k]+ "/"+ itemname + ".csv", index_col=0)
                else:
                    mat = ITEMDATAMAT

                mat_type[k] = mat

                FINDATA = CMPFINDATA[k].loc[CMPFINDATA[k].loc[:, "ITEM_CD"] == item, :]
                FINDATA = FINDATA.loc[FINDATA.loc[:, "ITEM_TYP"] == "M", :]

                if FINDATA.shape[0] != 0:
                    DATE_LIST = FINDATA.loc[:, "YYMM"].unique()
                    for j in range(len(DATE_LIST)):
                        DATE = DATE_LIST[0]
                        DATE_DATA = FINDATA.loc[FINDATA.loc[:, "YYMM"] == DATE, :]
                        ##0510 CMP_code change
                        CMP_code = "A" + DATE_DATA.loc[:, "CMP_CD"]
                        VAL = DATE_DATA.loc[:, "VAL"]

                        if TZ_ITEM_M.loc[TZ_ITEM_M.iloc[:, 0] == item, "UNT_TYP"].unique()[0] == "천원":
                            VAL = 1000 * VAL.astype('float64')

                        # Find if there is new CMP_code has appeared
                        for oo in range(len(CMP_code)):
                            if CMP_code[oo] not in mat_type[k].columns:
                                print(CMP_code[oo], "added")
                                mat_type[k][CMP_code[oo]] = np.nan

                        ##rownames가 ???????경우 ??????????????
                        if DATE not in mat_type[k].index:
                            print(DATE, "added_in", k)
                            mat_type[k].index = mat_type[k].index.insert(0, DATE)

                        mat_type[k].loc[DATE, CMP_code] = VAL

                    no = mat_type[k].apply(lambda x : all(x!=x), axis=1)
                    mat_type[k] = mat_type[k].loc[~no, :]

                    no = mat_type[k].apply(lambda x: all(x!=x))
                    mat_type[k] = mat_type[k].loc[:, ~no]

                # if  mat_type[k].shape[0]:
                #     mat_type[k] = mat_type[k].T
                #     mat_type[k].index = mat_type[k].index.insert(0, dt)

                if mat_type[k].shape[0] != 0:
                    mat_type[k] = mat_type[k].sort_index()
                mat_type[k].to_csv(by_ITEM_path + "/"+ term_type_folder[k]+ "/"+ itemname+ ".csv")


    #########################################################################################################
    #################################SECTOR##################################################################
    #########################################################################################################


    if TF_SEC_FINDATA_new.shape[0] != 0:
        FINDATAMAT = None
        TF_SEC_FINDATA = TF_SEC_FINDATA_new
        TC_SECTOR = TC_SECTOR.loc[TC_SECTOR.loc[:, "SEC_TYP"] == "W", :]
        SEC_CD = TF_SEC_FINDATA.iloc[:, 0][TF_SEC_FINDATA.iloc[:, 0].apply(lambda x: x not in TC_SECTOR.loc[:, "SEC_CD"])]

        COLNAMES_SEC = None

        # SECTOR information
        SECINFO = TC_SECTOR.loc[:, ("SEC_CD", "MKT_TYP")]
        COLNAMES_SEC = SECINFO.columns

        # SECFINDATA -> TZ_ITEM_M
        COLNAMES_SEC = COLNAMES_SEC.append(pd.Index(TZ_ITEM_M.loc[:, "ITEM_NM_ENG"]))
        ROWNAMES_SEC = TF_SEC_FINDATA.loc[:, "YYMM"].unique()

        # FINDATA
        SECFINDATA = dict()
        SECFINDATA[1] = TF_SEC_FINDATA.loc[TF_SEC_FINDATA.loc[:, "TERM_TYP"] == 1, :]
        SECFINDATA[2] = TF_SEC_FINDATA.loc[TF_SEC_FINDATA.loc[:, "TERM_TYP"] == 2, :]
        SECFINDATA[3] = TF_SEC_FINDATA.loc[TF_SEC_FINDATA.loc[:, "TERM_TYP"] == 31, :]
        SECFINDATA[4] = TF_SEC_FINDATA.loc[TF_SEC_FINDATA.loc[:, "TERM_TYP"] == 32, :]

        #####FINDATA_MAT###############
        COLNAMES_SEC = COLNAMES_SEC.unique()
        FINDATAMAT = pd.DataFrame(index=ROWNAMES_SEC, columns=COLNAMES_SEC)

        ##BASE_INFO##
        for i in range(len(SEC_CD)):
            print("code:", i, "/", len(SEC_CD))
            code = SEC_CD[i]
            print(code)

            mat_type = dict()

            for k in range(len(term_type)):
                print("term_type : ", k, "/", len(term_type))

                if os.path.exists(SEC_filepath + term_type_folder[k] + "/" + SEC_CD[i] + ".csv"):
                    mat = pd.read_csv(SEC_filepath+ term_type_folder[k]+ "/"+ SEC_CD[i]+ ".csv")
                    if mat.shape[0] == 0:
                        mat = FINDATAMAT
                else:
                    mat = FINDATAMAT

                ##
                mat.loc[ :, "SEC_CD"] = code
                mat.loc[ :, "MKT_TYP"] = SECINFO.loc[SECINFO.loc[:, "SEC_CD"] == code, "MKT_TYP"]
                ##

                # SECFINDATAINFO from here, need to sort the term type
                mat_type[k] = mat

                # SECFINDATA which is main point. connected to TZ_ITEM
                FINDATA = SECFINDATA[k][SECFINDATA[k].loc[:, "SEC_CD"] == code, :]
                if FINDATA.shape[0] != 0:
                    ITEM_LIST = FINDATA.loc[ :, "ITEM_CD"].unique()
                    for j in range(len(ITEM_LIST)):
                        ITEMDATA = FINDATA.loc[FINDATA.loc[ :, "ITEM_CD"] == ITEM_LIST[j], :]
                        ITEMNAME = TZ_ITEM_M.loc[TZ_ITEM_M.loc[:, 0] == ITEM_LIST[j], "ITEM_NM_ENG"]

                        if TZ_ITEM_M.loc[TZ_ITEM_M.iloc[ :, 0] == ITEM_LIST[j], "UNT_TYP"].unique() == "천원":
                            ITEMDATA.loc[:, "VAL"] = 1000 * ITEMDATA.loc[:, "VAL"].astype('float64')

                        for m in range(ITEMDATA.shape[0]):
                            DATE = ITEMDATA.iloc[m]["YYMM"]

                            # DATE add
                            if DATE not in mat_type[k].index:
                                print(DATE, "added")
                                mat_type[k][DATE, :] = np.nan

                            mat_type[k].loc[DATE, ITEMNAME] = ITEMDATA.iloc[m]["VAL"]

                if mat_type[k].shape[0] > 1:
                    cols = mat_type[k].columns.difference(["Common_Shares_FY_End", "Shares_All_FY_End"])
                    mat_type[k] = mat_type[k].dropna(how='all', subset=cols)
                # if mat_type[k].shape[0] == 0:
                #     mat_type[k] = mat_type[k].T
                #     mat_type[k].index = dt

                if mat_type[k].shape[0] != 0:
                    mat_type[k] = mat_type[k].sort_index()

            for o in range(len(term_type)):
                mat_type[o].to_csv(SEC_filepath+ term_type_folder[o]+ "/"+ SEC_CD[i]+ ".csv")


        for i in range(len(ITEMNAME_NEED_2)):
            print("ITEM:", i, "/", len(ITEMNAME_NEED_2))
            print(ITEMNAME_NEED_2[i])

            item = ITEMCODE_NEED_2[i]
            itemname = ITEMNAME_NEED_2[i]

            mat_type = dict()
            for k in range(len(term_type)):
                if len(SECFINDATA[k].loc[:, "SEC_CD"].unique()) == 0:
                    continue

                ITEMDATAMAT = pd.DataFrame(index=ROWNAMES_SEC, columns=SECFINDATA[k].loc[:, "SEC_CD"].drop_duplicates())

                if os.path.exists(by_SEC_ITEM_path+ "/"+ term_type_folder[k]+ "/"+itemname+ ".csv"):
                    mat = pd.read_csv(by_SEC_ITEM_path+ "/"+ term_type_folder[k]+ "/"+ itemname+ ".csv")
                else:
                    mat = ITEMDATAMAT

                mat_type[k] = mat
                FINDATA = SECFINDATA[k].loc[SECFINDATA[k].loc[:, "ITEM_CD"] == item, :]
                FINDATA = FINDATA.loc[FINDATA.loc[ :, "ITEM_TYP"] == "M", :]

                if FINDATA.shape[0] != 0:
                    DATE_LIST = FINDATA.loc[:, "YYMM"].drop_duplicates()

                    for j in range(len(DATE_LIST)):
                        DATE = FINDATA.iloc[j]["YYMM"]
                        DATE_DATA = FINDATA.loc[FINDATA.loc[ :, "YYMM"] == DATE, :]
                        SEC_code = DATE_DATA.loc[ :, "SEC_CD"]
                        VAL = DATE_DATA.loc[ :, "VAL"]

                        if TZ_ITEM_M.loc[TZ_ITEM_M.iloc[ :, 1] == item, "UNT_TYP"].drop_duplicate()[0] == "천원":
                            VAL = 1000 * VAL.astype('float64')

                        # Find if there is new SEC_code has appeared
                        for oo in range(len(SEC_code)):
                            if SEC_code[oo] not in mat_type[k].columns:
                                mat_type[k][SEC_code[oo]] = np.nan
                        ##rownames가 ???????경우 ??????????????

                        if DATE not in mat_type[k].index:
                            print(DATE, "added")
                            mat_type[k].loc[DATE, :] = np.nan

                        mat_type[k].loc[DATE, SEC_code] = VAL


                    mat_type[k] = mat_type[k].dropna(how='all', aixs=0)
                    mat_type[k] = mat_type[k].dropna(how='all', aixs=1)

                # if mat_type[k].shape[0] == 0:
                #     mat_type[k] = mat_type[k].T
                #     mat_type[k].index = dt

                if mat_type[k].shape[0] != 0:
                    mat_type[k] = mat_type[k].sort_index()
                mat_type[k].to_csv(by_SEC_ITEM_path+ "/"+ term_type_folder[k]+ "/"+ itemname+ ".csv")
