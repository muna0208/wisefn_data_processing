from enum import Enum
from collections import namedtuple

class TermAcctMethod(Enum):
    SEP_CUM = '1'  # cum
    SEP_NET = '2'  # net
    CON_CUM = '31'
    CON_NET = '32'

class TermAcctDir(Enum):
    SEP_CUM = "1_SEP_CUM"
    SEP_NET = "2_SEP_NET"
    CON_CUM = "31_CON_CUM"
    CON_NET = "32_CON_NET"

class FinAcctType(Enum):
    COMMON = '0'
    MANUFACTURE = '1'  

Need2 = namedtuple('Need2',['code','name']) 
FINITEMCODE_NEED_2 = ("111100","113100","122700","121500","121000",
                    "111000","113000","115000","701200","111300",
                    "113120","115200","122500","121200","132020",
                    "111600","131000","122600","423400","441600",
                    "331030","123200","123100","213000")
FINITEMNAME_NEED_2 = ("01LiquidAsset","02LiquidDebt","03NetIncome","04OperatingProfit",
                    "05SalesAccount","06TotalAsset","07TotalDebt","08TotalEquity",
                    "09Shares","10Cash","11ShortFinanceAsset","12PreferredEquity",
                    "13EarningBeforeTax","14GrossProfit","15DepreciationCost",
                    "16NonCurrentAsset","17OperatingCashFlow","18FirmTax","19DividendPerSharesYears",
                    "20TotalDividendYears","21EV_EBITDA","22EBITDA","23EBIT","24EV")
fin_need2 = [Need2(z[0], z[1]) for z in zip(FINITEMCODE_NEED_2, FINITEMNAME_NEED_2)]


CNSITEMCODE_NEED_2 = ("113100", "113120", "113240", "113250", "121000", 
                    "121500", "122500", "122700", "122710", "211500",
                    "312000", "314000", "331000", "382100", "382500", "423900")
CNSITEMNAME_NEED_2 = ("01Short_Term_Trading_Financial_Assets", "02Short_Term_Financial_Instruments",
                    "03Short_Term_Available_for_Sale_Financial_Assets",
                    "04Short_Term_Held_to_Maturity_Investments", "05Net_Sales", "06Operating_Profit",
                    "07Pre_tax_Profit_from_Continuing_Operations",
                    "08Net_Profit", "09Net_Profit(Owners)", "10ROE(Owners)", "11EPS(Owners)", 
                    "12BPS(Owners)", "13EV_EBITDA", "14P_E", "15P_B", "16DPS(Adj_Comm_Cash_FY_End)")
cns_need2 = [Need2(z[0], z[1]) for z in zip(CNSITEMCODE_NEED_2, CNSITEMNAME_NEED_2)]
