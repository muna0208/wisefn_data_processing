library(DBI)
library(dplyr)
library(RClickhouse)
# DBI info: https://rdrr.io/cran/DBI/man/

# selecting dbname is not effective with RClickhouse
conn <-DBI::dbConnect(RClickhouse::clickhouse(), 
    dbname='financial', 
    username='default', 
    password='',
    host="10.1.61.51")

# use dbname to select database
DBI::dbSendQuery(conn, "USE financial")
df <- DBI::dbGetQuery(conn, "SELECT * FROM cmpfin_bycmp_all WHERE CMP_CD = '000660'")
d <- df[with(df, order(YYMM, TERM_TYP, `_ts`)),]
d <- distinct(d, YYMM, TERM_TYP, .keep_all=TRUE)
d <- d[d$YYMM <= '202103',c('YYMM', 'TERM_TYP', 'MASTER_CHK', 'FS_QTR', 'Sales', 'Operating_Profit', 'Net_Profit', 'Current_Liabilities')]
d <- d[d$FS_QTR == '4' &((d$TERM_TYP=='1' & d$MASTER_CHK=='P') | (d$TERM_TYP=='31' & d$MASTER_CHK=='C')),]

# master quarter findata
print(d)


# 연간 주재무제표 만들기
######################## make it a function ########################
master_annual_findata <- function(conn, cmp_cd, fin_columns=c('Sales', 'Operating_Profit', 'Net_Profit', 'Current_Liabilities')) {
    query <- paste0("SELECT * FROM cmpfin_bycmp_all WHERE CMP_CD = ", "'", cmp_cd ,"'")
    print(query)
    df <- DBI::dbGetQuery(conn, query)
    d <- df[with(df, order(YYMM, TERM_TYP, `_ts`)),]
    d <- distinct(d, YYMM, TERM_TYP, .keep_all=TRUE)
    cols = c(c('YYMM', 'TERM_TYP', 'MASTER_CHK', 'FS_QTR'), fin_columns)
    print(cols)
    d <- d[d$YYMM <= '202103', cols]
    d <- d[d$FS_QTR == '4' &((d$TERM_TYP=='1' & d$MASTER_CHK=='P') | (d$TERM_TYP=='31' & d$MASTER_CHK=='C')),]
    return(d)
}

DBI::dbSendQuery(conn, "USE financial")
cmp_cds <- DBI::dbGetQuery(conn, "SELECT DISTINCT(CMP_CD) FROM cmpfin_bycmp_all")
for (cmp_cd in cmp_cds[,1]) {
    print(cmp_cd)
    am <- master_annual_findata(conn, cmp_cd )
    # print when 12 is not the settlement month
    if (!all(substr(am$YYMM, 5, 6) == '12')) {
        print(tail(am))
    }
}

# disconnect from db
DBI::dbDisconnect(conn)
