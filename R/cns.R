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
df <- DBI::dbGetQuery(conn, "SELECT * FROM quarterly_cmpcns WHERE CMP_CD = '000660'")

d <- df[with(df, order(CNS_DT, YYMM, TERM_TYP, `_ts`)),]
d <- distinct(d, YYMM, TERM_TYP, .keep_all=TRUE)
d <- d[d['YYMM'] <= '202103',c('YYMM', 'CNS_DT', 'CMP_CD', 'Net_Sales', 'Net_Profit', 'Operating_Profit')]
con <- d[d$YYMM >='201003' & d$YYMM <='202103', c('YYMM', 'CNS_DT', 'Net_Sales', 'Net_Profit', 'Operating_Profit'), drop=FALSE]


# 종목별 최신 컨센서스 
######################## make it a function ########################
latest_consensus <- function(conn, cmp_cd) {
    query <- paste0("SELECT * FROM quarterly_cmpcns WHERE CMP_CD = ", "'", cmp_cd ,"'")
    print(query)
    df <- DBI::dbGetQuery(conn, query)
    d <- df[with(df, order(CNS_DT, YYMM, TERM_TYP, `_ts`)),]
    d <- distinct(d, YYMM, TERM_TYP, .keep_all=TRUE)
    d <- d[d['YYMM'] <= '202103',c('YYMM', 'CNS_DT', 'CMP_CD', 'Net_Sales', 'Net_Profit', 'Operating_Profit')]
    d <- d[d$YYMM >='201003' & d$YYMM <='202103', c('YYMM', 'CNS_DT', 'Net_Sales', 'Net_Profit', 'Operating_Profit'), drop=FALSE]
    return(d)
}

DBI::dbSendQuery(conn, "USE financial")
cmp_cds <- DBI::dbGetQuery(conn, "SELECT DISTINCT(CMP_CD) FROM quarterly_cmpcns")
for (cmp_cd in cmp_cds[,1]) {
    print(cmp_cd)
    cns <- latest_consensus(conn, cmp_cd )
    print(tail(cns))
}

# disconnect from db
DBI::dbDisconnect(conn)

