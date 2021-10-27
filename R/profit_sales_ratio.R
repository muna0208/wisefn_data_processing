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

# "use dbname" to select database
DBI::dbSendQuery(conn, "USE financial")
op <- DBI::dbGetQuery(conn, "SELECT * FROM cmpfin_byitem WHERE ITEM_NM='04OperatingProfit'")
op <- op[with(op, order(YYMM, TERM_TYP, `_ts`)),]
op <- distinct(op, YYMM, TERM_TYP, .keep_all=TRUE)
mo = sapply(op[,'YYMM'], function(x) {substr(x, 5, 6)})
op[(mo == '03' | mo == '06' | mo == '09' | mo == '12') & op['TERM_TYP'] == '1', ]

sls <- DBI::dbGetQuery(conn, "SELECT * FROM cmpfin_byitem WHERE ITEM_NM='05SalesAccount'")
sls <- sls[with(sls, order(YYMM, TERM_TYP, `_ts`)),]
sls <- distinct(sls, YYMM, TERM_TYP, .keep_all=TRUE)
mo = sapply(sls[,'YYMM'], function(x) {substr(x, 5, 6)})
sls[(mo == '03' | mo == '06' | mo == '09' | mo == '12') & sls['TERM_TYP'] == '1', ]

op[, 5:10]/sls[, 5:10]
