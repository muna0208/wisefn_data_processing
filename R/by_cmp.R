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
# DBI::dbSendQuery(conn, "USE financial")

db_name <- "financial"
table_name <- "cmpfin_bycmp_all"
cmp_cd <- "000660"

df <- DBI::dbGetQuery(conn, paste0("SELECT * FROM ",db_name,".",table_name," WHERE CMP_CD = '",cmp_cd,"'"))

d <- df[with(df, order(YYMM, TERM_TYP, `_ts`, decreasing=TRUE)),]
d <- distinct(d, YYMM, TERM_TYP, .keep_all=TRUE)
d <- d[nrow(d):1, ]
d <- d[d['YYMM'] <= '202103',c('YYMM', 'TERM_TYP', 'CMP_CD', 'Operating_Profit', 'Sales')]
con <- d[d$TERM_TYP=='32' & d$YYMM >='201003' & d$YYMM <='202103', c('YYMM', 'Operating_Profit', 'Sales'), drop=FALSE]
print(con)

sep <- d[d$TERM_TYP=='2' & d$YYMM >='201003' & d$YYMM <='202103', c('YYMM', 'Operating_Profit', 'Sales'), drop=FALSE]
print(sep)

columns <-DBI::dbListFields(conn, "cmpfin_bycmp_all")
print(columns)
