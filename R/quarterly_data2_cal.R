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

db_name <- "financial"
table_name <- "cmpfin_bycmp_all"
cmp_cd <- "000660"
# use dbname to select database
DBI::dbSendQuery(conn, paste0("USE ", db_name))
# table columns
dbcols <-DBI::dbListFields(conn, table_name)

df <- DBI::dbGetQuery(conn, paste0("SELECT * FROM ",db_name,".",table_name," WHERE CMP_CD = '",cmp_cd,"'"))
d <- df[with(df, order(YYMM, TERM_TYP, `_ts`, decreasing=TRUE)),]
d <- distinct(d, YYMM, TERM_TYP, .keep_all=TRUE)
d <- d[nrow(d):1, ]

d <- d[nchar(d$CAL_YEAR) == 4 & nchar(d$CAL_QTR) > 0, ]
#연결
con <- d[d$TERM_TYP=='32' & d$CAL_YEAR >='2010' & d$CAL_YEAR <='2022', c('LAQ_YN', 'CAL_YEAR', 'CAL_QTR', 'DATA_EXISTS_YN', 'Sales'), drop=FALSE]
con <- con[1:which(con$LAQ_YN=="1"), ]
#개별
sep <- d[d$TERM_TYP=='2' & d$CAL_YEAR >='2010' & d$CAL_YEAR <='2022', c('LAQ_YN', 'CAL_YEAR', 'CAL_QTR', 'DATA_EXISTS_YN', 'Sales'), drop=FALSE]
sep <- sep[1:which(con$LAQ_YN=="1"), ]

DBI::dbDisconnect(conn)
