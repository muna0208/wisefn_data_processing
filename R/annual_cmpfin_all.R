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
table_name <- "annual_cmpfin_all"
cmp_cd <- "000660"
# use dbname to select database
DBI::dbSendQuery(conn, paste0("USE ", db_name))
# table columns
dbcols <-DBI::dbListFields(conn, table_name)

df <- DBI::dbGetQuery(conn, paste0("SELECT * FROM ",db_name,".",table_name," WHERE CMP_CD = '",cmp_cd,"'"))
d <- df[with(df, order(YEAR, `_ts`, decreasing=TRUE)),]
d <- distinct(d, YEAR, .keep_all=TRUE)
d <- d[nrow(d):1, ]

print(d)

DBI::dbDisconnect(conn)
