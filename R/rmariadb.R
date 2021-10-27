library(RMariaDB)
library(DBI)
# Connect to my-db as defined in ~/.my.cnf
# DBI info: https://rdrr.io/cran/DBI/man/

con <- DBI::dbConnect(RMariaDB::MariaDB(), 
    dbname='wisefn', 
    username='mining', 
    password='mining@2017',
    host='10.1.61.51',
    port=3306)

company <- DBI::dbGetQuery(con, "SELECT * FROM TC_COMPANY")
print(head(company))

findata <- DBI::dbGetQuery(con, "SELECT * FROM TF_CMP_FINDATA WHERE CMP_CD='000660'")
f <- findata[findata['YYMM']>='201901' & findata['YYMM']<='202106', ]
print(f)

DBI::dbDisconnect(con)
