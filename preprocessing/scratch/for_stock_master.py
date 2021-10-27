import clickhouse_driver
from sqlalchemy import create_engine, Column, MetaData, literal
from clickhouse_sqlalchemy import Table, make_session, get_declarative_base, types, engines
import pandas as pd
import datetime
import csv
import re
import logging
import sys

client = clickhouse_driver.Client(host='localhost', user='default', password='', database='wisefn')
now = datetime.datetime.now()
start = now - datetime.timedelta(days=3)
start_date = datetime.datetime.strftime(start, "%Y%m%d")
df = client.query_dataframe(f"SELECT * FROM TC_COMPANY WHERE DNDATE >= '{start_date}'")
grouped = df.groupby('DNDATE')
last_day = sorted([g for g in grouped.groups])[-1]
df = grouped.get_group(last_day)

now = datetime.datetime.now()
start = now - datetime.timedelta(days=3)
start_date = datetime.datetime.strftime(start, "%Y%m%d")
df = client.query_dataframe(f"SELECT * FROM TC_SECTOR WHERE DNDATE >= '{start_date}'")
grouped = df.groupby('DNDATE')
last_day = sorted([g for g in grouped.groups])[-1]
df = grouped.get_group(last_day)


now = datetime.datetime.now()
start = now - datetime.timedelta(days=3)
start_date = datetime.datetime.strftime(start, "%Y%m%d")
df = client.query_dataframe(f"SELECT * FROM TF_CMP_FINDATA_INFO WHERE DNDATE = (SELECT MAX(DNDATE) FROM TF_CMP_FINDATA_INFO)")







