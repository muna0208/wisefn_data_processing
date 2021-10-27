import clickhouse_driver
from sqlalchemy import create_engine, Column, MetaData, literal
from clickhouse_sqlalchemy import Table, make_session, get_declarative_base, types, engines
import pandas as pd
import datetime
import csv
import re
import logging
import sys


client = clickhouse_driver.Client(host='localhost', user='default', password='', database='financial')
for table in (
    'annual_cmpcns',
    'annual_cmpfin',
    'annual_cmpfin_all',
    'annual_seccns',
    'annual_secfin',
    'annual_secfin_all',
    'cmpcns_bycmp',
    'cmpcns_byitem',
    'cmpfin_bycmp',
    'cmpfin_bycmp_all',
    'cmpfin_byitem',
    'quarterly_cmpcns',
    'quarterly_cmpfin',
    'quarterly_cmpfin_all',
    'quarterly_seccns',
    'quarterly_secfin',
    'quarterly_secfin_all',
    'seccns_byitem',
    'seccns_bysec',
    'secfin_byitem',
    'secfin_bysec',
    'secfin_bysec_all'):
    client.execute(f"ALTER TABLE {table} MODIFY COLUMN _ts DateTime DEFAULT '1970-01-01 00:00:00'")
