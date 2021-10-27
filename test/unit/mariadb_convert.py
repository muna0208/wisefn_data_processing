import unittest
import configparser
from preprocessing.clickhouse_financial.clickhouse_helper import ClickHouseConnection, ClickHouseSchema
from preprocessing.defines import TermAcctMethod
from wisefn_clickhouse.wisefn2clickhouse_schema import ClickHouseSchema as WisefnClickHouseSchema
from preprocessing.mariadb_convert.all_items.cmp_findata_by_cmp_convert import generate_cmp_fin_by_cmp_all_mp, get_items
from wisefn_clickhouse.clickhouse_writer import ClickHouseWriter
from wisefn_mariadb.db_reader import MariaDBReader
from wisefn_mariadb.db_writer import MariaDBWriter
from wisefn_mariadb.wisefn2mariadb_schema import WiseFNSchema
import wisefn_test_data
import pandas as pd


class TestDBGenerator():

    db_name = 'test_wisefn'

    def __init__(self):
        import urllib.parse
        import sys
        import data
        import pandas as pd

        self.conf = configparser.ConfigParser()
        self.conf['ClickHouse'] = {}
        self.conf['ClickHouse']['host'] = '172.20.1.2'
        self.conf['ClickHouse']['user'] = 'default'
        self.conf['ClickHouse']['password'] = ''

        self.conf['MariaDB'] = {}
        connection_uri = 'mysql+pymysql://'
        ip_address = '172.20.1.3'
        user = 'root'
        password = 'mining@2017'
        c = f"{connection_uri}{user}:{urllib.parse.quote_plus(password)}@{ip_address}"
        self.conf['MariaDB']['connection_uri'] = c.replace('%', '%%')
        

    def prepare_data(self):
        import os
        source = 'mining@10.1.61.51:~mining/systemtrading/./WiseFN_Down_DATA/02ALL'
        dest = wisefn_test_data.__path__[0] + '/'
        print(f'sshpass -p "mining@2017" rsync -arzv --progress {source} {dest}')
        os.system(f'sshpass -p "mining@2017" rsync -aRz --progress {source} {dest}')

    def remove_data(self):
        import os
        dest = wisefn_test_data.__path__[0] + '/WiseFN_Down_DATA'
        print(f'rm -r {dest}')
        os.system(f'rm -r {dest}')

    def generate_wisefn_clickhoue_schema(self):
        import data

        schema = WisefnClickHouseSchema(
            host=self.conf['ClickHouse']['host'], 
            user=self.conf['ClickHouse']['user'], 
            password=self.conf['ClickHouse']['password'])
        schema._create_database(self.db_name)

        excel_file = data.__path__[0] + '/' + 'DBSpec_DaumSoft_20210407.xlsx'
        financial = pd.read_excel(excel_file, sheet_name=1, skiprows=2, index_col=None)
        concensus = pd.read_excel(excel_file, sheet_name=2, skiprows=2)
        items = pd.read_excel(excel_file, sheet_name=3, skiprows=2)
 
        schema = WisefnClickHouseSchema(
            host=self.conf['ClickHouse']['host'], 
            database=self.db_name,
            user=self.conf['ClickHouse']['user'], 
            password=self.conf['ClickHouse']['password'])
        schema._create_table_from_data_frame(financial)
        schema._create_table_from_data_frame(concensus)
        schema._create_table_from_data_frame(items)
        schema.client.disconnect()

    def generate_wisefn_mariadb_schema(self):
        import data
        import urllib.parse

        self.conf['MariaDB'] = {}
        connection_uri = 'mysql+pymysql://'
        ip_address = '172.20.1.3'
        user = 'root'
        password = 'mining@2017'
        c = f"{connection_uri}{user}:{urllib.parse.quote_plus(password)}@{ip_address}"
        self.conf['MariaDB']['connection_uri'] = c.replace('%', '%%')

        excel_file = data.__path__[0] + '/' + 'DBSpec_DaumSoft_20210407.xlsx'
        financial = pd.read_excel(excel_file, sheet_name=1, skiprows=2, index_col=None)
        concensus = pd.read_excel(excel_file, sheet_name=2, skiprows=2)
        items = pd.read_excel(excel_file, sheet_name=3, skiprows=2)

        schema = WiseFNSchema(connection_uri=self.conf['MariaDB']['connection_uri'] , db_name='')
        schema._create_database(self.db_name)
        schema = WiseFNSchema(connection_uri=self.conf['MariaDB']['connection_uri'] , db_name=self.db_name)
        schema.create_table_from_data_frame(financial)
        schema.create_table_from_data_frame(concensus)
        schema.create_table_from_data_frame(items)

    def generate_wisefn_mariadb(self):
        import pandas as pd
        import data

        input_path = wisefn_test_data.__path__[0] + '/WiseFN_Down_DATA/02ALL'
        excel_file = data.__path__[0] + '/' + 'DBSpec_DaumSoft_20210407.xlsx'
        cover = pd.read_excel(excel_file, sheet_name=0, skiprows=3)
        table_names = list(cover['파일명'].dropna())
        
        dbw = MariaDBWriter(self.conf['MariaDB']['connection_uri'] + '/' + self.db_name)
        self.conf['MariaDB']['db_name'] = self.db_name
        for table_name in table_names[:]:
            dbw.write_financial_csv_to_mariadb(self.conf, table_name, input_path, [''])

    def generate_wisefn_clickhouse(self):
        import pandas as pd
        import data

        input_path = wisefn_test_data.__path__[0] + '/WiseFN_Down_DATA/02ALL'
        excel_file = data.__path__[0] + '/' + 'DBSpec_DaumSoft_20210407.xlsx'
        cover = pd.read_excel(excel_file, sheet_name=0, skiprows=3)
        table_names = list(cover['파일명'].dropna())

        dbw = ClickHouseWriter(host=self.conf['ClickHouse']['host'], 
            database=self.db_name, 
            user=self.conf['ClickHouse']['user'], 
            password=self.conf['ClickHouse']['password'])
        for table_name in table_names[:]:
            dbw.write_financial_csv_to_clickhouse(table_name, input_path, [''])  


class TestMariaDBConvert(unittest.TestCase):
    conf = None
    financial_db_name = 'test_financial'
    wisefn_db_name = 'test_wisefn'

    @classmethod
    def setUpClass(cls):
        import urllib.parse
        import sys
        import data
        import pandas as pd

        cls.conf = configparser.ConfigParser()
        cls.conf['ClickHouse'] = {}
        cls.conf['ClickHouse']['host'] = '172.20.1.2'
        cls.conf['ClickHouse']['user'] = 'default'
        cls.conf['ClickHouse']['password'] = ''
        cls.conf['ClickHouse']['database'] = cls.financial_db_name

        cls.conf['MariaDB'] = {}
        connection_uri = 'mysql+pymysql://'
        ip_address = '172.20.1.3'
        user = 'root'
        password = 'mining@2017'
        c = f"{connection_uri}{user}:{urllib.parse.quote_plus(password)}@{ip_address}"
        cls.conf['MariaDB']['connection_uri'] = c.replace('%', '%%')

        schema = ClickHouseSchema(host=cls.conf['ClickHouse']['host'],
                    user=cls.conf['ClickHouse']['user'],
                    password=cls.conf['ClickHouse']['password'],
                    database='',)
        schema._create_database(cls.financial_db_name)
        schema = ClickHouseSchema(host=cls.conf['ClickHouse']['host'],
                    user=cls.conf['ClickHouse']['user'],
                    password=cls.conf['ClickHouse']['password'],
                    database=cls.financial_db_name,)
        schema._create_tables()
        schema._create_annual_tables()
        schema._create_quarterly_tables()
        schema.client.disconnect()        

    @classmethod
    def tearDownClass(cls):
        schema = ClickHouseSchema(
            host=cls.conf['ClickHouse']['host'], 
            user=cls.conf['ClickHouse']['user'], 
            password=cls.conf['ClickHouse']['password'])
        schema.client.execute(f"DROP database {cls.financial_db_name}")
        schema.client.disconnect()        

    def test_cmp_fin_by_cmp_all(self):
        self.conf['Path'] = {}
        self.conf['Path']['by_company_cmp_fin_all_path'] = ''
        self.conf['MariaDB']['db_name'] = self.wisefn_db_name
        self.conf['ClickHouse']['database'] = self.financial_db_name

        generate_cmp_fin_by_cmp_all_mp(self.conf, 
                                    to_csv=False, 
                                    to_clickhouse=True, 
                                    cmps=['000020', '000270', '005930'])
    

    def test_get_items(self):
        from preprocessing.mariadb_convert.all_items.cmp_findata_by_cmp_convert import get_items
        self.conf['MariaDB']['db_name'] = self.wisefn_db_name
        self.conf['ClickHouse']['database'] = self.financial_db_name
        db = MariaDBReader(self.conf['MariaDB']['connection_uri'] + '/' + self.conf['MariaDB']['db_name'])
        items = get_items(db)
        self.assertEqual(len(items), 43, "should be greater than 43")


    def test_get_item_unit(self):
        from preprocessing.mariadb_convert.all_items.cmp_findata_by_cmp_convert import get_item_unit
        self.conf['MariaDB']['db_name'] = self.wisefn_db_name
        self.conf['ClickHouse']['database'] = self.financial_db_name
        db = MariaDBReader(self.conf['MariaDB']['connection_uri'] + '/' + self.conf['MariaDB']['db_name'])
        items = get_items(db)
        item_unit = get_item_unit(items, unit='천원')
        self.assertEqual(len(item_unit), 25, "should be 25")


    def test_get_company(self):
        from preprocessing.mariadb_convert.all_items.cmp_findata_by_cmp_convert import get_company
        self.conf['MariaDB']['db_name'] = self.wisefn_db_name
        self.conf['ClickHouse']['database'] = self.financial_db_name
        db = MariaDBReader(self.conf['MariaDB']['connection_uri'] + '/' + self.conf['MariaDB']['db_name'])
        company = get_company(db)
        self.assertEqual(len(company), 2106, "should be 2106")


    def test_get_cmp_data_tuple_to_merge(self):
        from preprocessing.mariadb_convert.all_items.cmp_findata_by_cmp_convert import get_cmp_data_tuple_to_merge, get_items, get_item_unit
        self.conf['MariaDB']['db_name'] = self.wisefn_db_name
        self.conf['ClickHouse']['database'] = self.financial_db_name
        db = MariaDBReader(self.conf['MariaDB']['connection_uri'] + '/' + self.conf['MariaDB']['db_name'])
        
        items = get_items(db)
        item_unit = get_item_unit(items, unit='천원')

        findata, fininfo, finprd = get_cmp_data_tuple_to_merge(db, '000660', items)
        self.assertEqual(len(findata), 308, 'should be 308')
        self.assertEqual(len(fininfo), 348, 'should be 348')
        self.assertEqual(len(finprd), 95, 'should be 95')

        TermAcctMethod.SEP_NET.value
        findata_ = findata.loc[:,TermAcctMethod.SEP_NET.value,:].copy()
        cols1000 = item_unit.index.intersection(findata_.columns)
        findata_.loc[:, cols1000] = findata_.loc[:, cols1000] * 1000
        fininfo_ = fininfo.loc[:,[TermAcctMethod.SEP_NET.value],:].reset_index(level=[1])

        self.assertEqual(findata_.shape, (85,43), 'should be (85,43)')
        self.assertEqual(fininfo_.shape, (95,9), 'should be (95,9)')


    def test_write_clickhouse(self):
        from preprocessing.mariadb_convert.all_items.cmp_findata_by_cmp_convert import get_cmp_data_tuple_to_merge, get_items, get_item_unit, get_company, write_clickhouse
        import datetime
        from preprocessing.clickhouse_financial.shared_columns import column_map

        self.conf['MariaDB']['db_name'] = self.wisefn_db_name
        self.conf['ClickHouse']['database'] = self.financial_db_name
        db = MariaDBReader(self.conf['MariaDB']['connection_uri'] + '/' + self.conf['MariaDB']['db_name'])
        
        items = get_items(db)
        item_unit = get_item_unit(items, unit='천원')
        company = get_company(db)
        
        findata, fininfo, finprd = get_cmp_data_tuple_to_merge(db, '000660', items)

        TermAcctMethod.SEP_NET.value
        findata_ = findata.loc[:,TermAcctMethod.SEP_NET.value,:].copy()
        cols1000 = item_unit.index.intersection(findata_.columns)
        findata_.loc[:, cols1000] = findata_.loc[:, cols1000] * 1000
        fininfo_ = fininfo.loc[:,[TermAcctMethod.SEP_NET.value],:].reset_index(level=[1])

        con = ClickHouseConnection(
            host=self.conf['ClickHouse']['host'],
            user=self.conf['ClickHouse']['user'],
            password=self.conf['ClickHouse']['password'],
            database=self.conf['ClickHouse']['database'], )

        cmp_cd = '000660'
        table_name = 'cmpfin_bycmp_all'
        write_clickhouse(con, table_name, TermAcctMethod.SEP_NET, cmp_cd, findata_, fininfo_, finprd, company)

        cols = con.get_columns('cmpfin_bycmp_all')
        cols = [column_map[c] if c in column_map else c for c in cols]
        findata_ = findata_.dropna(how='all', axis='columns')
        df = pd.concat([finprd, fininfo_, findata_], axis='columns')
        cmpinfo = pd.DataFrame(index=df.index, columns=['CMP_CD', 'MKT_TYP','GICS_CD', 'WI26_CD'])
        df = pd.concat([cmpinfo, df], axis='columns')

        df.loc[:,'TERM_TYP'] = TermAcctMethod.SEP_NET.value
        df.loc[:,'CMP_CD'] = cmp_cd
        df.loc[:,'MKT_TYP'] = company.loc[cmp_cd,'MKT_TYP']
        df.loc[:,'GICS_CD'] = company.loc[cmp_cd,'GICS_CD']
        df.loc[:,'WI26_CD'] = company.loc[cmp_cd,'WI26_CD']
        
        df = df.rename_axis('YYMM').reset_index()
        str_cols = df.select_dtypes(exclude='float').columns
        df.loc[:, str_cols] = df.loc[:, str_cols].fillna('')
        df = df.reindex(cols, axis='columns')
        df['_ts'] = datetime.datetime.now()

        df2 = con.get_client().query_dataframe(f"SELECT * FROM {table_name} WHERE CMP_CD = '{cmp_cd}'")
        df = df.fillna(0)
        df2 = df2.fillna(0)
        for i in range(df2.shape[0]):
            for j in range(1, df2.shape[1]):
                if df.iloc[i,j] != df2.iloc[i,j]:
                    self.assertEqual(df.iloc[i,j], df2.iloc[i,j])


class TestClickHouseFiancialSchema(unittest.TestCase):
    conf = None
    db_name = 'test_db'
    
    @classmethod
    def setUpClass(cls):
        import urllib.parse
        import sys
        import data
        import pandas as pd

        cls.conf = configparser.ConfigParser()
        cls.conf['ClickHouse'] = {}
        cls.conf['ClickHouse']['host'] = '172.20.1.2'
        cls.conf['ClickHouse']['user'] = 'default'
        cls.conf['ClickHouse']['password'] = ''
        print('TestClickHouseFiancialSchema class setup')

    @classmethod
    def tearDownClass(cls):
        schema = ClickHouseSchema(
            host=cls.conf['ClickHouse']['host'], 
            user=cls.conf['ClickHouse']['user'], 
            password=cls.conf['ClickHouse']['password'])
        schema.client.execute(f"DROP database {cls.db_name}")
        schema.client.disconnect()
        print('TestClickHouseFiancialSchema class teardown')

    def test_schema(self):
        import pandas as pd
        import data

        schema = ClickHouseSchema(host=self.conf['ClickHouse']['host'],
                    user=self.conf['ClickHouse']['user'],
                    password=self.conf['ClickHouse']['password'],
                    database='',)
        schema._create_database(self.db_name)
        schema = ClickHouseSchema(host=self.conf['ClickHouse']['host'],
                    user=self.conf['ClickHouse']['user'],
                    password=self.conf['ClickHouse']['password'],
                    database=self.db_name,)
        schema._create_tables()
        schema._create_annual_tables()
        schema._create_quarterly_tables()


def init_db():
    dbgen = TestDBGenerator()
    dbgen.prepare_data()
    dbgen.generate_wisefn_clickhoue_schema()
    dbgen.generate_wisefn_clickhouse()
    dbgen.generate_wisefn_mariadb_schema()
    dbgen.generate_wisefn_mariadb()
    dbgen.remove_data()


if __name__ == '__main__':
    # import logging
    # logging.basicConfig(level=logging.INFO)
    # t = TestWiseFnMariaDB()
    # t.setUp()
    # t.test_schema()
    init_database = False
    if init_database:
        init_db()   
    
    # TestClickHouseFiancialSchema.setUpClass()
    # t = TestClickHouseFiancialSchema()
    # t.test_schema()
    # TestClickHouseFiancialSchema.tearDownClass()

    # TestMariaDBConvert.setUpClass()
    # t = TestMariaDBConvert()
    # t.test_cmp_fin_by_cmp_all()
    # t.test_get_items()
    # t.test_get_item_unit()
    # t.test_get_company()
    # t.test_get_cmp_data_tuple_to_merge()
    # t.test_write_clickhouse()
    # TestMariaDBConvert.tearDownClass()

    # prepare_data()
    unittest.main()
    # remove_data()