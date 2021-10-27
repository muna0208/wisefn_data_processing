import unittest
from wisefn_clickhouse.clickhouse_reader import ClickHouseReader
from wisefn_clickhouse.clickhouse_writer import ClickHouseWriter
from wisefn_clickhouse.wisefn2clickhouse_schema import ClickHouseSchema
import configparser
import wisefn_test_data


class TestWiseFnClickHouseReadWrite(unittest.TestCase):
    conf = None

    @staticmethod
    def _prepare_data():
        import os
        source = 'mining@10.1.61.51:~mining/systemtrading/./WiseFN_Down_DATA/02ALL_allitem/2020'
        dest = wisefn_test_data.__path__[0] + '/'
        print(f'sshpass -p "mining@2017" rsync -arzv --progress {source} {dest}')
        os.system(f'sshpass -p "mining@2017" rsync -aRz --progress {source} {dest}')

    @staticmethod
    def _remove_data():
        import os
        dest = wisefn_test_data.__path__[0] + '/WiseFN_Down_DATA'
        print(f'rm -r {dest}')
        os.system(f'rm -r {dest}')

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
        db_name = 'test_db'

        schema = ClickHouseSchema(
            host=cls.conf['ClickHouse']['host'], 
            user=cls.conf['ClickHouse']['user'], 
            password=cls.conf['ClickHouse']['password'])
        schema._create_database(db_name)

        excel_file = data.__path__[0] + '/' + 'DBSpec_DaumSoft_20210407.xlsx'
        financial = pd.read_excel(excel_file, sheet_name=1, skiprows=2, index_col=None)
        concensus = pd.read_excel(excel_file, sheet_name=2, skiprows=2)
        items = pd.read_excel(excel_file, sheet_name=3, skiprows=2)
 
        schema = ClickHouseSchema(
            host=cls.conf['ClickHouse']['host'], 
            database=db_name,
            user=cls.conf['ClickHouse']['user'], 
            password=cls.conf['ClickHouse']['password'])
        schema._create_table_from_data_frame(financial)
        schema._create_table_from_data_frame(concensus)
        schema._create_table_from_data_frame(items)
        schema.client.disconnect()

        cls._prepare_data()
        print('TestWiseFnClickHouseReadWrite class setup')

    @classmethod
    def tearDownClass(cls):
        db_name = 'test_db'
        schema = ClickHouseSchema(
            host=cls.conf['ClickHouse']['host'], 
            user=cls.conf['ClickHouse']['user'], 
            password=cls.conf['ClickHouse']['password'])
        schema.client.execute(f"DROP database {db_name}")
        schema.client.disconnect()

        cls._remove_data()
        print('TestWiseFnClickHouseReadWrite class teardown')

    def test_write_read_data(self):
        import pandas as pd
        import data

        excel_file = data.__path__[0] + '/' + 'DBSpec_DaumSoft_20210407.xlsx'
        cover = pd.read_excel(excel_file, sheet_name=0, skiprows=3)
        table_names = list(cover['파일명'].dropna())
        input_path = wisefn_test_data.__path__[0] + '/WiseFN_Down_DATA/02ALL_allitem'

        db_name = 'test_db'
        chw = ClickHouseWriter(host=self.conf['ClickHouse']['host'], 
            database=db_name, 
            user=self.conf['ClickHouse']['user'], 
            password=self.conf['ClickHouse']['password'])

        for table_name in table_names[:]:
            chw.write_annual_financial_csv_to_clickhouse(
                table_name, 
                input_path, 
                ['2020'])

        chr = ClickHouseReader(host=self.conf['ClickHouse']['host'], 
            database=db_name, 
            user=self.conf['ClickHouse']['user'], 
            password=self.conf['ClickHouse']['password'])

        for table_name in table_names[:]:
            df = chr.read_financial(table_name=table_name, cond="")
            print(table_name, len(df))
            self.assertGreater(len(df), 0, table_name)


class TestWiseFnClickHouseSchema(unittest.TestCase):
    conf = None

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
        db_name = 'test_db'

        schema = ClickHouseSchema(
            host=cls.conf['ClickHouse']['host'], 
            user=cls.conf['ClickHouse']['user'], 
            password=cls.conf['ClickHouse']['password'])
        schema._create_database(db_name)
        schema.client.disconnect()
        print('clickhouse class setup')

    @classmethod
    def tearDownClass(cls):
        db_name = 'test_db'
        schema = ClickHouseSchema(
            host=cls.conf['ClickHouse']['host'], 
            database=db_name,
            user=cls.conf['ClickHouse']['user'], 
            password=cls.conf['ClickHouse']['password'])
        schema.client.execute(f"DROP database {db_name}")
        schema.client.disconnect()
        print('clickhouse class teardown')

    def setUp(self):
        print('clickhouse setup')

    def tearDown(self):
        print('clickhouse teardown')
    
    def test_schema_shape(self):
        db_name = 'test_db'
        schema = ClickHouseSchema(
            host=self.conf['ClickHouse']['host'], 
            database=db_name,
            user=self.conf['ClickHouse']['user'], 
            password=self.conf['ClickHouse']['password'])

    def test_schema_creation(self):
        import data
        import pandas as pd

        excel_file = data.__path__[0] + '/' + 'DBSpec_DaumSoft_20210407.xlsx'
        financial = pd.read_excel(excel_file, sheet_name=1, skiprows=2, index_col=None)
        concensus = pd.read_excel(excel_file, sheet_name=2, skiprows=2)
        items = pd.read_excel(excel_file, sheet_name=3, skiprows=2)

        db_name = 'test_db'
        schema = ClickHouseSchema(
            host=self.conf['ClickHouse']['host'], 
            database=db_name,
            user=self.conf['ClickHouse']['user'], 
            password=self.conf['ClickHouse']['password'])
        schema._create_table_from_data_frame(financial)
        schema._create_table_from_data_frame(concensus)
        schema._create_table_from_data_frame(items)
        schema.client.disconnect()


if __name__ == '__main__':
    # import logging
    # logging.basicConfig(level=logging.INFO)
    # t = TestWiseFnMariaDB()
    # t.setUp()
    # t.test_schema()

    # prepare_data()
    unittest.main()
    # remove_data()