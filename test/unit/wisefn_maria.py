import unittest
from wisefn_mariadb.db_writer import MariaDBWriter
from wisefn_mariadb.db_reader import MariaDBReader
import configparser
import wisefn_test_data

from wisefn_mariadb.wisefn2mariadb_schema import WiseFNSchema



class TestWiseFnMariaDBReadWrite(unittest.TestCase):
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
        # cls.conf['Path'] = {}
        # cls.conf['Path']['base_path'] = wisefn_test_data.__path__[0] + '/'
        cls.conf['MariaDB'] = {}

        connection_uri = 'mysql+pymysql://'
        ip_address = '172.20.1.3'
        user = 'root'
        password = 'mining@2017'

        c = f"{connection_uri}{user}:{urllib.parse.quote_plus(password)}@{ip_address}"
        cls.conf['MariaDB']['connection_uri'] = c.replace('%', '%%')

        excel_file = data.__path__[0] + '/' + 'DBSpec_DaumSoft_20210407.xlsx'
        financial = pd.read_excel(excel_file, sheet_name=1, skiprows=2, index_col=None)
        concensus = pd.read_excel(excel_file, sheet_name=2, skiprows=2)
        items = pd.read_excel(excel_file, sheet_name=3, skiprows=2)

        db_name = 'test_db'
        schema = WiseFNSchema(connection_uri=cls.conf['MariaDB']['connection_uri'] , db_name='')
        schema._create_database('test_db')
        schema = WiseFNSchema(connection_uri=cls.conf['MariaDB']['connection_uri'] , db_name=db_name)
        schema.create_table_from_data_frame(financial)
        schema.create_table_from_data_frame(concensus)
        schema.create_table_from_data_frame(items)

        cls._prepare_data()

        print('TestWiseFnMariaDBReadWrite class setup')

    @classmethod
    def tearDownClass(cls):
        db_name = 'test_db'
        schema = WiseFNSchema(connection_uri=cls.conf['MariaDB']['connection_uri'] , db_name='')
        schema.engine.execute(f"DROP database {db_name}")

        cls._remove_data()

        print('TestWiseFnMariaDBReadWrite class teardown')
        #/home/mining/projects/wisefn_data_processing/wisefn_test_data/WiseFN_Down_DATA/2020/TC_COMPANY.TXT

    def test_write_read_data(self):
        import pandas as pd
        import data

        input_path = wisefn_test_data.__path__[0] + '/WiseFN_Down_DATA/02ALL_allitem'
        excel_file = data.__path__[0] + '/' + 'DBSpec_DaumSoft_20210407.xlsx'
        cover = pd.read_excel(excel_file, sheet_name=0, skiprows=3)
        table_names = list(cover['파일명'].dropna())
        db_name = 'test_db'

        dbw = MariaDBWriter(self.conf['MariaDB']['connection_uri'] + '/' + db_name)
        self.conf['MariaDB']['db_name'] = db_name
        for table_name in table_names[:]:
            # print(self.conf['MariaDB']['connection_uri'], table_name, input_path)
            dbw.write_financial_csv_to_mariadb(self.conf, table_name, input_path, ['2020'])

        dbr = MariaDBReader(self.conf['MariaDB']['connection_uri'] + '/' + db_name)
        for table_name in table_names[:]:
            # print(f'SELECT * FROM {table_name}')
            findata = pd.read_sql(f'SELECT * FROM {table_name}', con=dbr.engine)
            print(findata)


class TestWiseFnMariaDBSchema(unittest.TestCase):
    conf = None

    @classmethod
    def setUpClass(cls):
        import urllib.parse
        import sys
        import data
        import pandas as pd

        cls.conf = configparser.ConfigParser()
        # cls.conf['Path'] = {}
        # cls.conf['Path']['base_path'] = '/home/mining/systemtrading/WiseFN_Down_DATA'
        # cls.conf['Path']['txt_path'] = cls.conf['Path']['base_path'] + '/01TXT_file'
        cls.conf['MariaDB'] = {}

        connection_uri = 'mysql+pymysql://'
        ip_address = '172.20.1.3'
        user = 'root'
        password = 'mining@2017'

        c = f"{connection_uri}{user}:{urllib.parse.quote_plus(password)}@{ip_address}"
        cls.conf['MariaDB']['connection_uri'] = c.replace('%', '%%')

        excel_file = data.__path__[0] + '/' + 'DBSpec_DaumSoft_20210407.xlsx'
        financial = pd.read_excel(excel_file, sheet_name=1, skiprows=2, index_col=None)
        concensus = pd.read_excel(excel_file, sheet_name=2, skiprows=2)
        items = pd.read_excel(excel_file, sheet_name=3, skiprows=2)

        db_name = 'test_db'
        schema = WiseFNSchema(connection_uri=cls.conf['MariaDB']['connection_uri'] , db_name='')
        schema._create_database('test_db')
        schema = WiseFNSchema(connection_uri=cls.conf['MariaDB']['connection_uri'] , db_name=db_name)
        schema.create_table_from_data_frame(financial)
        schema.create_table_from_data_frame(concensus)
        schema.create_table_from_data_frame(items)
        print('maria db class setup')

    @classmethod
    def tearDownClass(cls):
        db_name = 'test_db'
        schema = WiseFNSchema(connection_uri=cls.conf['MariaDB']['connection_uri'] , db_name='')
        schema.engine.execute(f"DROP database {db_name}")
        print('maria db class teardown')

        
    def setUp(self):
        print('maria db setup')

    def tearDown(self):
        print('maria db teardown')
    
    def test_schema_shape(self):
        schema = WiseFNSchema(self.conf['MariaDB']['connection_uri'], db_name='test_db')
        fields = schema.get_fields('TC_COMPANY')
        self.assertEqual(fields.shape, (26,6))
        fields = schema.get_fields('TC_SECTOR')
        self.assertEqual(fields.shape, (6,6))
        fields = schema.get_fields('TF_CMP_FINDATA')
        self.assertEqual(fields.shape, (6,6))
        fields = schema.get_fields('TF_CMP_FINDATA_INFO')
        self.assertEqual(fields.shape, (11,6))
        fields = schema.get_fields('TF_CMP_FINPRD')
        self.assertEqual(fields.shape, (11,6))
        fields = schema.get_fields('TF_SEC_FINDATA')
        self.assertEqual(fields.shape, (6,6))
        fields = schema.get_fields('TT_CMP_CNS_DATA')
        self.assertEqual(fields.shape, (7,6))
        fields = schema.get_fields('TT_EST_MASTER')
        self.assertEqual(fields.shape, (5,6))
        fields = schema.get_fields('TT_SEC_CNS_DATA')
        self.assertEqual(fields.shape, (7,6))
        fields = schema.get_fields('TZ_ITEM')
        self.assertEqual(fields.shape, (10,6))
        fields = schema.get_fields('TZ_WORK_HST')
        self.assertEqual(fields.shape, (3,6))

    def test_schema_creation(self):
        import data
        import pandas as pd

        excel_file = data.__path__[0] + '/' + 'DBSpec_DaumSoft_20210407.xlsx'

        cover = pd.read_excel(excel_file, sheet_name=0, skiprows=3)
        financial = pd.read_excel(excel_file, sheet_name=1, skiprows=2, index_col=None)
        concensus = pd.read_excel(excel_file, sheet_name=2, skiprows=2)
        items = pd.read_excel(excel_file, sheet_name=3, skiprows=2)
        schema = WiseFNSchema(connection_uri=self.conf['MariaDB']['connection_uri'] , db_name='')
        schema._create_database('wisefn_test_db')

        schema = WiseFNSchema(connection_uri=self.conf['MariaDB']['connection_uri'] , db_name='wisefn_test_db')
        schema.create_table_from_data_frame(financial)
        schema.create_table_from_data_frame(concensus)
        schema.create_table_from_data_frame(items)

    def test_read_db(self):
        self.db_reader = MariaDBReader(self.conf['MariaDB']['connection_uri'])
        findata = self.db_reader.read_financial(table_name='wisefn.TF_CMP_FINDATA', cond="WHERE CMP_CD = '000020'")
        self.assertEqual(findata.shape, (76893, 6), "Should be (76893, 6)")
    


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    # t = TestWiseFnMariaDB()
    # t.setUp()
    # t.test_schema()

    # prepare_data()
    unittest.main()
    # remove_data()