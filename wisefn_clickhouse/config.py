import configparser
import logging
import urllib.parse

logger = logging.getLogger('clickhouse config')


def get_config(file_name):
    cfg = configparser.ConfigParser()
    try:
        cfg.read(file_name)
    except Exception as e:
        logger.error(f'config {e}')
    return cfg


if __name__ == '__main__':
    file_name = '/home/mining/PycharmProjects/wisefn_data_processing/db3.config'
    cfg = get_config(file_name)
    cfg['ClickHouse']['host']
    cfg['ClickHouse']['user']
    cfg['ClickHouse']['database']
    cfg['ClickHouse']['password']
