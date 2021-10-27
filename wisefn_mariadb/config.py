import configparser
import logging
import urllib.parse

logger = logging.getLogger('mariadb config')


def get_config(file_name):
    cfg = configparser.ConfigParser()
    try:
        cfg.read(file_name)
        connection_uri = (f"mysql+pymysql://{cfg['MariaDB']['user']}:{urllib.parse.quote_plus(cfg['MariaDB']['password'])}"
                         f"@{cfg['MariaDB']['ip_address']}")
        cfg['MariaDB']['connection_uri'] = connection_uri.replace('%', '%%')
    except Exception as e:
        logger.error(f'config {e}')
    
    return cfg


if __name__ == '__main__':
    file_name = '/home/mining/projects/wisefn_data_processing/db2513.config'
    cfg = get_config(file_name)
    cfg['MariaDB']['connection_uri']