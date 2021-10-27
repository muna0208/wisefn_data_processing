import configparser
import logging

logger = logging.getLogger('utils config')


def get_config(file_name):
    cfg = configparser.ConfigParser()
    try:
        cfg.read(file_name)
    except Exception as e:
        logger.error(f'config {e}')
    return cfg
