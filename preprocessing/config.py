import configparser
import logging

logger = logging.getLogger('preprocessing config')

config = {
    'last_day_only': True,
    'use_original_fields': True,
    'FeedALL': False,
}


def get_config(file_name):
    cfg = configparser.ConfigParser()
    try:
        cfg.read(file_name)
    except Exception as e:
        logger.error(f'config {e}')
    return cfg
