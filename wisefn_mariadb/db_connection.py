import os
import pandas as pd
from sqlalchemy import create_engine
from wisefn_mariadb.config import get_config
import logging

logger = logging.getLogger('maria_db_connection')


class MariaDBConnection:
    def __init__(self, connection_uri=None):
        if connection_uri is None:
            logger.error(f'connection_uri {connection_uri}')
            return
        self.connection_uri = connection_uri
        try:
            self.engine = create_engine(self.connection_uri)
        except Exception as e:
            logger.error(f'{e}')

    def get_alchemy_engine(self):
        return self.engine

