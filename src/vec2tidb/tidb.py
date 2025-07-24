from pytidb import TiDBClient
from sqlalchemy import Engine


def create_tidb_engine(database_url: str) -> Engine:
    return TiDBClient.connect(database_url).db_engine
