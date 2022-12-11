import os
from contextlib import contextmanager
import logging
import sqlite3
import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from dotenv import load_dotenv
from postgres_saver import PostgresSaver
from sqllite_extractor import SQLiteExtractor

load_dotenv()
logging.basicConfig(
    filename='log.txt',
)
logger = logging.getLogger('loader')


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres"""
    logger.info('start_loader')
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_extractor = SQLiteExtractor(connection)
    try:
        for table_name in ['film_work', 'genre', 'person', 'genre_film_work', 'person_film_work']:
            data = sqlite_extractor.fetch_batch_data(table_name)
            postgres_saver.inset_data(table_name, data)
        postgres_saver.cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        logger.exception(error)


@contextmanager
def conn_context(db_path: str):
    conn = sqlite3.connect(db_path)
    yield conn
    conn.close()


if __name__ == '__main__':
    dsl = {
        'dbname': os.environ.get('POSTGRES_DB'),
        'user': os.environ.get('POSTGRES_USER'),
        'password': os.environ.get('POSTGRES_PASSWORD'),
        'host': 'db',
        'port': 5432,
    }
    with conn_context('sqlite_to_postgres/db.sqlite') as sqlite_conn, psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
