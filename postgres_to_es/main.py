import time
from contextlib import closing
import psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv
from state import JsonFileStorage, State
from etl_process import EtlProcess
from logger import logger
from config import DbSettings, ElasticSettings

load_dotenv()


def main(ps_connect: dict, es_connect: dict):
    """
    Функция запуска внутренних компонентов ETL
    """
    while True:
        try:
            with closing(
                psycopg2.connect(**ps_connect, cursor_factory=DictCursor)
            ) as pg_conn, pg_conn.cursor() as curs:
                storage = JsonFileStorage('state.json')
                state = State(storage)

                EtlProcess.check_and_update(pg_conn, curs, es_connect, state)

            with closing(
                psycopg2.connect(**ps_connect, cursor_factory=DictCursor)
            ) as pg_conn, pg_conn.cursor() as curs:
                storage = JsonFileStorage('state_persons.json')
                state = State(storage)

                EtlProcess.check_and_update_persons(pg_conn, curs, es_connect, state)

            with closing(
                psycopg2.connect(**ps_connect, cursor_factory=DictCursor)
            ) as pg_conn, pg_conn.cursor() as curs:
                storage = JsonFileStorage('state_genres.json')
                state = State(storage)

                EtlProcess.check_and_update_genres(pg_conn, curs, es_connect, state)

        except psycopg2.OperationalError:
            logger.error('Error connecting to Postgres database')

        time.sleep(1)


if __name__ == '__main__':
    ps_connect = DbSettings().dict()
    es_connect = ElasticSettings().dict()
    main(ps_connect, es_connect)
