from datetime import datetime
from extract import PSExtract
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from transform import *
from load import ESLoad
from state import State


class EtlProcess:
    MODEL_NAMES = ['film_work', 'person', 'genre']

    @staticmethod
    def init_process(pg_conn: _connection, curs: DictCursor, es_connect: dict, state: State):
        last_modified = state.get_state("last_modified")
        offset = state.get_state("offset")
        postgres_extractor = PSExtract(pg_conn, curs, offset)
        es_loader = ESLoad(**es_connect)
        return last_modified, postgres_extractor, es_loader


    def check_and_update(pg_conn: _connection, curs: DictCursor, es_connect: dict, state: State):
        last_modified, postgres_extractor, es_loader = EtlProcess.init_process(pg_conn, curs, es_connect, state)
        for model_name in EtlProcess.MODEL_NAMES:
            while True:
                filmwork_data = postgres_extractor.extract_filmwork_data(last_modified, model_name)
                if filmwork_data:
                    transformed_filmwork_data = [parse_from_postgres_to_es(_) for _ in filmwork_data]
                    es_loader.send_data(es_loader.es, transformed_filmwork_data)

                    postgres_extractor.offset += len(filmwork_data)
                    state.set_state("offset", postgres_extractor.offset)
                    state.set_state('last_modified', filmwork_data[-1]['modified'].strftime('%Y-%m-%d'))
                else:
                    postgres_extractor.offset = 0
                    state.set_state("offset", 0)
                    break

    def check_and_update_persons(pg_conn: _connection, curs: DictCursor, es_connect: dict, state: State):
        last_modified, postgres_extractor, es_loader = EtlProcess.init_process(pg_conn, curs, es_connect, state)
        while True:
            data = postgres_extractor.extract_person_data(last_modified)
            if data:
                transformed_data = [parse_persons_postgres_to_es(_) for _ in data]
                es_loader.send_persons_data(es_loader.es, transformed_data)

                postgres_extractor.offset += len(data)
                state.set_state("offset", postgres_extractor.offset)
                state.set_state('last_modified', datetime.now().strftime('%Y-%m-%d'))
            else:
                postgres_extractor.offset = 0
                state.set_state("offset", 0)
                break

    def check_and_update_genres(pg_conn: _connection, curs: DictCursor, es_connect: dict, state: State):
        last_modified, postgres_extractor, es_loader = EtlProcess.init_process(pg_conn, curs, es_connect, state)
        while True:
            data = postgres_extractor.extract_genre_data(last_modified)
            if data:
                transformed_data = [parse_genres_postgres_to_es(_) for _ in data]
                es_loader.send_genres_data(es_loader.es, transformed_data)

                postgres_extractor.offset += len(data)
                state.set_state("offset", postgres_extractor.offset)
                state.set_state('last_modified', datetime.now().strftime('%Y-%m-%d'))
            else:
                postgres_extractor.offset = 0
                state.set_state("offset", 0)
                break