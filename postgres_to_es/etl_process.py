from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from extract import PSExtract
from transform import parse_from_postgres_to_es
from load import ESLoad
from state import State


class EtlProcess:
    MODEL_NAMES = ['film_work', 'person', 'genre']
    def check_and_update(pg_conn: _connection, curs: DictCursor, es_connect: dict, state: State):
        last_modified = state.get_state("last_modified")
        offset = state.get_state("offset")
        postgres_extractor = PSExtract(pg_conn, curs, offset)
        es_loader = ESLoad(**es_connect)
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