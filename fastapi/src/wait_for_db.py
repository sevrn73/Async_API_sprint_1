import time
import subprocess
import requests
import logging
from logging import config as logging_config
from core.logger import LOGGING
from contextlib import closing
import psycopg2
from psycopg2.extras import DictCursor
from core.config import PS

# Применяем настройки логирования
logging_config.dictConfig(LOGGING)
logger = logging.getLogger('loader')


def migrate_and_start(ps_connect: dict):
    db_conn = None
    logger.info('Waiting for database...')
    while not db_conn:
        try:
            with closing(
                psycopg2.connect(**ps_connect, cursor_factory=DictCursor)
            ) as pg_conn, pg_conn.cursor() as curs:
                db_conn = True

        except psycopg2.OperationalError:
            logger.info('Database unavailable, waititng 1 second...')
            time.sleep(1)

    logger.info('Database available!')

    request_body = """
    {
        "settings": {
            "refresh_interval": "1s",
            "analysis": {
            "filter": {
                "english_stop": {
                "type":       "stop",
                "stopwords":  "_english_"
                },
                "english_stemmer": {
                "type": "stemmer",
                "language": "english"
                },
                "english_possessive_stemmer": {
                "type": "stemmer",
                "language": "possessive_english"
                },
                "russian_stop": {
                "type":       "stop",
                "stopwords":  "_russian_"
                },
                "russian_stemmer": {
                "type": "stemmer",
                "language": "russian"
                }
            },
            "analyzer": {
                "ru_en": {
                "tokenizer": "standard",
                "filter": [
                    "lowercase",
                    "english_stop",
                    "english_stemmer",
                    "english_possessive_stemmer",
                    "russian_stop",
                    "russian_stemmer"
                ]
                }
            }
            }
        },
        "mappings": {
            "dynamic": "strict",
            "properties": {
            "id": {
                "type": "keyword"
            },
            "imdb_rating": {
                "type": "float"
            },
            "genre": {
                "type": "keyword"
            },
            "title": {
                "type": "text",
                "analyzer": "ru_en",
                "fields": {
                "raw": { 
                    "type":  "keyword"
                }
                }
            },
            "description": {
                "type": "text",
                "analyzer": "ru_en"
            },
            "director": {
                "type": "text",
                "analyzer": "ru_en"
            },
            "actors_names": {
                "type": "text",
                "analyzer": "ru_en"
            },
            "writers_names": {
                "type": "text",
                "analyzer": "ru_en"
            },
            "actors": {
                "type": "nested",
                "dynamic": "strict",
                "properties": {
                "id": {
                    "type": "keyword"
                },
                "name": {
                    "type": "text",
                    "analyzer": "ru_en"
                }
                }
            },
            "writers": {
                "type": "nested",
                "dynamic": "strict",
                "properties": {
                "id": {
                    "type": "keyword"
                },
                "name": {
                    "type": "text",
                    "analyzer": "ru_en"
                }
                }
            }
            }
        }
        }
    """
    time.sleep(10)
    requests.put(
        url='http://elasticsearch:9200/movies',
        headers={
            'Content-Type': 'application/json',
        },
        data=request_body,
    )

    time.sleep(1)
    p = subprocess.Popen(['python', '/opt/app/src/sqlite_to_postgres/load_data.py'])
    p.wait()

    subprocess.run(['python', '/opt/app/src/main.py'])


if __name__ == '__main__':
    ps_connect = PS().dict()
    migrate_and_start(ps_connect)
