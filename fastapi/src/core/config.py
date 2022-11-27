import os
from logging import config as logging_config

from core.logger import LOGGING
from pydantic import BaseSettings, Field


# Применяем настройки логирования
logging_config.dictConfig(LOGGING)


class PS(BaseSettings):
    dbname: str = Field('', env='POSTGRES_NAME')
    user: str = Field('', env='POSTGRES_USER')
    password: str = Field('', env='POSTGRES_PASSWORD')
    host: str = Field('db', env='DB_HOST')
    port: int = Field(5432, env='DB_PORT')


# Название проекта. Используется в Swagger-документации
PROJECT_NAME = os.getenv('PROJECT_NAME', 'movies')

# Настройки Redis
REDIS_HOST = os.getenv('REDIS_HOST', '127.0.0.1')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))

# Настройки Elasticsearch
ELASTIC_HOST = os.getenv('ELASTIC_HOST', '127.0.0.1')
ELASTIC_PORT = int(os.getenv('ELASTIC_PORT', 9200))
ES_USER = os.getenv('ES_USER', 'elastic')
ES_PASSWORD = os.getenv('ES_PASSWORD', 'changeme')

# Корень проекта
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
