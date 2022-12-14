import os
from logging import config as logging_config

from core.logger import LOGGING
from pydantic import BaseSettings, Field


# Применяем настройки логирования
logging_config.dictConfig(LOGGING)


class DbSettings(BaseSettings):
    dbname: str = Field('', env='POSTGRES_NAME')
    user: str = Field('', env='POSTGRES_USER')
    password: str = Field('', env='POSTGRES_PASSWORD')
    host: str = Field('db', env='DB_HOST')
    port: int = Field(5432, env='DB_PORT')

class ProjectSettings(BaseSettings):
    PROJECT_NAME = Field('movies', env='PROJECT_NAME')
    # Настройки Redis
    REDIS_HOST = Field('127.0.0.1', env='REDIS_HOST')
    REDIS_PORT = Field(6379, env='REDIS_PORT')
    # Настройки Elasticsearch
    ELASTIC_HOST = Field('127.0.0.1', env='ELASTIC_HOST')
    ELASTIC_PORT = Field(9200, env='ELASTIC_PORT')
    ES_USER = Field('elastic', env='ES_USER')
    ES_PASSWORD = Field('changeme', env='ES_PASSWORD')
    CACHE_EXPIRE_IN_SECONDS = Field(300, env='CACHE_EXPIRE_IN_SECONDS')
    BASE_DIR = Field(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

