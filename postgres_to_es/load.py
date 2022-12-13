from typing import List, Tuple
from elasticsearch import Elasticsearch, helpers
from p_schemas import ESFilmworkData, ESPersonData, ESGenreData
from backoff import backoff


class ESLoad:
    def __init__(self, es_host: str, es_user: str, es_password: str):
        """
        Parameters
        ----------
        :param es_host: host ElasticSearch
        :param es_user: username ElasticSearch
        :param es_password: пароль ElasticSearch
        ----------
        """
        self.es = Elasticsearch(es_host, basic_auth=(es_user, es_password), verify_certs=False)

    @staticmethod
    @backoff()
    def send_data(es: Elasticsearch, es_data: List[ESFilmworkData], index_name:str):
        """
        Метод выгрузки данных в ElasticSearch
        Parameters
        ----------
        :param es: клиент для соединения с ElasticSearch
        :param es_data: данные, выгружаемые в ElasticSearch
        :param index_name: индекс ElasticSearch, в который выгружаем данные
        ----------
        """

        query = [{"_index": index_name, "_id": data.id, "_source": data.dict()} for data in es_data]
        helpers.bulk(es, query)
