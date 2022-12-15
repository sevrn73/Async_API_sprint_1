from functools import lru_cache
from typing import Optional, List
from aioredis import Redis
from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends

from services.base_service import BaseService
from db.elastic import get_elastic
from db.redis import get_redis
from models.film import ESFilm


class FilmService(BaseService):
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        super().__init__(redis, elastic)
        self.model = ESFilm

    async def get_page_number(
        self, es_index: str, rating_filter: float, sort: bool, page_number: int, page_size: int
    ) -> Optional[List[ESFilm]]:
        data = await self._many_data_from_cache(
            f'{es_index}::rating_filter::{rating_filter}::sort::{sort}::page_number::{page_number}::page_size::{page_size}'
        )
        if not data:
            data = await self._get_data_from_elastic(es_index, rating_filter, sort, page_number, page_size)
            if not data:
                return None
            await self._put_many_data_to_cache(
                f'{es_index}::rating_filter::{rating_filter}::sort::{sort}::page_number::{page_number}::page_size::{page_size}',
                data,
            )
        return data

    async def _get_data_from_elastic(
        self, es_index: str, rating_filter: float, sort: bool, page_number: int, data_on_page: int
    ) -> Optional[List[ESFilm]]:
        try:
            data = await self.elastic.search(
                index=es_index,
                from_=page_number,
                body={
                    'query': {
                        'range': {
                            'imdb_rating': {
                                'gte': rating_filter if rating_filter else 0,
                            }
                        }
                    }
                },
                size=data_on_page,
                sort=f"imdb_rating:{'asc' if sort else 'desc'}",
            )
        except NotFoundError:
            return None
        return [self.model(**_['_source']) for _ in data['hits']['hits']]


@lru_cache()
def get_film_service(
    redis: Redis = Depends(get_redis),
    elastic: AsyncElasticsearch = Depends(get_elastic),
) -> FilmService:
    return FilmService(redis, elastic)
