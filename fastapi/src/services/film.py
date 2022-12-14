import json
from functools import lru_cache
from typing import Optional, List
from pydantic import parse_raw_as
from pydantic.json import pydantic_encoder
from aioredis import Redis
from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends

from services.base_service import BaseService
from core.config import ProjectSettings
from db.elastic import get_elastic
from db.redis import get_redis
from models.film import ESFilm


class FilmService(BaseService):
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        super().__init__(redis, elastic)
        self.model = ESFilm

    async def _get_from_elastic(self, film_id: str) -> Optional[ESFilm]:
        try:
            doc = await self.elastic.get('movies', film_id)
        except NotFoundError:
            return None
        return self.model(**doc['_source'])

    async def _get_data_from_elastic(
        self, rating_filter: float, sort: bool, page_number: int, films_on_page: int
    ) -> Optional[List[ESFilm]]:
        try:
            films = await self.elastic.search(
                index='movies',
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
                size=films_on_page,
                sort=f"imdb_rating:{'asc' if sort else 'desc'}",
            )
        except NotFoundError:
            return None
        return [self.model(**_['_source']) for _ in films['hits']['hits']]

    async def get_page_number(
        self, rating_filter: float, sort: bool, page_number: int, data_on_page: int
    ) -> Optional[List[ESFilm]]:
        data = await self._many_data_from_cache(f'{rating_filter}_{sort}_{page_number}_{data_on_page}')
        if not data:
            data = await self._get_data_from_elastic(rating_filter, sort, page_number, data_on_page)
            if not data:
                return None
            await self._put_many_data_to_cache(data, f'{rating_filter}_{sort}_{page_number}_{data_on_page}')
        return data

@lru_cache()
def get_film_service(
    redis: Redis = Depends(get_redis),
    elastic: AsyncElasticsearch = Depends(get_elastic),
) -> FilmService:
    return FilmService(redis, elastic)
