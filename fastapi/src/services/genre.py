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
from models.genre import ESGenre


class GenreService(BaseService):
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        super().__init__(redis, elastic)
        self.model = ESGenre

    async def _get_from_elastic(self, genre_id: str) -> Optional[ESGenre]:
        try:
            doc = await self.elastic.get('genres', genre_id)
        except NotFoundError:
            return None
        return self.model(**doc['_source'])

    async def _get_data_from_elastic(self, sort: bool, page_number: int, films_on_page: int) -> Optional[ESGenre]:
        try:
            genres = await self.elastic.search(
                index='genres',
                from_=page_number,
                size=films_on_page,
                sort=f"genre.keyword:{'asc' if sort else 'desc'}",
            )
        except NotFoundError:
            return None
        return [self.model(**_['_source']) for _ in genres['hits']['hits']]


@lru_cache()
def get_genre_service(
    redis: Redis = Depends(get_redis),
    elastic: AsyncElasticsearch = Depends(get_elastic),
) -> GenreService:
    return GenreService(redis, elastic)
