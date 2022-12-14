import json
from functools import lru_cache
from typing import Optional, List
from pydantic import parse_raw_as
from pydantic.json import pydantic_encoder
from aioredis import Redis
from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends

from core.config import ProjectSettings
from db.elastic import get_elastic
from db.redis import get_redis
from models.base_model import BaseModel


class BaseService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic
        self.model = BaseModel

    async def get_by_id(self, data_id: str) -> Optional[BaseModel]:
        data = await self._data_from_cache(data_id)
        if not data:
            data = await self._get_from_elastic(data_id)
            if not data:
                return None
            await self._put_data_to_cache(data)
        return data

    async def _get_from_elastic(self, data_id: str) -> Optional[BaseModel]:
        try:
            doc = await self.elastic.get('movies', data_id)
        except NotFoundError:
            return None
        return self.model(**doc['_source'])

    async def _data_from_cache(self, data_id: str) -> Optional[BaseModel]:
        data = await self.redis.get(data_id)
        if not data:
            return None
        data = self.model.parse_raw(data)
        return data

    async def _put_data_to_cache(self, data: BaseModel):
        await self.redis.set(data.id, data.json(), expire=ProjectSettings().CACHE_EXPIRE_IN_SECONDS)

    async def get_page_number(self, sort: bool, page_number: int, films_on_page: int) -> Optional[BaseModel]:
        data = await self._data_from_cache(f'{sort}_{page_number}_{films_on_page}')
        if not data:
            data = await self._get_data_from_elastic(sort, page_number, films_on_page)
            if not data:
                return None
            await self._put_many_data_to_cache(data, f'{sort}_{page_number}_{films_on_page}')
        return data

    async def _get_data_from_elastic(
        self, rating_filter: float, sort: bool, page_number: int, data_on_page: int
    ) -> Optional[List[BaseModel]]:
        try:
            data = await self.elastic.search(
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
                size=data_on_page,
                sort=f"imdb_rating:{'asc' if sort else 'desc'}",
            )
        except NotFoundError:
            return None
        return [self.model(**_['_source']) for _ in data['hits']['hits']]

    async def _many_data_from_cache(self, redis_key: str) -> Optional[List[BaseModel]]:
        data = await self.redis.get(redis_key)
        if not data:
            return None
        data = parse_raw_as(List[self.model], data)
        return data

    async def _put_many_data_to_cache(self, data: List[BaseModel], redis_key: str):
        await self.redis.set(
            redis_key, json.dumps(data, default=pydantic_encoder), expire=ProjectSettings().CACHE_EXPIRE_IN_SECONDS
        )