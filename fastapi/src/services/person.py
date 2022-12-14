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
from models.person import ESPerson


class PersonService(BaseService):
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        super().__init__(redis, elastic)
        self.model = ESPerson

    async def _get_from_elastic(self, person_id: str) -> Optional[ESPerson]:
        try:
            doc = await self.elastic.get('persons', person_id)
        except NotFoundError:
            return None
        return self.model(**doc['_source'])

    async def _get_data_from_elastic(self, sort: bool, page_number: int, films_on_page: int) -> Optional[ESPerson]:
        try:
            persons = await self.elastic.search(
                index='persons',
                from_=page_number,
                size=films_on_page,
                sort=f"name.keyword:{'asc' if sort else 'desc'}",
            )
        except NotFoundError:
            return None
        return [self.model(**_['_source']) for _ in persons['hits']['hits']]


@lru_cache()
def get_person_service(
    redis: Redis = Depends(get_redis),
    elastic: AsyncElasticsearch = Depends(get_elastic),
) -> PersonService:
    return PersonService(redis, elastic)
