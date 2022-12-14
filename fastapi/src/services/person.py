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
from models.person import ESPerson



class PersonService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    async def get_by_id(self, person_id: str) -> Optional[ESPerson]:
        person = await self._person_from_cache(person_id)
        if not person:
            person = await self._get_person_from_elastic(person_id)
            if not person:
                return None
            await self._put_person_to_cache(person)

        return person

    async def _get_person_from_elastic(self, person_id: str) -> Optional[ESPerson]:
        try:
            doc = await self.elastic.get('persons', person_id)
        except NotFoundError:
            return None
        return ESPerson(**doc['_source'])

    async def _person_from_cache(self, person_id: str) -> Optional[ESPerson]:
        data = await self.redis.get(person_id)
        if not data:
            return None

        person = ESPerson.parse_raw(data)
        return person

    async def _put_person_to_cache(self, person: ESPerson):
        await self.redis.set(person.id, person.json(), expire=ProjectSettings().CACHE_EXPIRE_IN_SECONDS)

    async def get_page_number(self, sort: bool, page_number: int, films_on_page: int) -> Optional[ESPerson]:
        persons = await self._persons_from_cache(f'{sort}_{page_number}_{films_on_page}')
        if not persons:
            persons = await self._get_persons_from_elastic(sort, page_number, films_on_page)
            if not persons:
                return None
            await self._put_persons_to_cache(persons, f'{sort}_{page_number}_{films_on_page}')

        return persons

    async def _get_persons_from_elastic(self, sort: bool, page_number: int, films_on_page: int) -> Optional[ESPerson]:
        try:
            persons = await self.elastic.search(
                index='persons',
                from_=page_number,
                size=films_on_page,
                sort=f"name.keyword:{'asc' if sort else 'desc'}",
            )
        except NotFoundError:
            return None
        return [ESPerson(**_['_source']) for _ in persons['hits']['hits']]

    async def _persons_from_cache(self, redis_key: str) -> Optional[ESPerson]:
        data = await self.redis.get(redis_key)
        if not data:
            return None

        persons = parse_raw_as(List[ESPerson], data)
        return persons

    async def _put_persons_to_cache(self, persons: List[ESPerson], redis_key: str):
        await self.redis.set(
            redis_key, json.dumps(persons, default=pydantic_encoder), expire=ProjectSettings().CACHE_EXPIRE_IN_SECONDS
        )


@lru_cache()
def get_person_service(
    redis: Redis = Depends(get_redis),
    elastic: AsyncElasticsearch = Depends(get_elastic),
) -> PersonService:
    return PersonService(redis, elastic)
