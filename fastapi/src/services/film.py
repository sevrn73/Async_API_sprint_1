import json
from functools import lru_cache
from typing import Optional, List
from pydantic import parse_raw_as
from pydantic.json import pydantic_encoder
from aioredis import Redis
from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends

from db.elastic import get_elastic
from db.redis import get_redis
from models.film import ESFilm

FILM_CACHE_EXPIRE_IN_SECONDS = 60 * 5  # 5 минут


class FilmService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    async def get_by_id(self, film_id: str) -> Optional[ESFilm]:
        film = await self._film_from_cache(film_id)
        if not film:
            film = await self._get_film_from_elastic(film_id)
            if not film:
                return None
            await self._put_film_to_cache(film)

        return film

    async def _get_film_from_elastic(self, film_id: str) -> Optional[ESFilm]:
        try:
            doc = await self.elastic.get('movies', film_id)
        except NotFoundError:
            return None
        return ESFilm(**doc['_source'])

    async def _film_from_cache(self, film_id: str) -> Optional[ESFilm]:
        data = await self.redis.get(film_id)
        if not data:
            return None

        film = ESFilm.parse_raw(data)
        return film

    async def _put_film_to_cache(self, film: ESFilm):
        await self.redis.set(film.id, film.json(), expire=FILM_CACHE_EXPIRE_IN_SECONDS)


class FilmsService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    async def get_by_ids(self, rating_filter: float, sort: bool, films_ids: List[str]) -> Optional[List[ESFilm]]:
        films = await self._films_from_cache(f'{rating_filter}_{sort}_{films_ids}')
        if not films:
            films = await self._get_films_from_elastic(rating_filter, films_ids)
            if not films:
                return None
            await self._put_films_to_cache(films, f'{rating_filter}_{sort}_{films_ids}')

        return films

    async def _get_films_from_elastic(self, rating_filter: float, films_ids: List[str]) -> Optional[List[ESFilm]]:
        result = []
        for film_id in films_ids:
            try:
                film = await self.elastic.get('movies', film_id)
            except NotFoundError:
                continue
            if rating_filter:
                if film['_source']['imdb_rating'] >= rating_filter:
                    result.append(ESFilm(**film['_source']))
            else:
                result.append(ESFilm(**film['_source']))
        return result

    async def _films_from_cache(self, redis_key: str) -> Optional[List[ESFilm]]:
        data = await self.redis.get(redis_key)
        if not data:
            return None

        films = parse_raw_as(List[ESFilm], data)
        return films

    async def _put_films_to_cache(self, films: List[ESFilm], redis_key: str):
        await self.redis.set(
            redis_key, json.dumps(films, default=pydantic_encoder), expire=FILM_CACHE_EXPIRE_IN_SECONDS
        )


@lru_cache()
def get_film_service(
    redis: Redis = Depends(get_redis),
    elastic: AsyncElasticsearch = Depends(get_elastic),
) -> FilmService:
    return FilmService(redis, elastic)


@lru_cache()
def get_films_service(
    redis: Redis = Depends(get_redis),
    elastic: AsyncElasticsearch = Depends(get_elastic),
) -> FilmsService:
    return FilmsService(redis, elastic)
