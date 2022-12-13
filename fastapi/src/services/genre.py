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
from models.genre import ESGenre

FILM_CACHE_EXPIRE_IN_SECONDS = 60 * 5  # 5 минут


class GenreService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    async def get_by_id(self, genre_id: str) -> Optional[ESGenre]:
        genre = await self._genre_from_cache(genre_id)
        if not genre:
            genre = await self._get_genre_from_elastic(genre_id)
            if not genre:
                return None
            await self._put_genre_to_cache(genre)

        return genre

    async def _get_genre_from_elastic(self, genre_id: str) -> Optional[ESGenre]:
        try:
            doc = await self.elastic.get('genres', genre_id)
        except NotFoundError:
            return None
        return ESGenre(**doc['_source'])

    async def _genre_from_cache(self, genre_id: str) -> Optional[ESGenre]:
        data = await self.redis.get(genre_id)
        if not data:
            return None

        genre = ESGenre.parse_raw(data)
        return genre

    async def _put_genre_to_cache(self, genre: ESGenre):
        await self.redis.set(genre.id, genre.json(), expire=FILM_CACHE_EXPIRE_IN_SECONDS)


class GenresService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    async def get_page_number(self, sort: bool, page_number: int, films_on_page: int) -> Optional[ESGenre]:
        genres = await self._genres_from_cache(f'{sort}_{page_number}_{films_on_page}')
        if not genres:
            genres = await self._get_genres_from_elastic(sort, page_number, films_on_page)
            if not genres:
                return None
            await self._put_genres_to_cache(genres, f'{sort}_{page_number}_{films_on_page}')

        return genres

    async def _get_genres_from_elastic(self, sort: bool, page_number: int, films_on_page: int) -> Optional[ESGenre]:
        try:
            genres = await self.elastic.search(
                index='genres',
                from_=page_number,
                size=films_on_page,
                sort=f"genre.keyword:{'asc' if sort else 'desc'}",
            )
        except NotFoundError:
            return None
        return [ESGenre(**_['_source']) for _ in genres['hits']['hits']]

    async def _genres_from_cache(self, redis_key: str) -> Optional[ESGenre]:
        data = await self.redis.get(redis_key)
        if not data:
            return None

        genres = parse_raw_as(List[ESGenre], data)
        return genres

    async def _put_genres_to_cache(self, genres: List[ESGenre], redis_key: str):
        await self.redis.set(
            redis_key, json.dumps(genres, default=pydantic_encoder), expire=FILM_CACHE_EXPIRE_IN_SECONDS
        )


@lru_cache()
def get_genre_service(
    redis: Redis = Depends(get_redis),
    elastic: AsyncElasticsearch = Depends(get_elastic),
) -> GenreService:
    return GenreService(redis, elastic)


@lru_cache()
def get_genres_service(
    redis: Redis = Depends(get_redis),
    elastic: AsyncElasticsearch = Depends(get_elastic),
) -> GenresService:
    return GenresService(redis, elastic)