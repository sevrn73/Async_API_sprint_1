from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from services.film import FilmService, get_film_service

router = APIRouter()


class Film(BaseModel):
    id: str
    title: str
    imdb_rating: Optional[float]


@router.get(
    '/{film_id}',
    response_model=Film,
    summary='Поиск кинопроизведений по id',
    description='поиск кинопроизведений по id',
    response_description='Название и рейтинг фильма',
)
async def film_details(
    film_id: str = '0312ed51-8833-413f-bff5-0e139c11264a', film_service: FilmService = Depends(get_film_service)
) -> Film:
    film = await film_service.get_by_id(film_id)
    if not film:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='film not found')

    return Film(id=film.id, title=film.title, imdb_rating=film.imdb_rating)


@router.get(
    '/movies/',
    response_model=List[Film],
    summary='Поиск кинопроизведений по страницам',
    description='поиск списка кинопроизведений по номеру страницы',
    response_description='Список с названием и рейтингом фильма',
)
async def movies_details(
    rating_filter: float = None,
    sort: bool = False,
    page_number: int = 1,
    films_on_page: int = 20,
    film_service: FilmService = Depends(get_film_service),
) -> List[Film]:

    films = await film_service.get_page_number(rating_filter, sort, page_number, films_on_page)
    if not films:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='films not found')

    return films
