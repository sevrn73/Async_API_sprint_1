from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional
from services.film import FilmService, get_film_service, FilmsService, get_films_service

router = APIRouter()


class Film(BaseModel):
    id: str
    title: str
    imdb_rating: Optional[float]


@router.get('/{film_id}', response_model=Film)
async def film_details(film_id: str, film_service: FilmService = Depends(get_film_service)) -> Film:
    film = await film_service.get_by_id(film_id)
    if not film:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='film not found')

    return Film(id=film.id, title=film.title, imdb_rating=film.imdb_rating)


@router.get('/movies/', response_model=List[Film])
async def movies_details(
    rating_filter: float = None,
    sort: bool = False,
    films_ids: List[str] = Query(
        default=['0312ed51-8833-413f-bff5-0e139c11264a', '025c58cd-1b7e-43be-9ffb-8571a613579b']
    ),
    film_service: FilmsService = Depends(get_films_service),
) -> List[Film]:

    films = await film_service.get_by_ids(rating_filter, sort, films_ids)
    if not films:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='films not found')

    return (
        sorted(films, key=lambda film: film.imdb_rating)
        if not sort
        else sorted(films, key=lambda film: film.imdb_rating)[::-1]
    )
