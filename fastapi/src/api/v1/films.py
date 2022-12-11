from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import List
from services.film import FilmService, get_film_service

router = APIRouter()


class Film(BaseModel):
    id: str
    title: str


@router.get('/{film_id}', response_model=Film)
async def film_details(film_id: str, film_service: FilmService = Depends(get_film_service)) -> Film:
    film = await film_service.get_by_id(film_id)
    if not film:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='film not found')

    return Film(id=film.id, title=film.title)


@router.get('/movies/', response_model=List[Film])
async def film_details(films_ids: List[str], film_service: FilmService = Depends(get_film_service)) -> List[Film]:

    result = []
    for film_id in films_ids:
        film = await film_service.get_by_id(film_id)
        result.append(Film(id=film.id, title=film.title))
    if not result:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='films not found')

    return result
