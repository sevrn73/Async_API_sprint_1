from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from typing import List
from services.genre import GenreService, get_genre_service, GenresService, get_genres_service

router = APIRouter()


class Genre(BaseModel):
    id: str
    genre: str


@router.get('/{genre_id}', response_model=Genre)
async def genre_details(
    genre_id: str = '6c162475-c7ed-4461-9184-001ef3d9f26e', genre_service: GenreService = Depends(get_genre_service)
) -> Genre:
    genre = await genre_service.get_by_id(genre_id)
    if not genre:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='genre not found')

    return Genre(id=genre.id, genre=genre.genre)


@router.get('/genres/', response_model=List[Genre])
async def genres_details(
    sort: bool = False,
    page_number: int = 1,
    genres_on_page: int = 5,
    genre_service: GenresService = Depends(get_genres_service),
) -> List[Genre]:

    genres = await genre_service.get_page_number(sort, page_number, genres_on_page)
    if not genres:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='genres not found')

    return genres
