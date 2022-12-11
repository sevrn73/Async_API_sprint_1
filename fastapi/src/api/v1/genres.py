from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from typing import List
from services.genre import GenreService, get_genre_service

router = APIRouter()


class Genre(BaseModel):
    id: str
    genre: str


@router.get('/{genre_id}', response_model=Genre)
async def genre_details(genre_id: str, genre_service: GenreService = Depends(get_genre_service)) -> Genre:
    genre = await genre_service.get_by_id(genre_id)
    if not genre:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='genre not found')

    return Genre(id=genre.id, genre=genre.genre)


@router.get('/genres/', response_model=List[Genre])
async def genres_details(
    genres_ids: List[str] = Query(
        default=['6c162475-c7ed-4461-9184-001ef3d9f26e', '237fd1e4-c98e-454e-aa13-8a13fb7547b5']
    ),
    genre_service: GenreService = Depends(get_genre_service),
) -> List[Genre]:

    result = []
    for genre_id in genres_ids:
        genre = await genre_service.get_by_id(genre_id)
        result.append(Genre(id=genre.id, genre=genre.genre))
    if not result:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='genres not found')

    return result
