from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from typing import List
from services.genre import GenreService, get_genre_service
from core.exception_detail import ExceptionDetail

router = APIRouter()


class Genre(BaseModel):
    id: str
    genre: str


@router.get('/{genre_id}', response_model=Genre)
async def genre_details(
    genre_id: str = Query(default='6c162475-c7ed-4461-9184-001ef3d9f26e'),
    genre_service: GenreService = Depends(get_genre_service),
) -> Genre:
    genre = await genre_service.get_by_id('genres', genre_id)
    if not genre:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail=ExceptionDetail.GenreDetails)

    return Genre(id=genre.id, genre=genre.genre)


@router.get('/genres/', response_model=List[Genre])
async def genres_details(
    sort: bool = False,
    page_number: int = Query(default=1, alias='page[number]', ge=1),
    page_size: int = Query(default=5, alias='page[size]', ge=1),
    genre_service: GenreService = Depends(get_genre_service),
) -> List[Genre]:

    genres = await genre_service.get_page_number('genres', sort, page_number, page_size)
    if not genres:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail=ExceptionDetail.GenresDetails)

    return genres
