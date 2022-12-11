from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from services.person import PersonService, get_person_service

router = APIRouter()


class Person(BaseModel):
    id: str
    name: str


@router.get('/{person_id}', response_model=Person)
async def person_details(person_id: str, genre_service: PersonService = Depends(get_person_service)) -> Person:
    person = await genre_service.get_by_id(person_id)
    if not person:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='person not found')

    return Person(id=person.id, title=person.name)
