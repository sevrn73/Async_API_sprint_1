from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import List
from services.person import PersonService, get_person_service

router = APIRouter()


class Person(BaseModel):
    id: str
    name: str


@router.get('/{person_id}', response_model=Person)
async def person_details(
    person_id: str = 'e039eedf-4daf-452a-bf92-a0085c68e156',
    person_service: PersonService = Depends(get_person_service),
) -> Person:
    person = await person_service.get_by_id(person_id)
    if not person:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='person not found')

    return Person(id=person.id, name=person.name)


@router.get('/persons/', response_model=List[Person])
async def persons_details(
    sort: bool = False,
    page_number: int = 1,
    persons_on_page: int = 5,
    person_service: PersonService = Depends(get_person_service),
) -> List[Person]:

    persons = await person_service.get_page_number(sort, page_number, persons_on_page)
    if not persons:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='persons not found')

    return persons
