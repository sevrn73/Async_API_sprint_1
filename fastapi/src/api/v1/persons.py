from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from typing import List
from services.person import PersonService, get_person_service
from core.exception_detail import ExceptionDetail

router = APIRouter()


class Person(BaseModel):
    id: str
    name: str


@router.get('/{person_id}', response_model=Person)
async def person_details(
    person_id: str = Query(default='e039eedf-4daf-452a-bf92-a0085c68e156'),
    person_service: PersonService = Depends(get_person_service),
) -> Person:
    person = await person_service.get_by_id('persons', person_id)
    if not person:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail=ExceptionDetail.PersonDetails)

    return Person(id=person.id, name=person.name)


@router.get('/persons/', response_model=List[Person])
async def persons_details(
    sort: bool = False,
    page_number: int = Query(default=1, alias='page[number]', ge=1),
    page_size: int = Query(default=5, alias='page[size]', ge=1),
    person_service: PersonService = Depends(get_person_service),
) -> List[Person]:

    persons = await person_service.get_page_number('persons', sort, page_number, page_size)
    if not persons:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail=ExceptionDetail.PersonsDetails)

    return persons
