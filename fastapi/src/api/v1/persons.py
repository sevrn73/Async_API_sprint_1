from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from typing import List
from services.person import PersonService, get_person_service

router = APIRouter()


class Person(BaseModel):
    id: str
    name: str


@router.get('/{person_id}', response_model=Person)
async def person_details(person_id: str, person_service: PersonService = Depends(get_person_service)) -> Person:
    person = await person_service.get_by_id(person_id)
    if not person:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='person not found')

    return Person(id=person.id, name=person.name)


@router.get('/persons/', response_model=List[Person])
async def persons_details(
    presons_ids: List[str] = Query(
        default=['e039eedf-4daf-452a-bf92-a0085c68e156', '3217bc91-bcfc-44eb-a609-82d228115c50']
    ),
    person_service: PersonService = Depends(get_person_service),
) -> List[Person]:

    result = []
    for person_id in presons_ids:
        preson = await person_service.get_by_id(person_id)
        result.append(Person(id=preson.id, name=preson.name))
    if not result:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='presons not found')

    return result
