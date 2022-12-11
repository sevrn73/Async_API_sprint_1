from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel


class Person(BaseModel):
    person_id: str
    role: str
    full_name: str


class FilmworkData(BaseModel):
    fw_id: str
    title: str
    description: Optional[str]
    rating: Optional[float]
    type: str
    created: datetime
    modified: datetime
    persons: Optional[List[Person]]
    genres: Optional[List[str]]


class ESGenreData(BaseModel):
    id: str
    genre: str
    description: Optional[str]

class ESPersonData(BaseModel):
    id: str
    name: str

class ESFilmworkData(BaseModel):
    id: str
    imdb_rating: Optional[float]
    genre: List[str]
    title: str
    description: Optional[str]
    director: List[str]
    actors_names: List[str]
    writers_names: List[str]
    actors: List[ESPersonData]
    writers: List[ESPersonData]
