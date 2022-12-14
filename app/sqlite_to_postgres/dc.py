from dataclasses import dataclass, field


@dataclass
class TimeStampedMixin:
    created: str
    modified: str


@dataclass
class UUIDMixin:
    id: str


@dataclass
class Genre(UUIDMixin, TimeStampedMixin):
    name: str
    description: str


@dataclass
class Person(UUIDMixin, TimeStampedMixin):
    full_name: str


@dataclass
class Filmwork(UUIDMixin, TimeStampedMixin):
    title: str
    description: str
    creation_date: str
    type: str
    certificate: str = field(default='')
    rating: float = field(default=0.0)
    file_path: str = field(default=None)


@dataclass
class GenreFilmWork(UUIDMixin):
    created: str
    film_work_id: str
    genre_id: str


@dataclass
class PersonFilmWork(UUIDMixin):
    role: str
    created: str
    film_work_id: str
    person_id: str
