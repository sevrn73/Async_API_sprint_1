from typing import Optional, List
from models.base_model import ESModel


class ESPerson(ESModel):
    id: str
    name: str
    film_ids: List[str]


class ESActor(ESPerson):
    pass


class ESDirector(ESPerson):
    pass


class ESWriter(ESPerson):
    pass
