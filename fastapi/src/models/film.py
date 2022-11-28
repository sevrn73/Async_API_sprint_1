from typing import Optional, List

from models.base_model import ESModel
from models.person import ESPerson


class ESFilm(ESModel):
    id: str
    # imdb_rating: Optional[float]
    # genre: List[str]  # [ESGenre] ?
    title: str
    description: Optional[str]
    # director: List[str]
    # actors_names: List[str]
    # writers_names: List[str]
    # actors: List[ESPerson]
    # writers: List[ESPerson]
