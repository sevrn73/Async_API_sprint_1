from typing import Optional

from models.base_model import ESModel


class ESFilm(ESModel):
    id: str
    imdb_rating: Optional[float]
    title: str
    description: Optional[str]
