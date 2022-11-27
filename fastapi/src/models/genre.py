from typing import Optional, List

from base_model import ESModel


class ESGenre(ESModel):
    id: str
    name: str
    description: str
    film_ids: List[str]