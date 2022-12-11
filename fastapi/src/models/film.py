from typing import Optional, List

from models.base_model import ESModel
from models.person import ESPerson


class ESFilm(ESModel):
    id: str
    title: str
    description: Optional[str]
