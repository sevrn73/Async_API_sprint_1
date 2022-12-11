from typing import Optional, List

from models.base_model import ESModel


class ESGenre(ESModel):
    id: str
    name: str
    description: str
