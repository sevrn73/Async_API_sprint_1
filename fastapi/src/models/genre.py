from typing import Optional, List

from models.base_model import ESModel


class ESGenre(ESModel):
    id: str
    genre: str
    description: Optional[str]
