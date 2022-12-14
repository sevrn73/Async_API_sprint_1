from typing import Optional

from models.base_model import ESModel


class ESGenre(ESModel):
    id: str
    genre: str
    description: Optional[str]
