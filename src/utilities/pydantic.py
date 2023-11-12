from __future__ import annotations

from pydantic import BaseModel
from typing_extensions import override


class HashableBaseModel(BaseModel):
    """Subclass of BaseModel which is hashable."""

    @override
    def __hash__(self) -> int:
        return hash((type(self), *self.__dict__.values()))
