from __future__ import annotations

from typing import TypeVar

from pydantic import BaseModel
from typing_extensions import override

from utilities.atomicwrites import writer
from utilities.pathvalidate import valid_path
from utilities.types import PathLike

_BM = TypeVar("_BM", bound=BaseModel)


class HashableBaseModel(BaseModel):
    """Subclass of BaseModel which is hashable."""

    @override
    def __hash__(self) -> int:
        return hash((type(self), *self.__dict__.values()))


def load_model(model: type[_BM], path: PathLike, /) -> _BM:
    with valid_path(path).open() as fh:
        return model.model_validate_json(fh.read())


def save_model(model: BaseModel, path: PathLike, /, *, overwrite: bool = False) -> None:
    with writer(path, overwrite=overwrite) as temp, temp.open(mode="w") as fh:
        _ = fh.write(model.model_dump_json())


__all__ = ["HashableBaseModel", "load_model", "save_model"]
