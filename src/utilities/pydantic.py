from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, override

from pydantic import BaseModel

from utilities.atomicwrites import writer

if TYPE_CHECKING:
    from utilities.types import PathLike


class HashableBaseModel(BaseModel):
    """Subclass of BaseModel which is hashable."""

    @override
    def __hash__(self) -> int:
        return hash((type(self), *self.__dict__.values()))


def load_model[T: BaseModel](model: type[T], path: PathLike, /) -> T:
    path = Path(path)
    try:
        return model.model_validate_json(path.read_text())
    except FileNotFoundError:
        raise _LoadModelFileNotFoundError(model=model, path=path) from None
    except IsADirectoryError:  # skipif-not-windows
        raise _LoadModelIsADirectoryError(model=model, path=path) from None


@dataclass(kw_only=True, slots=True)
class LoadModelError(Exception):
    model: type[BaseModel]
    path: Path


@dataclass(kw_only=True, slots=True)
class _LoadModelFileNotFoundError(LoadModelError):
    @override
    def __str__(self) -> str:
        return f"Unable to load {self.model}; path {str(self.path)!r} must exist."


@dataclass(kw_only=True, slots=True)
class _LoadModelIsADirectoryError(LoadModelError):
    @override
    def __str__(self) -> str:
        return f"Unable to load {self.model}; path {str(self.path)!r} must not be a directory."  # skipif-not-windows


def save_model(model: BaseModel, path: PathLike, /, *, overwrite: bool = False) -> None:
    with writer(path, overwrite=overwrite) as temp:
        _ = temp.write_text(model.model_dump_json())


__all__ = ["HashableBaseModel", "LoadModelError", "load_model", "save_model"]
