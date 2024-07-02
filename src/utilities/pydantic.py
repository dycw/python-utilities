from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeVar

from pydantic import BaseModel
from typing_extensions import override

from utilities.pathlib import ensure_path

if TYPE_CHECKING:
    from pathlib import Path

    from utilities.types import PathLike

_BM = TypeVar("_BM", bound=BaseModel)


class HashableBaseModel(BaseModel):
    """Subclass of BaseModel which is hashable."""

    @override
    def __hash__(self) -> int:
        return hash((type(self), *self.__dict__.values()))


def load_model(model: type[_BM], path: PathLike, /) -> _BM:
    path = ensure_path(path)
    try:
        with path.open() as fh:
            return model.model_validate_json(fh.read())
    except FileNotFoundError:
        raise _LoadModelFileNotFoundError(model=model, path=path) from None
    except IsADirectoryError:  # pragma: os-ne-windows
        raise _LoadModelIsADirectoryError(model=model, path=path) from None


@dataclass(kw_only=True)
class LoadModelError(Exception):
    model: type[BaseModel]
    path: Path


@dataclass(kw_only=True)
class _LoadModelFileNotFoundError(LoadModelError):
    @override
    def __str__(self) -> str:
        return f"Unable to load {self.model}; path {str(self.path)!r} must exist."


@dataclass(kw_only=True)
class _LoadModelIsADirectoryError(LoadModelError):
    @override
    def __str__(self) -> str:
        return f"Unable to load {self.model}; path {str(self.path)!r} must not be a directory."  # pragma: os-ne-windows


def save_model(model: BaseModel, path: PathLike, /, *, overwrite: bool = False) -> None:
    from utilities.atomicwrites import writer

    with writer(path, overwrite=overwrite) as temp, temp.open(mode="w") as fh:
        _ = fh.write(model.model_dump_json())


__all__ = ["HashableBaseModel", "LoadModelError", "load_model", "save_model"]
