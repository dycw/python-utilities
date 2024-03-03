from __future__ import annotations

import gzip
from pickle import dump, load
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from utilities.types import PathLike


def read_pickle(path: PathLike, /) -> Any:
    """Read an object from disk."""
    with gzip.open(path, mode="rb") as gz:
        return load(gz)  # noqa: S301


def write_pickle(obj: Any, path: PathLike, /, *, overwrite: bool = False) -> None:
    """Write an object to disk."""
    from utilities.atomicwrites import writer

    with writer(path, overwrite=overwrite) as temp, gzip.open(temp, mode="wb") as gz:
        dump(obj, gz)


__all__ = ["read_pickle", "write_pickle"]
