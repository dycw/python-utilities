import gzip
from pickle import dump, load
from typing import Any

from beartype import beartype

from utilities.atomicwrites import writer
from utilities.pathlib import PathLike


@beartype
def read_pickle(path: PathLike, /) -> Any:
    """Read an object from disk."""
    with gzip.open(path, mode="rb") as gz:
        return load(gz)  # noqa: S301


@beartype
def write_pickle(obj: Any, path: PathLike, /, *, overwrite: bool = False) -> None:
    """Write an object to disk."""
    with writer(path, overwrite=overwrite) as temp, gzip.open(temp, mode="wb") as gz:
        dump(obj, gz)
