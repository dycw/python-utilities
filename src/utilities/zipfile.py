from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING
from zipfile import ZipFile

from utilities.tempfile import TemporaryDirectory

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from utilities.types import PathLike


@contextmanager
def yield_zip_file_contents(path: PathLike, /) -> Iterator[list[Path]]:
    """Yield the contents of a zipfile in a temporary directory."""
    with ZipFile(path) as zf, TemporaryDirectory() as temp:
        zf.extractall(path=temp)
        yield list(temp.iterdir())
    _ = zf  # make coverage understand this is returned


__all__ = ["yield_zip_file_contents"]
