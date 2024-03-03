from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from zipfile import ZipFile

from utilities.tempfile import TemporaryDirectory
from utilities.types import PathLike


@contextmanager
def yield_zip_file_contents(path: PathLike, /) -> Iterator[list[Path]]:
    """Yield the contents of a zipfile in a temporary directory."""
    with ZipFile(path) as zf, TemporaryDirectory() as temp:
        zf.extractall(path=temp)
        yield list(temp.iterdir())
    _ = zf  # make coverage understand this is returned


__all__ = ["yield_zip_file_contents"]
