from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from zipfile import ZipFile

from beartype import beartype

from utilities.pathlib import PathLike
from utilities.tempfile import TemporaryDirectory


@contextmanager
@beartype
def yield_zip_file_contents(path: PathLike, /) -> Iterator[list[Path]]:
    """Yield the contents of a zipfile in a temporary directory."""

    with ZipFile(path) as zf, TemporaryDirectory() as temp:
        zf.extractall(path=temp)
        yield list(temp.iterdir())
    _ = zf  # make coverage understand this is returned
