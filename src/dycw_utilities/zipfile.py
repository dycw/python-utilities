from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from zipfile import ZipFile

from dycw_utilities.pathlib import PathLike
from dycw_utilities.tempfile import TemporaryDirectory
from dycw_utilities.typeguard import typeguard_ignore


@typeguard_ignore
@contextmanager
def yield_zip_file_contents(path: PathLike, /) -> Iterator[list[Path]]:
    """Yield the contents of a zipfile in a temporary directory."""

    with ZipFile(path) as zf, TemporaryDirectory() as temp:
        zf.extractall(path=temp)
        yield list(temp.iterdir())
    _ = zf  # make coverage understand this is returned
