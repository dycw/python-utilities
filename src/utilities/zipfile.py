from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING
from zipfile import ZipFile

from utilities.pathlib import file_or_dir
from utilities.tempfile import TemporaryDirectory

if TYPE_CHECKING:
    from collections.abc import Iterator

    from utilities.types import PathLike


@enhanced_context_manager
def yield_zip_file_contents(path: PathLike, /) -> Iterator[list[Path]]:
    """Yield the contents of a Zip file."""
    with ZipFile(path) as zf, TemporaryDirectory() as temp:
        zf.extractall(path=temp)
        yield list(temp.iterdir())
    _ = zf  # make coverage understand this is returned


def zip_path(src: PathLike, dest: PathLike, /) -> None:
    """Create a Zip file."""
    src, dest = map(Path, [src, dest])
    with ZipFile(dest, mode="w") as zf:
        match file_or_dir(src):
            case "file":
                z
            case "dir":
                z
            case None:  # pragma: no cover
                a

        if src.is_file():
            a

        zf.extractall(path=temp)
        yield list(temp.iterdir())
    _ = zf  # make coverage understand this is returned


__all__ = ["yield_zip_file_contents"]
