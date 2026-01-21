from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, assert_never
from zipfile import ZipFile

from utilities.atomicwrites import writer
from utilities.contextlib import enhanced_context_manager
from utilities.core import (
    OneEmptyError,
    OneNonUniqueError,
    TemporaryDirectory,
    file_or_dir,
    one,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from utilities.types import PathLike


@enhanced_context_manager
def yield_zip_file_contents(path: PathLike, /) -> Iterator[Path]:
    """Yield the contents of a Zip file."""
    with ZipFile(path) as zf, TemporaryDirectory() as temp:
        zf.extractall(path=temp)
        try:
            yield one(temp.iterdir())
        except (OneEmptyError, OneNonUniqueError):
            yield temp


##


def zip_paths(src_or_dest: PathLike, /, *srcs_or_dest: PathLike) -> None:
    """Create a Zip file."""
    *srcs, dest = map(Path, [src_or_dest, *srcs_or_dest])
    with writer(dest, overwrite=True) as temp, ZipFile(temp, mode="w") as zf:
        for src_i in sorted(srcs):
            match file_or_dir(src_i):
                case "file":
                    zf.write(src_i, src_i.name)
                case "dir":
                    for p in sorted(src_i.rglob("**/*")):
                        zf.write(p, p.relative_to(src_i))
                case None:
                    ...
                case never:
                    assert_never(never)


__all__ = ["yield_zip_file_contents"]
