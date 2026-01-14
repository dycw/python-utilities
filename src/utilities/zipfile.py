from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, assert_never
from zipfile import ZipFile

from utilities.atomicwrites import writer
from utilities.contextlib import enhanced_context_manager
from utilities.iterables import OneEmptyError, OneNonUniqueError, one
from utilities.pathlib import file_or_dir
from utilities.tempfile import TemporaryDirectory

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


def zip_paths(src: PathLike, /, *srcs_or_dest: PathLike) -> None:
    """Create a Zip file."""
    all_paths = list(map(Path, [src, *srcs_or_dest]))
    *srcs, dest = all_paths
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
