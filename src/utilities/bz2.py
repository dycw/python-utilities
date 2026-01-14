from __future__ import annotations

from pathlib import Path
from shutil import copyfileobj
from tarfile import TarFile
from typing import IO, TYPE_CHECKING, BinaryIO, assert_never

from utilities.atomicwrites import writer
from utilities.pathlib import file_or_dir

if TYPE_CHECKING:
    from collections.abc import Callable

    from utilities.types import PathLike


def compress_paths(
    src_or_dest: PathLike,
    func: Callable[[PathLike], BinaryIO],
    /,
    *srcs_or_dest: PathLike,
) -> None:
    """Create a Gzip file."""
    *srcs, dest = list(map(Path, [src_or_dest, *srcs_or_dest]))
    with writer(dest, overwrite=True) as temp, func(temp) as buffer:
        match srcs:
            case [src]:
                match file_or_dir(src):
                    case "file":
                        with src.open(mode="rb") as fh:
                            copyfileobj(fh, buffer)
                    case "dir":
                        with TarFile(mode="w", fileobj=buffer) as tar:
                            _compress_paths_add_dir(src, tar)
                    case None:
                        ...
                    case never:
                        assert_never(never)
            case _:
                with TarFile(mode="w", fileobj=buffer) as tar:
                    for src_i in sorted(srcs):
                        match file_or_dir(src_i):
                            case "file":
                                tar.add(src_i, src_i.name)
                            case "dir":
                                _compress_paths_add_dir(src_i, tar)
                            case None:
                                ...
                            case never:
                                assert_never(never)


def _compress_paths_add_dir(path: PathLike, tar: TarFile, /) -> None:
    path = Path(path)
    for p in sorted(path.rglob("**/*")):
        tar.add(p, p.relative_to(path))


__all__ = ["compress_paths"]
