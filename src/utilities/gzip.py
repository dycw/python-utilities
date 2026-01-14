from __future__ import annotations

import gzip
from gzip import GzipFile
from pathlib import Path
from shutil import copyfileobj
from tarfile import TarFile
from typing import TYPE_CHECKING, assert_never

from utilities.atomicwrites import writer
from utilities.pathlib import file_or_dir

if TYPE_CHECKING:
    from utilities.types import PathLike


def gzip_paths(
    src: PathLike, src_or_dest: PathLike, /, *srcs_or_dest: PathLike
) -> None:
    """Create a Gzip file."""
    all_paths = list(map(Path, [src, src_or_dest, *srcs_or_dest]))
    *srcs, dest = all_paths
    with writer(dest, overwrite=True) as temp, GzipFile(temp, mode="wb") as gz:
        match srcs:
            case []:  # pragma: no cover
                raise NotImplementedError
            case [src]:
                match file_or_dir(src):
                    case "file":
                        with src.open(mode="rb") as fh:
                            copyfileobj(fh, gz)
                    case "dir":
                        with TarFile(mode="w", fileobj=gz) as tar:
                            _gzip_paths_add_dir(src, tar)
                    case None:
                        ...
                    case never:
                        assert_never(never)
            case _:
                with TarFile(mode="w", fileobj=gz) as tar:
                    for src_i in sorted(srcs):
                        match file_or_dir(src_i):
                            case "file":
                                tar.add(src_i, src_i.name)
                            case "dir":
                                _gzip_paths_add_dir(src_i, tar)
                            case None:
                                ...
                            case never:
                                assert_never(never)


def _gzip_paths_add_dir(path: PathLike, tar: TarFile, /) -> None:
    path = Path(path)
    for p in sorted(path.rglob("**/*")):
        tar.add(p, p.relative_to(path))


##


def read_binary(path: PathLike, /, *, decompress: bool = False) -> bytes:
    """Read a byte string from disk."""
    path = Path(path)
    if decompress:
        with gzip.open(path) as gz:
            return gz.read()
    else:
        return path.read_bytes()


def write_binary(
    data: bytes, path: PathLike, /, *, compress: bool = False, overwrite: bool = False
) -> None:
    """Write a byte string to disk."""
    with writer(path, compress=compress, overwrite=overwrite) as temp:
        _ = temp.write_bytes(data)


__all__ = ["read_binary", "write_binary"]
