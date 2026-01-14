from __future__ import annotations

import gzip
from gzip import GzipFile
from pathlib import Path
from shutil import copyfileobj
from tarfile import ReadError, TarFile
from typing import TYPE_CHECKING, assert_never

from utilities.atomicwrites import writer
from utilities.contextlib import enhanced_context_manager
from utilities.errors import ImpossibleCaseError
from utilities.iterables import OneEmptyError, OneNonUniqueError, one
from utilities.pathlib import file_or_dir
from utilities.tempfile import TemporaryDirectory, TemporaryFile

if TYPE_CHECKING:
    from collections.abc import Iterator

    from utilities.types import PathLike


def gzip_paths(
    src: PathLike, src_or_dest: PathLike, /, *srcs_or_dest: PathLike
) -> None:
    """Create a Gzip file."""
    all_paths = list(map(Path, [src, src_or_dest, *srcs_or_dest]))
    *srcs, dest = all_paths
    with writer(dest, overwrite=True) as temp, GzipFile(temp, mode="wb") as gz:
        match srcs:
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


##


@enhanced_context_manager
def yield_gzip_file_contents(path: PathLike, /) -> Iterator[Path]:
    """Yield the contents of a Gzip file."""
    with GzipFile(path, mode="rb") as gz:
        try:
            with TarFile(fileobj=gz) as tf, TemporaryDirectory() as temp:
                tf.extractall(path=temp, filter="data")
                try:
                    yield one(temp.iterdir())
                except (OneEmptyError, OneNonUniqueError):
                    yield temp
        except ReadError as error:
            (arg,) = error.args
            if arg == "empty file":
                with TemporaryDirectory() as temp:
                    yield temp
            elif arg == "truncated header":
                _ = gz.seek(0)
                with TemporaryFile() as temp, temp.open(mode="wb") as fh:
                    copyfileobj(gz, fh)
                    _ = fh.seek(0)
                    yield temp
            else:  # pragma: no cover
                raise ImpossibleCaseError(case=[f"{arg=}"]) from None


__all__ = ["read_binary", "write_binary"]
