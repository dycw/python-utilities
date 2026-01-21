from __future__ import annotations

import gzip
from contextlib import contextmanager
from gzip import GzipFile
from pathlib import Path
from typing import TYPE_CHECKING, cast

from utilities.atomicwrites import writer
from utilities.compression import compress_paths, yield_compressed_contents

if TYPE_CHECKING:
    from collections.abc import Iterator

    from utilities.types import PathLike, PathToBinaryIO


def gzip_paths(src_or_dest: PathLike, /, *srcs_or_dest: PathLike) -> None:
    """Create a Gzip file."""

    def func(path: PathLike, /) -> GzipFile:
        return GzipFile(path, mode="wb")

    compress_paths(cast("PathToBinaryIO", func), src_or_dest, *srcs_or_dest)


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


@contextmanager
def yield_gzip_contents(path: PathLike, /) -> Iterator[Path]:
    """Yield the contents of a Gzip file."""

    def func(path: PathLike, /) -> GzipFile:
        return GzipFile(path, mode="rb")

    with yield_compressed_contents(path, cast("PathToBinaryIO", func)) as temp:
        yield temp


__all__ = ["gzip_paths", "read_binary", "write_binary", "yield_gzip_contents"]
