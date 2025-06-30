from __future__ import annotations

import gzip
from pathlib import Path
from typing import TYPE_CHECKING

from utilities.atomicwrites import writer

if TYPE_CHECKING:
    from utilities.types import PathLike


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
