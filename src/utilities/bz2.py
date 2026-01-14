from __future__ import annotations

from bz2 import BZ2File
from typing import TYPE_CHECKING, cast

from utilities.compression import compress_paths, yield_compressed_contents
from utilities.contextlib import enhanced_context_manager
from utilities.types import PathLike

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from utilities.types import PathLike, PathToBinaryIO


def bz2_paths(src_or_dest: PathLike, /, *srcs_or_dest: PathLike) -> None:
    """Create a BZ2 file."""

    def func(path: PathLike, /) -> BZ2File:
        return BZ2File(path, mode="wb")

    compress_paths(cast("PathToBinaryIO", func), src_or_dest, *srcs_or_dest)


##


@enhanced_context_manager
def yield_bz2_contents(path: PathLike, /) -> Iterator[Path]:
    """Yield the contents of a BZ2 file."""

    def func(path: PathLike, /) -> BZ2File:
        return BZ2File(path, mode="rb")

    with yield_compressed_contents(path, cast("PathToBinaryIO", func)) as temp:
        yield temp


__all__ = ["bz2_paths", "yield_bz2_contents"]
