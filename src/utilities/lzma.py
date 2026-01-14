from __future__ import annotations

from lzma import LZMAFile
from typing import TYPE_CHECKING, cast

from utilities.compression import compress_paths, yield_compressed_contents
from utilities.contextlib import enhanced_context_manager
from utilities.types import PathLike

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from utilities.types import PathLike, PathToBinaryIO


def lzma_paths(src_or_dest: PathLike, /, *srcs_or_dest: PathLike) -> None:
    """Create an LZMA file."""

    def func(path: PathLike, /) -> LZMAFile:
        return LZMAFile(path, mode="wb")

    compress_paths(cast("PathToBinaryIO", func), src_or_dest, *srcs_or_dest)


##


@enhanced_context_manager
def yield_lzma_contents(path: PathLike, /) -> Iterator[Path]:
    """Yield the contents of an LZMA file."""

    def func(path: PathLike, /) -> LZMAFile:
        return LZMAFile(path, mode="rb")

    with yield_compressed_contents(path, cast("PathToBinaryIO", func)) as temp:
        yield temp


__all__ = ["lzma_paths", "yield_lzma_contents"]
