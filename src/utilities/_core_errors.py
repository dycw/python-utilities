from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, override

from utilities.types import SupportsRichComparison

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path


###############################################################################
#### builtins #################################################################
###############################################################################


@dataclass(kw_only=True, slots=True)
class MinNullableError[T: SupportsRichComparison](Exception):
    iterable: Iterable[T | None]

    @override
    def __str__(self) -> str:
        return f"Minimum of {self.iterable} is undefined"


@dataclass(kw_only=True, slots=True)
class MaxNullableError[T: SupportsRichComparison](Exception):
    iterable: Iterable[T | None]

    @override
    def __str__(self) -> str:
        return f"Maximum of {self.iterable} is undefined"


###############################################################################
#### compression ##############################################################
###############################################################################


@dataclass(kw_only=True, slots=True)
class CompressBZ2Error(Exception):
    srcs: list[Path]
    dest: Path

    @override
    def __str__(self) -> str:
        return _compress_error_msg(self.srcs, self.dest)


@dataclass(kw_only=True, slots=True)
class CompressGzipError(Exception):
    srcs: list[Path]
    dest: Path

    @override
    def __str__(self) -> str:
        return _compress_error_msg(self.srcs, self.dest)


@dataclass(kw_only=True, slots=True)
class CompressLZMAError(Exception):
    srcs: list[Path]
    dest: Path

    @override
    def __str__(self) -> str:
        return _compress_error_msg(self.srcs, self.dest)


def _compress_error_msg(srcs: Iterable[Path], dest: Path, /) -> str:
    return f"Cannot compress source(s) {[repr(str(s)) for s in srcs]} since destination {str(dest)!r} already exists"


@dataclass(kw_only=True, slots=True)
class _CompressFilesError(Exception):
    srcs: list[Path]
    dest: Path


__all__ = [
    "CompressBZ2Error",
    "CompressGzipError",
    "CompressLZMAError",
    "MaxNullableError",
    "MinNullableError",
]
