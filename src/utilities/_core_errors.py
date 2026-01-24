from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, override

from utilities.types import PathLike, SupportsRichComparison

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


@dataclass(kw_only=True, slots=True)
class CompressZipError(Exception):
    srcs: list[Path]
    dest: Path

    @override
    def __str__(self) -> str:
        return _compress_error_msg(self.srcs, self.dest)


def _compress_error_msg(srcs: Iterable[PathLike], dest: PathLike, /) -> str:
    return f"Cannot compress source(s) {[repr(str(s)) for s in srcs]} since destination {str(dest)!r} already exists"


@dataclass(kw_only=True, slots=True)
class CompressFilesError(Exception):
    srcs: list[Path]
    dest: Path


##


@dataclass(kw_only=True, slots=True)
class YieldBZ2Error(Exception):
    path: Path

    @override
    def __str__(self) -> str:
        return _yield_uncompressed_error_msg(self.path)


@dataclass(kw_only=True, slots=True)
class YieldGzipError(Exception):
    path: Path

    @override
    def __str__(self) -> str:
        return _yield_uncompressed_error_msg(self.path)


@dataclass(kw_only=True, slots=True)
class YieldLZMAError(Exception):
    path: Path

    @override
    def __str__(self) -> str:
        return _yield_uncompressed_error_msg(self.path)


@dataclass(kw_only=True, slots=True)
class YieldZipError(Exception):
    path: Path

    @override
    def __str__(self) -> str:
        return _yield_uncompressed_error_msg(self.path)


def _yield_uncompressed_error_msg(path: PathLike, /) -> str:
    return f"Cannot uncompress {str(path)!r} since it does not exist"


@dataclass(kw_only=True, slots=True)
class YieldUncompressedError(Exception):
    path: Path


__all__ = [
    "CompressBZ2Error",
    "CompressFilesError",
    "CompressGzipError",
    "CompressLZMAError",
    "CompressZipError",
    "MaxNullableError",
    "MinNullableError",
    "YieldBZ2Error",
    "YieldGzipError",
    "YieldLZMAError",
    "YieldUncompressedError",
    "YieldZipError",
]
