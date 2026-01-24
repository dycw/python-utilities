from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from reprlib import repr as repr_
from typing import TYPE_CHECKING, assert_never, override

from utilities.types import CopyOrMove, PathLike, SupportsRichComparison

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
        return f"Minimum of {repr_(self.iterable)} is undefined"


@dataclass(kw_only=True, slots=True)
class MaxNullableError[T: SupportsRichComparison](Exception):
    iterable: Iterable[T | None]

    @override
    def __str__(self) -> str:
        return f"Maximum of {repr_(self.iterable)} is undefined"


###############################################################################
#### compression ##############################################################
###############################################################################


def _compress_error_msg(srcs: Iterable[PathLike], dest: PathLike, /) -> str:
    return f"Cannot compress source(s) {[repr(str(s)) for s in srcs]} since destination {str(dest)!r} already exists"


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


@dataclass(kw_only=True, slots=True)
class CompressFilesError(Exception):
    srcs: list[Path]
    dest: Path

    @override
    def __str__(self) -> str:
        raise NotImplementedError  # pragma: no cover


##


def _yield_uncompressed_error_msg(path: PathLike, /) -> str:
    return f"Cannot uncompress {str(path)!r} since it does not exist"


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


@dataclass(kw_only=True, slots=True)
class YieldUncompressedError(Exception):
    path: Path

    @override
    def __str__(self) -> str:
        raise NotImplementedError  # pragma: no cover


###############################################################################
#### itertools ################################################################
###############################################################################


@dataclass(kw_only=True, slots=True)
class OneError[T](Exception):
    iterables: tuple[Iterable[T], ...]


@dataclass(kw_only=True, slots=True)
class OneEmptyError[T](OneError[T]):
    @override
    def __str__(self) -> str:
        return f"Iterable(s) {repr_(self.iterables)} must not be empty"


@dataclass(kw_only=True, slots=True)
class OneNonUniqueError[T](OneError):
    first: T
    second: T

    @override
    def __str__(self) -> str:
        return f"Iterable(s) {repr_(self.iterables)} must contain exactly one item; got {repr_(self.first)}, {repr_(self.second)} and perhaps more"


##


@dataclass(kw_only=True, slots=True)
class OneStrError(Exception):
    iterable: Iterable[str]
    text: str
    head: bool = False
    case_sensitive: bool = False


@dataclass(kw_only=True, slots=True)
class OneStrEmptyError(OneStrError):
    @override
    def __str__(self) -> str:
        head = f"Iterable {repr_(self.iterable)} does not contain"
        match self.head, self.case_sensitive:
            case False, True:
                tail = repr(self.text)
            case False, False:
                tail = f"{self.text!r} (modulo case)"
            case True, True:
                tail = f"any string starting with {self.text!r}"
            case True, False:
                tail = f"any string starting with {self.text!r} (modulo case)"
            case never:
                assert_never(never)
        return f"{head} {tail}"


@dataclass(kw_only=True, slots=True)
class OneStrNonUniqueError(OneStrError):
    first: str
    second: str

    @override
    def __str__(self) -> str:
        head = f"Iterable {repr_(self.iterable)} must contain"
        match self.head, self.case_sensitive:
            case False, True:
                mid = f"{self.text!r} exactly once"
            case False, False:
                mid = f"{self.text!r} exactly once (modulo case)"
            case True, True:
                mid = f"exactly one string starting with {self.text!r}"
            case True, False:
                mid = f"exactly one string starting with {self.text!r} (modulo case)"
            case never:
                assert_never(never)
        return f"{head} {mid}; got {self.first!r}, {self.second!r} and perhaps more"


###############################################################################
#### os #######################################################################
###############################################################################


def _copy_or_move_source_not_found_error_msg(src: PathLike, /) -> str:
    return f"Source {str(src)!r} does not exist"


def _copy_or_move_dest_already_exists_error_msg(
    mode: CopyOrMove, src: PathLike, dest: PathLike, /
) -> str:
    return f"Cannot {mode} source {str(src)!r} since destination {str(dest)!r} already exists"


@dataclass(kw_only=True, slots=True)
class CopyError(Exception): ...


@dataclass(kw_only=True, slots=True)
class CopySourceNotFoundError(CopyError):
    src: Path

    @override
    def __str__(self) -> str:
        return _copy_or_move_source_not_found_error_msg(self.src)


@dataclass(kw_only=True, slots=True)
class CopyDestinationExistsError(CopyError):
    src: Path
    dest: Path

    @override
    def __str__(self) -> str:
        return _copy_or_move_dest_already_exists_error_msg("copy", self.src, self.dest)


@dataclass(kw_only=True, slots=True)
class MoveError(Exception): ...


@dataclass(kw_only=True, slots=True)
class MoveSourceNotFoundError(MoveError):
    src: Path

    @override
    def __str__(self) -> str:
        return _copy_or_move_source_not_found_error_msg(self.src)


@dataclass(kw_only=True, slots=True)
class MoveDestinationExistsError(MoveError):
    src: Path
    dest: Path

    @override
    def __str__(self) -> str:
        return _copy_or_move_dest_already_exists_error_msg("move", self.src, self.dest)


@dataclass(kw_only=True, slots=True)
class CopyOrMoveSourceNotFoundError(Exception):
    src: Path

    @override
    def __str__(self) -> str:
        raise NotImplementedError  # pragma: no cover


@dataclass(kw_only=True, slots=True)
class CopyOrMoveDestinationExistsError(Exception):
    src: Path
    dest: Path

    @override
    def __str__(self) -> str:
        raise NotImplementedError  # pragma: no cover


###############################################################################
#### writers ##################################################################
###############################################################################


@dataclass(kw_only=True, slots=True)
class YieldWritePathError(Exception):
    path: Path

    @override
    def __str__(self) -> str:
        return f"Cannot write to {str(self.path)!r} since it already exists"


__all__ = [
    "CompressBZ2Error",
    "CompressFilesError",
    "CompressGzipError",
    "CompressLZMAError",
    "CompressZipError",
    "CopyDestinationExistsError",
    "CopyError",
    "CopyOrMoveDestinationExistsError",
    "CopyOrMoveSourceNotFoundError",
    "CopySourceNotFoundError",
    "MaxNullableError",
    "MinNullableError",
    "MoveDestinationExistsError",
    "MoveError",
    "MoveSourceNotFoundError",
    "OneEmptyError",
    "OneError",
    "OneNonUniqueError",
    "YieldBZ2Error",
    "YieldGzipError",
    "YieldLZMAError",
    "YieldUncompressedError",
    "YieldWritePathError",
    "YieldZipError",
]
