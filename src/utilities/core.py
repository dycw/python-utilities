from __future__ import annotations

import datetime as dt
import os
import re
import reprlib
import shutil
import tempfile
from bz2 import BZ2File
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from functools import _lru_cache_wrapper, partial, wraps
from gzip import GzipFile
from itertools import chain, islice
from lzma import LZMAFile
from os import chdir, environ, getenv, getpid
from pathlib import Path
from re import VERBOSE, Pattern, findall
from shutil import copyfileobj, copytree
from string import Template
from tarfile import ReadError, TarFile
from tempfile import NamedTemporaryFile as _NamedTemporaryFile
from textwrap import dedent
from threading import get_ident
from time import time_ns
from types import (
    BuiltinFunctionType,
    FunctionType,
    MethodDescriptorType,
    MethodType,
    MethodWrapperType,
    WrapperDescriptorType,
)
from typing import TYPE_CHECKING, Any, Literal, assert_never, cast, overload, override
from uuid import uuid4
from warnings import catch_warnings, filterwarnings
from zoneinfo import ZoneInfo

from typing_extensions import TypeIs
from whenever import Date, PlainDateTime, Time, ZonedDateTime

from utilities.constants import (
    LOCAL_TIME_ZONE,
    LOCAL_TIME_ZONE_NAME,
    RICH_EXPAND_ALL,
    RICH_INDENT_SIZE,
    RICH_MAX_DEPTH,
    RICH_MAX_LENGTH,
    RICH_MAX_STRING,
    RICH_MAX_WIDTH,
    UTC,
    Sentinel,
    _get_now,
    sentinel,
)
from utilities.constants import _get_now_local as get_now_local
from utilities.types import (
    TIME_ZONES,
    CopyOrMove,
    FilterWarningsAction,
    PathToBinaryIO,
    PatternLike,
    StrDict,
    StrMapping,
    SupportsRichComparison,
    TimeZone,
    TimeZoneLike,
    TypeLike,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
    from types import TracebackType

    from whenever import Date, PlainDateTime, Time

    from utilities.types import FileOrDir, MaybeIterable, PathLike


###############################################################################
#### builtins #################################################################
###############################################################################


@overload
def get_class[T](obj: type[T], /) -> type[T]: ...
@overload
def get_class[T](obj: T, /) -> type[T]: ...
def get_class[T](obj: T | type[T], /) -> type[T]:
    """Get the class of an object, unless it is already a class."""
    return obj if isinstance(obj, type) else type(obj)


def get_class_name(obj: Any, /, *, qual: bool = False) -> str:
    """Get the name of the class of an object, unless it is already a class."""
    cls = get_class(obj)
    return f"{cls.__module__}.{cls.__qualname__}" if qual else cls.__name__


##


def get_func_name(obj: Callable[..., Any], /) -> str:
    """Get the name of a callable."""
    if isinstance(obj, BuiltinFunctionType):
        return obj.__name__
    if isinstance(obj, FunctionType):
        name = obj.__name__
        pattern = r"^.+\.([A-Z]\w+\." + name + ")$"
        try:
            (full_name,) = findall(pattern, obj.__qualname__)
        except ValueError:
            return name
        return full_name
    if isinstance(obj, MethodType):
        return f"{get_class_name(obj.__self__)}.{obj.__name__}"
    if isinstance(
        obj,
        MethodType | MethodDescriptorType | MethodWrapperType | WrapperDescriptorType,
    ):
        return obj.__qualname__
    if isinstance(obj, _lru_cache_wrapper):
        return cast("Any", obj).__name__
    if isinstance(obj, partial):
        return get_func_name(obj.func)
    return get_class_name(obj)


##


@overload
def min_nullable[T: SupportsRichComparison](
    iterable: Iterable[T | None], /, *, default: Sentinel = ...
) -> T: ...
@overload
def min_nullable[T: SupportsRichComparison, U](
    iterable: Iterable[T | None], /, *, default: U = ...
) -> T | U: ...
def min_nullable[T: SupportsRichComparison, U](
    iterable: Iterable[T | None], /, *, default: U | Sentinel = sentinel
) -> T | U:
    """Compute the minimum of a set of values; ignoring nulls."""
    values = (i for i in iterable if i is not None)
    if is_sentinel(default):
        try:
            return min(values)
        except ValueError:
            raise MinNullableError(iterable=iterable) from None
    return min(values, default=default)


@dataclass(kw_only=True, slots=True)
class MinNullableError[T: SupportsRichComparison](Exception):
    iterable: Iterable[T | None]

    @override
    def __str__(self) -> str:
        return f"Minimum of {repr_(self.iterable)} is undefined"


@overload
def max_nullable[T: SupportsRichComparison](
    iterable: Iterable[T | None], /, *, default: Sentinel = ...
) -> T: ...
@overload
def max_nullable[T: SupportsRichComparison, U](
    iterable: Iterable[T | None], /, *, default: U = ...
) -> T | U: ...
def max_nullable[T: SupportsRichComparison, U](
    iterable: Iterable[T | None], /, *, default: U | Sentinel = sentinel
) -> T | U:
    """Compute the maximum of a set of values; ignoring nulls."""
    values = (i for i in iterable if i is not None)
    if is_sentinel(default):
        try:
            return max(values)
        except ValueError:
            raise MaxNullableError(iterable=iterable) from None
    return max(values, default=default)


@dataclass(kw_only=True, slots=True)
class MaxNullableError[T: SupportsRichComparison](Exception):
    iterable: Iterable[T | None]

    @override
    def __str__(self) -> str:
        return f"Maximum of {repr_(self.iterable)} is undefined"


###############################################################################
#### compression ##############################################################
###############################################################################


def compress_bz2(
    src_or_dest: PathLike, /, *srcs_or_dest: PathLike, overwrite: bool = False
) -> None:
    """Create a BZ2 file."""

    def func(path: PathLike, /) -> BZ2File:
        return BZ2File(path, mode="wb")

    func2 = cast("PathToBinaryIO", func)
    try:
        _compress_files(func2, src_or_dest, *srcs_or_dest, overwrite=overwrite)
    except _CompressFilesError as error:
        raise CompressBZ2Error(srcs=error.srcs, dest=error.dest) from None


@dataclass(kw_only=True, slots=True)
class CompressBZ2Error(Exception):
    srcs: list[Path]
    dest: Path

    @override
    def __str__(self) -> str:
        return f"Cannot compress source(s) {repr_(list(map(str, self.srcs)))} since destination {repr_str(self.dest)} already exists"


def compress_gzip(
    src_or_dest: PathLike, /, *srcs_or_dest: PathLike, overwrite: bool = False
) -> None:
    """Create a Gzip file."""

    def func(path: PathLike, /) -> GzipFile:
        return GzipFile(path, mode="wb")

    func2 = cast("PathToBinaryIO", func)
    try:
        _compress_files(func2, src_or_dest, *srcs_or_dest, overwrite=overwrite)
    except _CompressFilesError as error:
        raise CompressGzipError(srcs=error.srcs, dest=error.dest) from None


@dataclass(kw_only=True, slots=True)
class CompressGzipError(Exception):
    srcs: list[Path]
    dest: Path

    @override
    def __str__(self) -> str:
        return f"Cannot compress source(s) {repr_(list(map(str, self.srcs)))} since destination {repr_str(self.dest)} already exists"


def compress_lzma(
    src_or_dest: PathLike, /, *srcs_or_dest: PathLike, overwrite: bool = False
) -> None:
    """Create an LZMA file."""

    def func(path: PathLike, /) -> LZMAFile:
        return LZMAFile(path, mode="wb")

    func2 = cast("PathToBinaryIO", func)
    try:
        _compress_files(func2, src_or_dest, *srcs_or_dest, overwrite=overwrite)
    except _CompressFilesError as error:
        raise CompressLZMAError(srcs=error.srcs, dest=error.dest) from None


@dataclass(kw_only=True, slots=True)
class CompressLZMAError(Exception):
    srcs: list[Path]
    dest: Path

    @override
    def __str__(self) -> str:
        return f"Cannot compress source(s) {repr_(list(map(str, self.srcs)))} since destination {repr_str(self.dest)} already exists"


def _compress_files(
    func: PathToBinaryIO,
    src_or_dest: PathLike,
    /,
    *srcs_or_dest: PathLike,
    overwrite: bool = False,
) -> None:
    *srcs, dest = map(Path, [src_or_dest, *srcs_or_dest])
    try:
        with yield_write_path(dest, overwrite=overwrite) as temp, func(temp) as buffer:
            match srcs:
                case [src]:
                    match file_or_dir(src):
                        case "file":
                            with src.open(mode="rb") as fh:
                                copyfileobj(fh, buffer)
                        case "dir":
                            with TarFile(mode="w", fileobj=buffer) as tar:
                                _compress_files_add_dir(src, tar)
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
                                    _compress_files_add_dir(src_i, tar)
                                case None:
                                    ...
                                case never:
                                    assert_never(never)
    except YieldWritePathError as error:
        raise _CompressFilesError(srcs=srcs, dest=error.path) from None


@dataclass(kw_only=True, slots=True)
class _CompressFilesError(Exception):
    srcs: list[Path]
    dest: Path

    @override
    def __str__(self) -> str:
        return f"Cannot compress source(s) {repr_str(self.srcs)} since destination {repr_str(self.dest)} already exists"


def _compress_files_add_dir(path: PathLike, tar: TarFile, /) -> None:
    path = Path(path)
    for p in sorted(path.rglob("**/*")):
        tar.add(p, p.relative_to(path))


##


@contextmanager
def yield_bz2(path: PathLike, /) -> Iterator[Path]:
    """Yield the contents of a BZ2 file."""

    def func(path: PathLike, /) -> BZ2File:
        return BZ2File(path, mode="rb")

    with _yield_uncompressed(path, cast("PathToBinaryIO", func)) as temp:
        yield temp


@contextmanager
def yield_gzip(path: PathLike, /) -> Iterator[Path]:
    """Yield the contents of a Gzip file."""

    def func(path: PathLike, /) -> GzipFile:
        return GzipFile(path, mode="rb")

    with _yield_uncompressed(path, cast("PathToBinaryIO", func)) as temp:
        yield temp


@contextmanager
def yield_lzma(path: PathLike, /) -> Iterator[Path]:
    """Yield the contents of an LZMA file."""

    def func(path: PathLike, /) -> LZMAFile:
        return LZMAFile(path, mode="rb")

    with _yield_uncompressed(path, cast("PathToBinaryIO", func)) as temp:
        yield temp


@contextmanager
def _yield_uncompressed(path: PathLike, func: PathToBinaryIO, /) -> Iterator[Path]:
    with func(path) as buffer:
        try:
            with TarFile(fileobj=buffer) as tf, TemporaryDirectory() as temp:
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
            elif arg in {"bad checksum", "invalid header", "truncated header"}:
                _ = buffer.seek(0)
                with TemporaryFile() as temp, temp.open(mode="wb") as fh:
                    copyfileobj(buffer, fh)
                    _ = fh.seek(0)
                    yield temp
            else:  # pragma: no cover
                raise NotImplementedError(arg) from None


###############################################################################
#### constants ################################################################
###############################################################################


def is_none(obj: Any, /) -> TypeIs[None]:
    """Check if an object is `None`."""
    return obj is None


def is_not_none(obj: Any, /) -> bool:
    """Check if an object is not `None`."""
    return obj is not None


##


def is_sentinel(obj: Any, /) -> TypeIs[Sentinel]:
    """Check if an object is the sentinel."""
    return obj is sentinel


###############################################################################
#### contextlib ###############################################################
###############################################################################


@contextmanager
def suppress_super_attribute_error() -> Iterator[None]:
    """Suppress the super() attribute error, for mix-ins."""
    try:
        yield
    except AttributeError as error:
        if not _suppress_super_attribute_error_pattern.search(error.args[0]):
            raise


_suppress_super_attribute_error_pattern = re.compile(
    r"'super' object has no attribute '\w+'"
)


###############################################################################
#### functools ################################################################
###############################################################################


def not_func[**P](func: Callable[P, bool], /) -> Callable[P, bool]:
    """Lift a boolean-valued function to return its conjugation."""

    @wraps(func)
    def wrapped(*args: P.args, **kwargs: P.kwargs) -> bool:
        return not func(*args, **kwargs)

    return wrapped


###############################################################################
#### functions ################################################################
###############################################################################


@overload
def first[T](tup: tuple[T], /) -> T: ...
@overload
def first[T](tup: tuple[T, Any], /) -> T: ...
@overload
def first[T](tup: tuple[T, Any, Any], /) -> T: ...
@overload
def first[T](tup: tuple[T, Any, Any, Any], /) -> T: ...
def first(tup: tuple[Any, ...], /) -> Any:
    """Get the first element in a tuple."""
    return tup[0]


@overload
def second[T](tup: tuple[Any, T], /) -> T: ...
@overload
def second[T](tup: tuple[Any, T, Any], /) -> T: ...
@overload
def second[T](tup: tuple[Any, T, Any, Any], /) -> T: ...
def second(tup: tuple[Any, ...], /) -> Any:
    """Get the second element in a tuple."""
    return tup[1]


@overload
def last[T](tup: tuple[T], /) -> T: ...
@overload
def last[T](tup: tuple[Any, T], /) -> T: ...
@overload
def last[T](tup: tuple[Any, Any, T], /) -> T: ...
@overload
def last[T](tup: tuple[Any, Any, Any, T], /) -> T: ...
def last(tup: tuple[Any, ...], /) -> Any:
    """Get the last element in a tuple."""
    return tup[-1]


##


def identity[T](obj: T, /) -> T:
    """Return the object itself."""
    return obj


###############################################################################
#### itertools ################################################################
###############################################################################


def always_iterable[T](obj: MaybeIterable[T], /) -> Iterable[T]:
    """Typed version of `always_iterable`."""
    obj = cast("Any", obj)
    if isinstance(obj, str | bytes):
        return cast("list[T]", [obj])
    try:
        return iter(cast("Iterable[T]", obj))
    except TypeError:
        return cast("list[T]", [obj])


##


def chunked[T](iterable: Iterable[T], n: int, /) -> Iterator[Sequence[T]]:
    """Break an iterable into lists of length n."""
    return iter(partial(take, n, iter(iterable)), [])


##


def one[T](*iterables: Iterable[T]) -> T:
    """Return the unique value in a set of iterables."""
    it = chain(*iterables)
    try:
        first = next(it)
    except StopIteration:
        raise OneEmptyError(iterables=iterables) from None
    try:
        second = next(it)
    except StopIteration:
        return first
    raise OneNonUniqueError(iterables=iterables, first=first, second=second)


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
        return f"Iterable(s) {repr_(self.iterables)} must contain exactly one item; got {self.first}, {self.second} and perhaps more"


##


def one_str(
    iterable: Iterable[str],
    text: str,
    /,
    *,
    head: bool = False,
    case_sensitive: bool = False,
) -> str:
    """Find the unique string in an iterable."""
    as_list = list(iterable)
    match head, case_sensitive:
        case False, True:
            it = (t for t in as_list if t == text)
        case False, False:
            it = (t for t in as_list if t.lower() == text.lower())
        case True, True:
            it = (t for t in as_list if t.startswith(text))
        case True, False:
            it = (t for t in as_list if t.lower().startswith(text.lower()))
        case never:
            assert_never(never)
    try:
        return one(it)
    except OneEmptyError:
        raise OneStrEmptyError(
            iterable=as_list, text=text, head=head, case_sensitive=case_sensitive
        ) from None
    except OneNonUniqueError as error:
        raise OneStrNonUniqueError(
            iterable=as_list,
            text=text,
            head=head,
            case_sensitive=case_sensitive,
            first=error.first,
            second=error.second,
        ) from None


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
                tail = f"{repr_(self.text)} (modulo case)"
            case True, True:
                tail = f"any string starting with {repr_(self.text)}"
            case True, False:
                tail = f"any string starting with {repr_(self.text)} (modulo case)"
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
                mid = f"{repr_(self.text)} exactly once"
            case False, False:
                mid = f"{repr_(self.text)} exactly once (modulo case)"
            case True, True:
                mid = f"exactly one string starting with {repr_(self.text)}"
            case True, False:
                mid = (
                    f"exactly one string starting with {repr_(self.text)} (modulo case)"
                )
            case never:
                assert_never(never)
        return f"{head} {mid}; got {repr_(self.first)}, {repr_(self.second)} and perhaps more"


##


def take[T](n: int, iterable: Iterable[T], /) -> Sequence[T]:
    """Return first n items of the iterable as a list."""
    return list(islice(iterable, n))


##


@overload
def transpose[T1](iterable: Iterable[tuple[T1]], /) -> tuple[list[T1]]: ...
@overload
def transpose[T1, T2](
    iterable: Iterable[tuple[T1, T2]], /
) -> tuple[list[T1], list[T2]]: ...
@overload
def transpose[T1, T2, T3](
    iterable: Iterable[tuple[T1, T2, T3]], /
) -> tuple[list[T1], list[T2], list[T3]]: ...
@overload
def transpose[T1, T2, T3, T4](
    iterable: Iterable[tuple[T1, T2, T3, T4]], /
) -> tuple[list[T1], list[T2], list[T3], list[T4]]: ...
@overload
def transpose[T1, T2, T3, T4, T5](
    iterable: Iterable[tuple[T1, T2, T3, T4, T5]], /
) -> tuple[list[T1], list[T2], list[T3], list[T4], list[T5]]: ...
def transpose(iterable: Iterable[tuple[Any]]) -> tuple[list[Any], ...]:  # pyright: ignore[reportInconsistentOverload]
    """Typed verison of `transpose`."""
    return tuple(map(list, zip(*iterable, strict=True)))


##


def unique_everseen[T](
    iterable: Iterable[T], /, *, key: Callable[[T], Any] | None = None
) -> Iterator[T]:
    """Yield unique elements, preserving order."""
    seenset = set()
    seenset_add = seenset.add
    seenlist = []
    seenlist_add = seenlist.append
    use_key = key is not None
    for element in iterable:
        k = key(element) if use_key else element
        try:
            if k not in seenset:
                seenset_add(k)
                yield element
        except TypeError:
            if k not in seenlist:
                seenlist_add(k)
                yield element


###############################################################################
#### os #######################################################################
###############################################################################


def copy(src: PathLike, dest: PathLike, /, *, overwrite: bool = False) -> None:
    """Copy a file atomically."""
    src, dest = map(Path, [src, dest])
    _copy_or_move(src, dest, "copy", overwrite=overwrite)


def move(src: PathLike, dest: PathLike, /, *, overwrite: bool = False) -> None:
    """Move a file atomically."""
    src, dest = map(Path, [src, dest])
    _copy_or_move(src, dest, "move", overwrite=overwrite)


@dataclass(kw_only=True, slots=True)
class CopyOrMoveError(Exception): ...


def _copy_or_move(
    src: Path, dest: Path, mode: CopyOrMove, /, *, overwrite: bool = False
) -> None:
    match file_or_dir(src), file_or_dir(dest), overwrite:
        case None, _, _:
            raise _CopyOrMoveSourceNotFoundError(src=src)
        case "file" | "dir", "file" | "dir", False:
            raise _CopyOrMoveDestinationExistsError(mode=mode, src=src, dest=dest)
        case ("file", None, _) | ("file", "file", True):
            _copy_or_move__file_to_file(src, dest, mode)
        case "file", "dir", True:
            _copy_or_move__file_to_dir(src, dest, mode)
        case ("dir", None, _) | ("dir", "dir", True):
            _copy_or_move__dir_to_dir(src, dest, mode)
        case "dir", "file", True:
            _copy_or_move__dir_to_file(src, dest, mode)
        case never:
            assert_never(never)


@dataclass(kw_only=True, slots=True)
class _CopyOrMoveSourceNotFoundError(CopyOrMoveError):
    src: Path

    @override
    def __str__(self) -> str:
        return f"Source {repr_str(self.src)} does not exist"


@dataclass(kw_only=True, slots=True)
class _CopyOrMoveDestinationExistsError(CopyOrMoveError):
    mode: CopyOrMove
    src: Path
    dest: Path

    @override
    def __str__(self) -> str:
        return f"Cannot {self.mode} source {repr_str(self.src)} since destination {repr_str(self.dest)} already exists"


def _copy_or_move__file_to_file(src: Path, dest: Path, mode: CopyOrMove, /) -> None:
    with yield_adjacent_temp_file(dest) as temp:
        _copy_or_move__shutil_file(src, temp, mode, dest)


def _copy_or_move__file_to_dir(src: Path, dest: Path, mode: CopyOrMove, /) -> None:
    with (
        yield_adjacent_temp_dir(dest) as temp_dir,
        yield_adjacent_temp_file(dest) as temp_file,
    ):
        _ = dest.replace(temp_dir)
        _copy_or_move__shutil_file(src, temp_file, mode, dest)


def _copy_or_move__dir_to_dir(src: Path, dest: Path, mode: CopyOrMove, /) -> None:
    with yield_adjacent_temp_dir(dest) as temp1, yield_adjacent_temp_dir(dest) as temp2:
        with suppress(FileNotFoundError):
            _ = dest.replace(temp1)
        _copy_or_move__shutil_dir(src, temp2, mode, dest)


def _copy_or_move__dir_to_file(src: Path, dest: Path, mode: CopyOrMove, /) -> None:
    with (
        yield_adjacent_temp_file(dest) as temp_file,
        yield_adjacent_temp_dir(dest) as temp_dir,
    ):
        _ = dest.replace(temp_file)
        _copy_or_move__shutil_dir(src, temp_dir, mode, dest)


def _copy_or_move__shutil_file(
    src: Path, temp: Path, mode: CopyOrMove, dest: Path, /
) -> None:
    match mode:
        case "copy":
            _ = shutil.copy(src, temp)
        case "move":
            _ = shutil.move(src, temp)
        case never:
            assert_never(never)
    _ = temp.replace(dest)


def _copy_or_move__shutil_dir(
    src: Path, temp: Path, mode: CopyOrMove, dest: Path, /
) -> None:
    match mode:
        case "copy":
            _ = copytree(src, temp, dirs_exist_ok=True)
            _ = temp.replace(dest)
        case "move":
            _ = shutil.move(src, temp)
            _ = (temp / src.name).replace(dest)
        case never:
            assert_never(never)


##


@overload
def get_env(
    key: str, /, *, case_sensitive: bool = False, default: str, nullable: bool = False
) -> str: ...
@overload
def get_env(
    key: str,
    /,
    *,
    case_sensitive: bool = False,
    default: None = None,
    nullable: Literal[False] = False,
) -> str: ...
@overload
def get_env(
    key: str,
    /,
    *,
    case_sensitive: bool = False,
    default: str | None = None,
    nullable: bool = False,
) -> str | None: ...
def get_env(
    key: str,
    /,
    *,
    case_sensitive: bool = False,
    default: str | None = None,
    nullable: bool = False,
) -> str | None:
    """Get an environment variable."""
    try:
        key_use = one_str(environ, key, case_sensitive=case_sensitive)
    except OneStrEmptyError:
        match default, nullable:
            case None, False:
                raise GetEnvError(key=key, case_sensitive=case_sensitive) from None
            case None, True:
                return None
            case str(), _:
                return default
            case never:
                assert_never(never)
    return environ[key_use]


@dataclass(kw_only=True, slots=True)
class GetEnvError(Exception):
    key: str
    case_sensitive: bool = False

    @override
    def __str__(self) -> str:
        desc = f"No environment variable {repr_(self.key)}"
        return desc if self.case_sensitive else f"{desc} (modulo case)"


##


@contextmanager
def yield_temp_environ(
    env: Mapping[str, str | None] | None = None, **env_kwargs: str | None
) -> Iterator[None]:
    """Yield a temporary environment."""
    mapping: dict[str, str | None] = ({} if env is None else dict(env)) | env_kwargs
    prev = {key: getenv(key) for key in mapping}
    _yield_temp_environ_apply(mapping)
    try:
        yield
    finally:
        _yield_temp_environ_apply(prev)


def _yield_temp_environ_apply(mapping: Mapping[str, str | None], /) -> None:
    for key, value in mapping.items():
        if value is None:
            with suppress(KeyError):
                del environ[key]
        else:
            environ[key] = value


###############################################################################
#### pathlib ##################################################################
###############################################################################


@overload
def file_or_dir(path: PathLike, /, *, exists: Literal[True]) -> FileOrDir: ...
@overload
def file_or_dir(path: PathLike, /, *, exists: bool = False) -> FileOrDir | None: ...
def file_or_dir(path: PathLike, /, *, exists: bool = False) -> FileOrDir | None:
    """Classify a path as a file, directory or non-existent."""
    path = Path(path)
    match path.exists(), path.is_file(), path.is_dir(), exists:
        case True, True, False, _:
            return "file"
        case True, False, True, _:
            return "dir"
        case False, False, False, True:
            raise _FileOrDirMissingError(path=path)
        case False, False, False, False:
            return None
        case _:
            raise _FileOrDirTypeError(path=path)


@dataclass(kw_only=True, slots=True)
class FileOrDirError(Exception):
    path: Path


@dataclass(kw_only=True, slots=True)
class _FileOrDirMissingError(FileOrDirError):
    @override
    def __str__(self) -> str:
        return f"Path does not exist: {repr_str(self.path)}"


@dataclass(kw_only=True, slots=True)
class _FileOrDirTypeError(FileOrDirError):
    @override
    def __str__(self) -> str:
        return f"Path is neither a file nor a directory: {repr_str(self.path)}"


##


@contextmanager
def yield_temp_cwd(path: PathLike, /) -> Iterator[None]:
    """Yield a temporary working directory."""
    prev = Path.cwd()
    chdir(path)
    try:
        yield
    finally:
        chdir(prev)


###############################################################################
#### re #######################################################################
###############################################################################


def extract_group(pattern: PatternLike, text: str, /, *, flags: int = 0) -> str:
    """Extract a group.

    The regex must have 1 capture group, and this must match exactly once.
    """
    pattern_use = _to_pattern(pattern, flags=flags)
    match pattern_use.groups:
        case 0:
            raise _ExtractGroupNoCaptureGroupsError(pattern=pattern_use, text=text)
        case 1:
            matches: list[str] = pattern_use.findall(text)
            match len(matches):
                case 0:
                    raise _ExtractGroupNoMatchesError(
                        pattern=pattern_use, text=text
                    ) from None
                case 1:
                    return matches[0]
                case _:
                    raise _ExtractGroupMultipleMatchesError(
                        pattern=pattern_use, text=text, matches=matches
                    ) from None
        case _:
            raise _ExtractGroupMultipleCaptureGroupsError(
                pattern=pattern_use, text=text
            )


@dataclass(kw_only=True, slots=True)
class ExtractGroupError(Exception):
    pattern: Pattern[str]
    text: str


@dataclass(kw_only=True, slots=True)
class _ExtractGroupMultipleCaptureGroupsError(ExtractGroupError):
    @override
    def __str__(self) -> str:
        return f"Pattern {self.pattern} must contain exactly one capture group; it had multiple"


@dataclass(kw_only=True, slots=True)
class _ExtractGroupMultipleMatchesError(ExtractGroupError):
    matches: list[str]

    @override
    def __str__(self) -> str:
        return f"Pattern {self.pattern} must match against {self.text} exactly once; matches were {self.matches}"


@dataclass(kw_only=True, slots=True)
class _ExtractGroupNoCaptureGroupsError(ExtractGroupError):
    @override
    def __str__(self) -> str:
        return f"Pattern {self.pattern} must contain exactly one capture group; it had none".format(
            self.pattern
        )


@dataclass(kw_only=True, slots=True)
class _ExtractGroupNoMatchesError(ExtractGroupError):
    @override
    def __str__(self) -> str:
        return f"Pattern {self.pattern} must match against {self.text}"


##


def extract_groups(pattern: PatternLike, text: str, /, *, flags: int = 0) -> list[str]:
    """Extract multiple groups.

    The regex may have any number of capture groups, and they must collectively
    match exactly once.
    """
    pattern_use = _to_pattern(pattern, flags=flags)
    if (n_groups := pattern_use.groups) == 0:
        raise _ExtractGroupsNoCaptureGroupsError(pattern=pattern_use, text=text)
    matches: list[str] = pattern_use.findall(text)
    match len(matches), n_groups:
        case 0, _:
            raise _ExtractGroupsNoMatchesError(pattern=pattern_use, text=text)
        case 1, 1:
            return matches
        case 1, _:
            return list(matches[0])
        case _:
            raise _ExtractGroupsMultipleMatchesError(
                pattern=pattern_use, text=text, matches=matches
            )


@dataclass(kw_only=True, slots=True)
class ExtractGroupsError(Exception):
    pattern: Pattern[str]
    text: str


@dataclass(kw_only=True, slots=True)
class _ExtractGroupsMultipleMatchesError(ExtractGroupsError):
    matches: list[str]

    @override
    def __str__(self) -> str:
        return f"Pattern {self.pattern} must match against {self.text} exactly once; matches were {self.matches}"


@dataclass(kw_only=True, slots=True)
class _ExtractGroupsNoCaptureGroupsError(ExtractGroupsError):
    pattern: Pattern[str]
    text: str

    @override
    def __str__(self) -> str:
        return f"Pattern {self.pattern} must contain at least one capture group"


@dataclass(kw_only=True, slots=True)
class _ExtractGroupsNoMatchesError(ExtractGroupsError):
    @override
    def __str__(self) -> str:
        return f"Pattern {self.pattern} must match against {self.text}"


##


def _to_pattern(pattern: PatternLike, /, *, flags: int = 0) -> Pattern[str]:
    match pattern:
        case Pattern():
            return pattern
        case str():
            return re.compile(pattern, flags=flags)
        case never:
            assert_never(never)


###############################################################################
#### reprlib ##################################################################
###############################################################################


def repr_(
    obj: Any,
    /,
    *,
    max_width: int = RICH_MAX_WIDTH,
    indent_size: int = RICH_INDENT_SIZE,
    max_length: int | None = RICH_MAX_LENGTH,
    max_string: int | None = RICH_MAX_STRING,
    max_depth: int | None = RICH_MAX_DEPTH,
    expand_all: bool = RICH_EXPAND_ALL,
) -> str:
    """Get the representation of an object."""
    try:
        from rich.pretty import pretty_repr
    except ModuleNotFoundError:  # pragma: no cover
        return reprlib.repr(obj)
    return pretty_repr(
        obj,
        max_width=max_width,
        indent_size=indent_size,
        max_length=max_length,
        max_string=max_string,
        max_depth=max_depth,
        expand_all=expand_all,
    )


##


def repr_str(
    obj: Any,
    /,
    *,
    max_width: int = RICH_MAX_WIDTH,
    indent_size: int = RICH_INDENT_SIZE,
    max_length: int | None = RICH_MAX_LENGTH,
    max_string: int | None = RICH_MAX_STRING,
    max_depth: int | None = RICH_MAX_DEPTH,
    expand_all: bool = RICH_EXPAND_ALL,
) -> str:
    """Get the representation of the string of an object."""
    return repr_(
        str(obj),
        max_width=max_width,
        indent_size=indent_size,
        max_length=max_length,
        max_string=max_string,
        max_depth=max_depth,
        expand_all=expand_all,
    )


###############################################################################
#### tempfile #################################################################
###############################################################################


class TemporaryDirectory:
    """Wrapper around `TemporaryDirectory` with a `Path` attribute."""

    def __init__(
        self,
        *,
        suffix: str | None = None,
        prefix: str | None = None,
        dir: PathLike | None = None,  # noqa: A002
        ignore_cleanup_errors: bool = False,
        delete: bool = True,
    ) -> None:
        super().__init__()
        self._temp_dir = _TemporaryDirectoryNoResourceWarning(
            suffix=suffix,
            prefix=prefix,
            dir=dir,
            ignore_cleanup_errors=ignore_cleanup_errors,
            delete=delete,
        )
        self.path = Path(self._temp_dir.name)

    def __enter__(self) -> Path:
        return Path(self._temp_dir.__enter__())

    def __exit__(
        self,
        exc: type[BaseException] | None,
        val: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._temp_dir.__exit__(exc, val, tb)


class _TemporaryDirectoryNoResourceWarning(tempfile.TemporaryDirectory):
    @classmethod
    @override
    def _cleanup(  # pyright: ignore[reportGeneralTypeIssues]
        cls,
        name: str,
        warn_message: str,
        ignore_errors: bool = False,
        delete: bool = True,
    ) -> None:
        with suppress_warnings(category=ResourceWarning):
            return super()._cleanup(  # pyright: ignore[reportAttributeAccessIssue]
                name, warn_message, ignore_errors=ignore_errors, delete=delete
            )


##


@contextmanager
def TemporaryFile(  # noqa: N802
    *,
    dir: PathLike | None = None,  # noqa: A002
    suffix: str | None = None,
    prefix: str | None = None,
    ignore_cleanup_errors: bool = False,
    delete: bool = True,
    name: str | None = None,
    data: bytes | None = None,
    text: str | None = None,
) -> Iterator[Path]:
    """Yield a temporary file."""
    if dir is None:
        with (
            TemporaryDirectory(
                suffix=suffix,
                prefix=prefix,
                dir=dir,
                ignore_cleanup_errors=ignore_cleanup_errors,
                delete=delete,
            ) as temp_dir,
            _temporary_file_outer(
                temp_dir,
                suffix=suffix,
                prefix=prefix,
                delete=delete,
                name=name,
                data=data,
                text=text,
            ) as temp,
        ):
            yield temp
    else:
        with _temporary_file_outer(
            dir,
            suffix=suffix,
            prefix=prefix,
            delete=delete,
            name=name,
            data=data,
            text=text,
        ) as temp:
            yield temp


@contextmanager
def _temporary_file_outer(
    path: PathLike,
    /,
    *,
    suffix: str | None = None,
    prefix: str | None = None,
    delete: bool = True,
    name: str | None = None,
    data: bytes | None = None,
    text: str | None = None,
) -> Iterator[Path]:
    with _temporary_file_inner(
        Path(path), suffix=suffix, prefix=prefix, delete=delete, name=name
    ) as temp:
        if data is not None:
            _ = temp.write_bytes(data)
        if text is not None:
            _ = temp.write_text(text)
        yield temp


@contextmanager
def _temporary_file_inner(
    path: Path,
    /,
    *,
    suffix: str | None = None,
    prefix: str | None = None,
    delete: bool = True,
    name: str | None = None,
) -> Iterator[Path]:
    with _NamedTemporaryFile(
        suffix=suffix, prefix=prefix, dir=path, delete=delete, delete_on_close=False
    ) as temp:
        if name is None:
            yield Path(path, temp.name)
        else:
            _ = shutil.move(path / temp.name, path / name)
            yield path / name


##


@contextmanager
def yield_adjacent_temp_dir(path: PathLike, /) -> Iterator[Path]:
    """Yield a temporary directory adjacent to target path."""

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(suffix=".tmp", prefix=path.name, dir=path.parent) as temp:
        yield temp


@contextmanager
def yield_adjacent_temp_file(path: PathLike, /) -> Iterator[Path]:
    """Yield a temporary file adjacent to target path."""

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryFile(dir=path.parent, suffix=".tmp", prefix=path.name) as temp:
        yield temp


###############################################################################
#### text #####################################################################
###############################################################################


def kebab_case(text: str, /) -> str:
    """Convert text into kebab case."""
    return _kebab_snake_case(text, "-")


def snake_case(text: str, /) -> str:
    """Convert text into snake case."""
    return _kebab_snake_case(text, "_")


def _kebab_snake_case(text: str, separator: str, /) -> str:
    """Convert text into kebab/snake case."""
    leading = _kebab_leading_pattern.search(text) is not None
    trailing = _kebab_trailing_pattern.search(text) is not None
    parts = _kebab_pascal_pattern.findall(text)
    parts = (p for p in parts if len(p) >= 1)
    parts = chain([""] if leading else [], parts, [""] if trailing else [])
    return separator.join(parts).lower()


_kebab_leading_pattern = re.compile(r"^_")
_kebab_trailing_pattern = re.compile(r"_$")


def pascal_case(text: str, /) -> str:
    """Convert text to pascal case."""
    parts = _kebab_pascal_pattern.findall(text)
    parts = [p for p in parts if len(p) >= 1]
    parts = list(map(_pascal_case_upper_or_title, parts))
    return "".join(parts)


def _pascal_case_upper_or_title(text: str, /) -> str:
    return text if text.isupper() else text.title()


_kebab_pascal_pattern = re.compile(
    r"""
    [A-Z]+(?=[A-Z][a-z0-9]) | # all caps followed by Upper+lower or digit (API in APIResponse2)
    [A-Z]?[a-z]+[0-9]*      | # normal words with optional trailing digits (Text123)
    [A-Z]+[0-9]*            | # consecutive caps with optional trailing digits (ID2)
    """,
    flags=VERBOSE,
)


##


def normalize_multi_line_str(text: str, /) -> str:
    """Normalize a multi-line string."""
    stripped = text.strip("\n")
    return normalize_str(dedent(stripped))


def normalize_str(text: str, /) -> str:
    """Normalize a string."""
    return text.strip("\n") + "\n"


##


def substitute(
    path_or_text: PathLike,
    /,
    *,
    environ: bool = False,
    mapping: StrMapping | None = None,
    safe: bool = False,
    **kwargs: Any,
) -> str:
    """Substitute from a Path or string."""
    match path_or_text:
        case Path() as path:
            return substitute(
                path.read_text(), environ=environ, mapping=mapping, safe=safe, **kwargs
            )
        case str() as text:
            template = Template(text)
            mapping_use: StrMapping = {} if mapping is None else mapping
            kwargs_use: StrDict = (os.environ if environ else {}) | kwargs
            if safe:
                return template.safe_substitute(mapping_use, **kwargs_use)
            try:
                return template.substitute(mapping_use, **kwargs_use)
            except KeyError as error:
                raise SubstituteError(key=error.args[0]) from None
        case never:
            assert_never(never)


@dataclass(kw_only=True, slots=True)
class SubstituteError(Exception):
    key: str

    @override
    def __str__(self) -> str:
        return f"Missing key: {repr_(self.key)}"


##


def unique_str() -> str:
    """Generate a unique string."""
    now = time_ns()
    pid = getpid()
    ident = get_ident()
    key = str(uuid4()).replace("-", "")
    return f"{now}_{pid}_{ident}_{key}"


###############################################################################
#### warnings #################################################################
###############################################################################


@contextmanager
def suppress_warnings(
    *, message: str = "", category: TypeLike[Warning] | None = None
) -> Iterator[None]:
    """Suppress warnings."""
    with _yield_caught_warnings("ignore", message=message, category=category):
        yield


@contextmanager
def yield_warnings_as_errors(
    *, message: str = "", category: TypeLike[Warning] | None = None
) -> Iterator[None]:
    """Catch warnings as errors."""
    with _yield_caught_warnings("error", message=message, category=category):
        yield


@contextmanager
def _yield_caught_warnings(
    action: FilterWarningsAction,
    /,
    *,
    message: str = "",
    category: TypeLike[Warning] | None = None,
) -> Iterator[None]:
    with catch_warnings():
        match category:
            case None:
                filterwarnings(action, message=message)
            case type():
                filterwarnings(action, message=message, category=category)
            case tuple():
                for c in category:
                    filterwarnings(action, message=message, category=c)
            case never:
                assert_never(never)
        yield


###############################################################################
#### whenever #################################################################
###############################################################################


def get_now(time_zone: TimeZoneLike = UTC, /) -> ZonedDateTime:
    """Get the current zoned date-time."""
    return _get_now(to_time_zone_name(time_zone))


def get_now_plain(time_zone: TimeZoneLike = UTC, /) -> PlainDateTime:
    """Get the current plain date-time."""
    return get_now(time_zone).to_plain()


def get_now_local_plain() -> PlainDateTime:
    """Get the current plain date-time in the local time-zone."""
    return get_now_local().to_plain()


##


def get_time(time_zone: TimeZoneLike = UTC, /) -> Time:
    """Get the current time."""
    return get_now(time_zone).time()


def get_time_local() -> Time:
    """Get the current time in the local time-zone."""
    return get_time(LOCAL_TIME_ZONE)


##


def get_today(time_zone: TimeZoneLike = UTC, /) -> Date:
    """Get the current, timezone-aware local date."""
    return get_now(time_zone).date()


def get_today_local() -> Date:
    """Get the current, timezone-aware local date."""
    return get_today(LOCAL_TIME_ZONE)


##


###############################################################################
#### writers ##################################################################
###############################################################################


def write_bytes(path: PathLike, data: bytes, /, *, overwrite: bool = False) -> None:
    """Write data to a file."""
    with yield_write_path(path, overwrite=overwrite) as temp:
        _ = temp.write_bytes(data)


def write_text(path: PathLike, text: str, /, *, overwrite: bool = False) -> None:
    """Write text to a file."""
    with yield_write_path(path, overwrite=overwrite) as temp:
        _ = temp.write_text(normalize_str(text))


##


@contextmanager
def yield_write_path(
    path: PathLike, /, *, compress: bool = False, overwrite: bool = False
) -> Iterator[Path]:
    """Yield a temporary path for atomically writing files to disk."""
    with yield_adjacent_temp_file(path) as temp:
        yield temp
        try:
            if compress:
                compress_gzip(temp, path, overwrite=overwrite)
            else:
                move(temp, path, overwrite=overwrite)
        except _CopyOrMoveDestinationExistsError as error:
            raise YieldWritePathError(path=error.dest) from None


@dataclass(kw_only=True, slots=True)
class YieldWritePathError(Exception):
    path: Path

    @override
    def __str__(self) -> str:
        return f"Cannot write to {repr_str(self.path)} since it already exists"


###############################################################################
#### zoneinfo #################################################################
###############################################################################


def to_zone_info(obj: TimeZoneLike, /) -> ZoneInfo:
    """Convert to a time-zone."""
    match obj:
        case ZoneInfo() as zone_info:
            return zone_info
        case ZonedDateTime() as date_time:
            return ZoneInfo(date_time.tz)
        case "local" | "localtime":
            return LOCAL_TIME_ZONE
        case str() as key:
            return ZoneInfo(key)
        case dt.tzinfo() as tzinfo:
            if tzinfo is dt.UTC:
                return UTC
            raise _ToZoneInfoInvalidTZInfoError(time_zone=obj)
        case dt.datetime() as date_time:
            if date_time.tzinfo is None:
                raise _ToZoneInfoPlainDateTimeError(date_time=date_time)
            return to_zone_info(date_time.tzinfo)
        case never:
            assert_never(never)


@dataclass(kw_only=True, slots=True)
class ToTimeZoneError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _ToZoneInfoInvalidTZInfoError(ToTimeZoneError):
    time_zone: dt.tzinfo

    @override
    def __str__(self) -> str:
        return f"Invalid time-zone: {self.time_zone}"


@dataclass(kw_only=True, slots=True)
class _ToZoneInfoPlainDateTimeError(ToTimeZoneError):
    date_time: dt.datetime

    @override
    def __str__(self) -> str:
        return f"Plain date-time: {self.date_time}"


##


def to_time_zone_name(obj: TimeZoneLike, /) -> TimeZone:
    """Convert to a time zone name."""
    match obj:
        case ZoneInfo() as zone_info:
            return cast("TimeZone", zone_info.key)
        case ZonedDateTime() as date_time:
            return cast("TimeZone", date_time.tz)
        case "local" | "localtime":
            return LOCAL_TIME_ZONE_NAME
        case str() as time_zone:
            if time_zone in TIME_ZONES:
                return time_zone
            raise _ToTimeZoneNameInvalidKeyError(time_zone=time_zone)
        case dt.tzinfo() as tzinfo:
            if tzinfo is dt.UTC:
                return cast("TimeZone", UTC.key)
            raise _ToTimeZoneNameInvalidTZInfoError(time_zone=obj)
        case dt.datetime() as date_time:
            if date_time.tzinfo is None:
                raise _ToTimeZoneNamePlainDateTimeError(date_time=date_time)
            return to_time_zone_name(date_time.tzinfo)
        case never:
            assert_never(never)


@dataclass(kw_only=True, slots=True)
class ToTimeZoneNameError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _ToTimeZoneNameInvalidKeyError(ToTimeZoneNameError):
    time_zone: str

    @override
    def __str__(self) -> str:
        return f"Invalid time-zone: {self.time_zone!r}"


@dataclass(kw_only=True, slots=True)
class _ToTimeZoneNameInvalidTZInfoError(ToTimeZoneNameError):
    time_zone: dt.tzinfo

    @override
    def __str__(self) -> str:
        return f"Invalid time-zone: {self.time_zone}"


@dataclass(kw_only=True, slots=True)
class _ToTimeZoneNamePlainDateTimeError(ToTimeZoneNameError):
    date_time: dt.datetime

    @override
    def __str__(self) -> str:
        return f"Plain date-time: {self.date_time}"


__all__ = [
    "CompressBZ2Error",
    "CompressGzipError",
    "CompressLZMAError",
    "ExtractGroupError",
    "ExtractGroupsError",
    "FileOrDirError",
    "GetEnvError",
    "MaxNullableError",
    "MinNullableError",
    "OneEmptyError",
    "OneError",
    "OneNonUniqueError",
    "OneStrEmptyError",
    "OneStrError",
    "OneStrNonUniqueError",
    "SubstituteError",
    "TemporaryDirectory",
    "TemporaryFile",
    "ToTimeZoneError",
    "ToTimeZoneNameError",
    "always_iterable",
    "chunked",
    "compress_bz2",
    "compress_gzip",
    "compress_lzma",
    "extract_group",
    "extract_groups",
    "file_or_dir",
    "get_class",
    "get_class_name",
    "get_env",
    "get_func_name",
    "get_now",
    "get_now_local",
    "get_now_local_plain",
    "get_now_plain",
    "get_time",
    "get_time_local",
    "get_today",
    "get_today_local",
    "is_none",
    "is_not_none",
    "is_sentinel",
    "max_nullable",
    "min_nullable",
    "normalize_multi_line_str",
    "normalize_str",
    "not_func",
    "one",
    "one_str",
    "repr_",
    "repr_str",
    "substitute",
    "suppress_super_attribute_error",
    "suppress_warnings",
    "take",
    "to_time_zone_name",
    "to_zone_info",
    "transpose",
    "unique_everseen",
    "unique_str",
    "write_bytes",
    "write_text",
    "yield_adjacent_temp_dir",
    "yield_adjacent_temp_file",
    "yield_bz2",
    "yield_gzip",
    "yield_lzma",
    "yield_temp_cwd",
    "yield_temp_environ",
    "yield_warnings_as_errors",
]
