# ruff: noqa: RUF001
from __future__ import annotations

import asyncio
import datetime as dt
import gzip
import math
import os
import pickle
import re
import shutil
import sys
import tempfile
import time
from bz2 import BZ2File
from collections import Counter
from collections.abc import Callable, Hashable, Iterable, Iterator, MutableMapping
from contextlib import ExitStack, contextmanager, suppress
from dataclasses import dataclass, replace
from functools import _lru_cache_wrapper, partial, reduce, wraps
from gzip import GzipFile
from itertools import chain, islice
from logging import (
    CRITICAL,
    DEBUG,
    ERROR,
    INFO,
    WARNING,
    Filter,
    Formatter,
    Handler,
    Logger,
    LoggerAdapter,
    LogRecord,
    StreamHandler,
    getLevelName,
    getLevelNamesMapping,
    getLogger,
    setLogRecordFactory,
)
from logging.handlers import RotatingFileHandler
from lzma import LZMAFile
from operator import or_
from os import chdir, environ, getenv, getpid
from pathlib import Path
from re import VERBOSE, Pattern, findall
from shutil import copyfile, copyfileobj, copytree
from stat import (
    S_IMODE,
    S_IRGRP,
    S_IROTH,
    S_IRUSR,
    S_IWGRP,
    S_IWOTH,
    S_IWUSR,
    S_IXGRP,
    S_IXOTH,
    S_IXUSR,
)
from string import Template
from subprocess import CalledProcessError, check_output
from tarfile import ReadError, TarFile
from tempfile import NamedTemporaryFile as _NamedTemporaryFile
from textwrap import dedent, indent
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
from typing import (
    TYPE_CHECKING,
    Any,
    Concatenate,
    Literal,
    Self,
    assert_never,
    cast,
    overload,
    override,
)
from uuid import uuid4
from warnings import catch_warnings, filterwarnings
from zipfile import ZipFile
from zoneinfo import ZoneInfo

from coloredlogs import ColoredFormatter
from rich.console import Console
from rich.pretty import pretty_repr
from rich.table import Table
from typing_extensions import TypeIs
from whenever import (
    Date,
    DateDelta,
    DateTimeDelta,
    PlainDateTime,
    Time,
    TimeDelta,
    ZonedDateTime,
)

import utilities.constants
from utilities._core_errors import (
    CheckUniqueError,
    CompressBZ2Error,
    CompressGzipError,
    CompressLZMAError,
    CompressZipError,
    CopyDestinationExistsError,
    CopyError,
    CopySourceNotFoundError,
    DeltaComponentsError,
    ExtractGroupError,
    ExtractGroupMultipleCaptureGroupsError,
    ExtractGroupMultipleMatchesError,
    ExtractGroupNoCaptureGroupsError,
    ExtractGroupNoMatchesError,
    ExtractGroupsError,
    ExtractGroupsMultipleMatchesError,
    ExtractGroupsNoCaptureGroupsError,
    ExtractGroupsNoMatchesError,
    FileOrDirError,
    FileOrDirMissingError,
    FileOrDirTypeError,
    FirstNonDirectoryParentError,
    GetEnvError,
    GetLoggingLevelNameError,
    GetLoggingLevelNumberError,
    MaxNullableError,
    MaybeColoredFormatterError,
    MinNullableError,
    MoveDestinationExistsError,
    MoveError,
    MoveSourceNotFoundError,
    NumDaysError,
    NumHoursError,
    NumMicroSecondsError,
    NumMilliSecondsError,
    NumMinutesError,
    NumMonthsError,
    NumNanoSecondsError,
    NumSecondsError,
    NumWeeksError,
    NumYearsError,
    OneEmptyError,
    OneError,
    OneNonUniqueError,
    OneStrEmptyError,
    OneStrError,
    OneStrNonUniqueError,
    PermissionsError,
    PermissionsFromHumanIntDigitError,
    PermissionsFromHumanIntRangeError,
    PermissionsFromIntError,
    PermissionsFromTextError,
    ReadBytesError,
    ReadBytesFileNotFoundError,
    ReadBytesIsADirectoryError,
    ReadBytesNotADirectoryError,
    ReadPickleError,
    ReadPickleFileNotFoundError,
    ReadPickleIsADirectoryError,
    ReadPickleNotADirectoryError,
    ReadTextError,
    ReadTextFileNotFoundError,
    ReadTextIfExistingFileError,
    ReadTextIfExistingFileIsADirectoryError,
    ReadTextIfExistingFileNotADirectoryError,
    ReadTextIsADirectoryError,
    ReadTextNotADirectoryError,
    ReprTableError,
    ReprTableHeaderError,
    ReprTableItemsError,
    SubstituteError,
    ToTimeZoneNameError,
    ToTimeZoneNameInvalidKeyError,
    ToTimeZoneNameInvalidTZInfoError,
    ToTimeZoneNamePlainDateTimeError,
    ToZoneInfoError,
    ToZoneInfoInvalidTZInfoError,
    ToZoneInfoPlainDateTimeError,
    WhichError,
    WriteBytesError,
    WritePickleError,
    WriteTextError,
    YieldBZ2Error,
    YieldBZ2FileNotFoundError,
    YieldBZ2IsADirectoryError,
    YieldBZ2NotADirectoryError,
    YieldGzipError,
    YieldGzipFileNotFoundError,
    YieldGzipIsADirectoryError,
    YieldGzipNotADirectoryError,
    YieldLZMAError,
    YieldLZMAFileNotFoundError,
    YieldLZMAIsADirectoryError,
    YieldLZMANotADirectoryError,
    YieldUncompressedError,
    YieldUncompressedFileNotFoundError,
    YieldUncompressedIsADirectoryError,
    YieldUncompressedNotADirectoryError,
    YieldWritePathError,
    YieldZipError,
    YieldZipFileNotFoundError,
    YieldZipIsADirectoryError,
    YieldZipNotADirectoryError,
)
from utilities._core_errors import CompressFilesError as _CompressFilesError
from utilities._core_errors import (
    CopyOrMoveDestinationExistsError as _CopyOrMoveDestinationExistsError,
)
from utilities._core_errors import (
    CopyOrMoveSourceNotFoundError as _CopyOrMoveSourceNotFoundError,
)
from utilities._core_errors import (
    YieldUncompressedFileNotFoundError as _YieldUncompressedFileNotFoundError,
)
from utilities._core_errors import (
    YieldUncompressedIsADirectoryError as _YieldUncompressedIsADirectoryError,
)
from utilities._core_errors import (
    YieldUncompressedNotADirectoryError as _YieldUncompressedNotADirectoryError,
)
from utilities.constants import (
    ABS_TOL,
    BACKUP_COUNT,
    CUSTOM_FIELD_STYLES,
    CUSTOM_LEVEL_STYLES,
    DAYS_PER_WEEK,
    HOSTNAME,
    HOURS_PER_DAY,
    HOURS_PER_WEEK,
    LOCAL_TIME_ZONE,
    LOCAL_TIME_ZONE_NAME,
    MAX_BYTES,
    MICROSECONDS_PER_DAY,
    MICROSECONDS_PER_HOUR,
    MICROSECONDS_PER_MILLISECOND,
    MICROSECONDS_PER_MINUTE,
    MICROSECONDS_PER_SECOND,
    MICROSECONDS_PER_WEEK,
    MILLISECONDS_PER_DAY,
    MILLISECONDS_PER_HOUR,
    MILLISECONDS_PER_MINUTE,
    MILLISECONDS_PER_SECOND,
    MILLISECONDS_PER_WEEK,
    MINUTES_PER_DAY,
    MINUTES_PER_HOUR,
    MINUTES_PER_WEEK,
    MONTHS_PER_YEAR,
    NANOSECONDS_PER_DAY,
    NANOSECONDS_PER_HOUR,
    NANOSECONDS_PER_MICROSECOND,
    NANOSECONDS_PER_MILLISECOND,
    NANOSECONDS_PER_MINUTE,
    NANOSECONDS_PER_SECOND,
    NANOSECONDS_PER_WEEK,
    REL_TOL,
    RICH_EXPAND_ALL,
    RICH_INDENT_SIZE,
    RICH_MAX_DEPTH,
    RICH_MAX_LENGTH,
    RICH_MAX_STRING,
    RICH_MAX_WIDTH,
    RICH_SHOW_EDGE,
    RICH_SHOW_LINES,
    SECONDS_PER_DAY,
    SECONDS_PER_HOUR,
    SECONDS_PER_MINUTE,
    SECONDS_PER_WEEK,
    UTC,
    Sentinel,
    _get_now,
    sentinel,
)
from utilities.types import (
    LOG_LEVELS,
    TIME_ZONES,
    ArgsAndKwargs,
    CopyOrMove,
    Dataclass,
    Duration,
    FilterWarningsAction,
    LoggerLike,
    LogLevel,
    MaybeCallableDateLike,
    MaybeType,
    Number,
    Pair,
    PathToBinaryIO,
    PatternLike,
    SequenceStr,
    StrDict,
    StrMapping,
    SupportsRichComparison,
    TableLike,
    TimeZone,
    TimeZoneLike,
    Triple,
    TypeLike,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping, Sequence
    from contextvars import ContextVar
    from logging import _ExcInfoType, _FilterType, _FormatStyle
    from types import TracebackType

    from whenever import PlainDateTime, Time

    from utilities.types import Delta, FileOrDir, MaybeIterable, PathLike


###############################################################################
#### asyncio ##################################################################
###############################################################################


async def async_sleep(duration: Duration | None = None, /) -> None:
    """Sleep which accepts durations."""
    match duration:
        case int() | float():
            await asyncio.sleep(duration)
        case DateDelta() | TimeDelta() | DateTimeDelta():
            await asyncio.sleep(num_nanoseconds(duration) / NANOSECONDS_PER_SECOND)
        case None:
            ...
        case never:
            assert_never(never)


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


###############################################################################
#### coloredlogs ##############################################################
###############################################################################


class MaybeColoredFormatter(Formatter):
    """Formatter with easy toggling of colour."""

    _formatter: Formatter

    @override
    def __init__(
        self,
        fmt: str | None = None,
        datefmt: str | None = None,
        style: _FormatStyle = "%",
        validate: bool = True,
        *,
        defaults: StrMapping | None = None,
        color: bool = False,
    ) -> None:
        if (datefmt is not None) or (style != "%") or (defaults is not None):
            raise MaybeColoredFormatterError(
                datefmt=datefmt, style=style, defaults=defaults
            )
        super().__init__(fmt, datefmt, "{", validate, defaults=defaults)
        if color:
            self._formatter = ColoredFormatter(
                fmt=fmt,
                style="{",
                level_styles=CUSTOM_LEVEL_STYLES,
                field_styles=CUSTOM_FIELD_STYLES,
            )
        else:
            self._formatter = Formatter(fmt=fmt, style="{")

    @override
    def format(self, record: LogRecord) -> str:
        return self._formatter.format(record)


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


def _compress_files(
    func: PathToBinaryIO,
    src_or_dest: PathLike,
    /,
    *srcs_or_dest: PathLike,
    overwrite: bool = False,
    perms: PermissionsLike | None = None,
    owner: str | int | None = None,
    group: str | int | None = None,
) -> None:
    *srcs, dest = map(Path, [src_or_dest, *srcs_or_dest])
    try:
        with (
            yield_write_path(
                dest, overwrite=overwrite, perms=perms, owner=owner, group=group
            ) as temp,
            func(temp) as buffer,
        ):
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


def _compress_files_add_dir(path: PathLike, tar: TarFile, /) -> None:
    path = Path(path)
    for p in sorted(path.rglob("**/*")):
        tar.add(p, p.relative_to(path))


##


def compress_zip(
    src_or_dest: PathLike,
    /,
    *srcs_or_dest: PathLike,
    overwrite: bool = False,
    perms: PermissionsLike | None = None,
    owner: str | int | None = None,
    group: str | int | None = None,
) -> None:
    """Create a Zip file."""
    *srcs, dest = map(Path, [src_or_dest, *srcs_or_dest])
    try:
        with (
            yield_write_path(
                dest, overwrite=overwrite, perms=perms, owner=owner, group=group
            ) as temp,
            ZipFile(temp, mode="w") as zf,
        ):
            for src_i in sorted(srcs):
                match file_or_dir(src_i):
                    case "file":
                        zf.write(src_i, src_i.name)
                    case "dir":
                        for p in sorted(src_i.rglob("**/*")):
                            zf.write(p, p.relative_to(src_i))
                    case None:
                        ...
                    case never:
                        assert_never(never)
    except YieldWritePathError as error:
        raise CompressZipError(srcs=srcs, dest=error.path) from None


##


@contextmanager
def yield_bz2(path: PathLike, /) -> Iterator[Path]:
    """Yield the contents of a BZ2 file."""

    def func(path: PathLike, /) -> BZ2File:
        return BZ2File(path, mode="rb")

    try:
        with _yield_uncompressed(path, cast("PathToBinaryIO", func)) as temp:
            yield temp
    except _YieldUncompressedFileNotFoundError as error:
        raise YieldBZ2FileNotFoundError(path=error.path) from None
    except _YieldUncompressedIsADirectoryError as error:
        raise YieldBZ2IsADirectoryError(path=error.path) from None
    except _YieldUncompressedNotADirectoryError as error:
        raise YieldBZ2NotADirectoryError(
            path=error.path, parent=first_non_directory_parent(error.path)
        ) from None


@contextmanager
def yield_gzip(path: PathLike, /) -> Iterator[Path]:
    """Yield the contents of a Gzip file."""

    def func(path: PathLike, /) -> GzipFile:
        return GzipFile(path, mode="rb")

    try:
        with _yield_uncompressed(path, cast("PathToBinaryIO", func)) as temp:
            yield temp
    except _YieldUncompressedFileNotFoundError as error:
        raise YieldGzipFileNotFoundError(path=error.path) from None
    except _YieldUncompressedIsADirectoryError as error:
        raise YieldGzipIsADirectoryError(path=error.path) from None
    except _YieldUncompressedNotADirectoryError as error:
        raise YieldGzipNotADirectoryError(
            path=error.path, parent=first_non_directory_parent(error.path)
        ) from None


@contextmanager
def yield_lzma(path: PathLike, /) -> Iterator[Path]:
    """Yield the contents of an LZMA file."""

    def func(path: PathLike, /) -> LZMAFile:
        return LZMAFile(path, mode="rb")

    try:
        with _yield_uncompressed(path, cast("PathToBinaryIO", func)) as temp:
            yield temp
    except _YieldUncompressedFileNotFoundError as error:
        raise YieldLZMAFileNotFoundError(path=error.path) from None
    except _YieldUncompressedIsADirectoryError as error:
        raise YieldLZMAIsADirectoryError(path=error.path) from None
    except _YieldUncompressedNotADirectoryError as error:
        raise YieldLZMANotADirectoryError(
            path=error.path, parent=first_non_directory_parent(error.path)
        ) from None


@contextmanager
def _yield_uncompressed(path: PathLike, func: PathToBinaryIO, /) -> Iterator[Path]:
    path = Path(path)
    try:
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
    except FileNotFoundError:
        raise _YieldUncompressedFileNotFoundError(path=path) from None
    except IsADirectoryError:
        raise _YieldUncompressedIsADirectoryError(path=path) from None
    except NotADirectoryError:
        raise _YieldUncompressedNotADirectoryError(path=path) from None


##


@contextmanager
def yield_zip(path: PathLike, /) -> Iterator[Path]:
    """Yield the contents of a Zip file."""
    path = Path(path)
    try:
        with ZipFile(path) as zf, TemporaryDirectory() as temp:
            zf.extractall(path=temp)
            try:
                yield one(temp.iterdir())
            except (OneEmptyError, OneNonUniqueError):
                yield temp
    except FileNotFoundError:
        raise YieldZipFileNotFoundError(path=path) from None
    except IsADirectoryError:
        raise YieldZipIsADirectoryError(path=path) from None
    except NotADirectoryError:
        raise YieldZipNotADirectoryError(
            path=path, parent=first_non_directory_parent(path)
        ) from None


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
#### contextvars ##############################################################
###############################################################################


@contextmanager
def yield_temp_context_var(var: ContextVar[bool], /) -> Iterator[None]:
    """Yield a temporary boolean context var as True."""
    token = var.set(True)
    try:
        yield
    finally:
        _ = var.reset(token)


###############################################################################
#### dataclass ################################################################
###############################################################################


@overload
def replace_non_sentinel(
    obj: Dataclass, /, *, in_place: Literal[True], **kwargs: Any
) -> None: ...
@overload
def replace_non_sentinel[T: Dataclass](
    obj: T, /, *, in_place: Literal[False] = False, **kwargs: Any
) -> T: ...
@overload
def replace_non_sentinel[T: Dataclass](
    obj: T, /, *, in_place: bool = False, **kwargs: Any
) -> T | None: ...
def replace_non_sentinel[T: Dataclass](
    obj: T, /, *, in_place: bool = False, **kwargs: Any
) -> T | None:
    """Replace attributes on a dataclass, filtering out sentinel values."""
    if in_place:
        for k, v in kwargs.items():
            if not is_sentinel(v):
                setattr(obj, k, v)
        return None
    return replace(obj, **{k: v for k, v in kwargs.items() if not is_sentinel(v)})


###############################################################################
#### errors ###################################################################
###############################################################################


def repr_error(error: MaybeType[BaseException], /) -> str:
    """Get a string representation of an error."""
    match error:
        case ExceptionGroup() as group:
            descs = list(map(repr_error, group.exceptions))
            joined = ", ".join(descs)
            return f"{get_class_name(group)}({joined})"
        case CalledProcessError():
            table = repr_table(
                ("returncode", error.returncode),
                ("cmd", error.cmd),
                ("stdout", error.stdout),
                ("stderr", error.stderr),
                show_edge=False,
            )
            table = normalize_multi_line_str(table)
            indented = indent(table, 4 * " ")
            return f"{get_class_name(error)}(\n{indented})"
        case BaseException():
            return f"{get_class_name(error)}({error})"
        case type():
            return get_class_name(error)
        case never:
            assert_never(never)


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
#### grp ######################################################################
###############################################################################


get_gid_name = utilities.constants._get_gid_name  # noqa: SLF001


def get_file_group(path: PathLike, /) -> str | None:
    """Get the group of a file."""
    gid = Path(path).stat().st_gid
    return get_gid_name(gid)


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


def check_unique(*items: Hashable) -> None:
    """Check an iterable contains only unique items."""
    counts = {k: v for k, v in Counter(items).items() if v > 1}
    if len(counts) >= 1:
        raise CheckUniqueError(items=items, counts=counts)


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
#### logging ##################################################################
###############################################################################


def add_adapter[**P](
    logger: LoggerLike,
    process: Callable[Concatenate[str, P], str],
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> LoggerAdapter:
    """Add an adapter to a logger."""

    class CustomAdapter(LoggerAdapter):
        @override
        def process(
            self, msg: str, kwargs: MutableMapping[str, Any]
        ) -> tuple[str, MutableMapping[str, Any]]:
            extra = cast("ArgsAndKwargs", self.extra)
            new_msg = process(msg, *extra["args"], **extra["kwargs"])
            return new_msg, kwargs

    return CustomAdapter(logger, extra=ArgsAndKwargs(args=args, kwargs=kwargs))


##


def add_filters(
    obj: LoggerLike | Handler, filter_: _FilterType, /, *filters: _FilterType
) -> None:
    """Add a set of filters to a handler."""
    all_filters = [filter_, *filters]
    match obj:
        case Logger() | str() as logger_like:
            logger = to_logger(logger_like)
            for filter_i in all_filters:
                logger.addFilter(filter_i)
        case Handler() as handler:
            for filter_i in all_filters:
                handler.addFilter(filter_i)
        case never:
            assert_never(never)


##


def get_logging_level_name(level: int, /) -> LogLevel:
    """Get the logging level name."""
    name = getLevelName(level)
    if name in LOG_LEVELS:
        return name
    raise GetLoggingLevelNameError(level=level) from None


def get_logging_level_number(level: LogLevel, /) -> int:
    """Get the logging level number."""
    mapping = getLevelNamesMapping()
    try:
        return mapping[level]
    except KeyError:
        raise GetLoggingLevelNumberError(level=level) from None


##


def log_debug(
    logger: LoggerLike | None,
    msg: str,
    *args: Any,
    exc_info: _ExcInfoType | None = None,
    stack_info: bool = False,
    stacklevel: int = 1,
    extra: StrMapping | None = None,
) -> None:
    """Log at the debug level."""
    _log_if_given(
        logger,
        DEBUG,
        msg,
        *args,
        exc_info=exc_info,
        stack_info=stack_info,
        stacklevel=stacklevel + 1,
        extra=extra,
    )


def log_info(
    logger: LoggerLike | None,
    msg: str,
    *args: Any,
    exc_info: _ExcInfoType | None = None,
    stack_info: bool = False,
    stacklevel: int = 1,
    extra: StrMapping | None = None,
) -> None:
    """Log at the info level."""
    _log_if_given(
        logger,
        INFO,
        msg,
        *args,
        exc_info=exc_info,
        stack_info=stack_info,
        stacklevel=stacklevel + 1,
        extra=extra,
    )


def log_warning(
    logger: LoggerLike | None,
    msg: str,
    *args: Any,
    exc_info: _ExcInfoType | None = None,
    stack_info: bool = False,
    stacklevel: int = 1,
    extra: StrMapping | None = None,
) -> None:
    """Log at the warning level."""
    _log_if_given(
        logger,
        WARNING,
        msg,
        *args,
        exc_info=exc_info,
        stack_info=stack_info,
        stacklevel=stacklevel + 1,
        extra=extra,
    )


def log_error(
    logger: LoggerLike | None,
    msg: str,
    *args: Any,
    exc_info: _ExcInfoType | None = None,
    stack_info: bool = False,
    stacklevel: int = 1,
    extra: StrMapping | None = None,
) -> None:
    """Log at the error level."""
    _log_if_given(
        logger,
        ERROR,
        msg,
        *args,
        exc_info=exc_info,
        stack_info=stack_info,
        stacklevel=stacklevel + 1,
        extra=extra,
    )


def log_exception(
    logger: LoggerLike | None,
    msg: str,
    *args: Any,
    exc_info: _ExcInfoType = True,
    stack_info: bool = False,
    stacklevel: int = 1,
    extra: StrMapping | None = None,
) -> None:
    """Log at the error level with exception information."""
    return log_error(
        logger,
        msg,
        *args,
        exc_info=exc_info,
        stack_info=stack_info,
        stacklevel=stacklevel + 1,
        extra=extra,
    )


def log_critical(
    logger: LoggerLike | None,
    msg: str,
    *args: Any,
    exc_info: _ExcInfoType | None = None,
    stack_info: bool = False,
    stacklevel: int = 1,
    extra: StrMapping | None = None,
) -> None:
    """Log at the critical level."""
    _log_if_given(
        logger,
        CRITICAL,
        msg,
        *args,
        exc_info=exc_info,
        stack_info=stack_info,
        stacklevel=stacklevel + 1,
        extra=extra,
    )


def _log_if_given(
    logger: LoggerLike | None,
    level: int,
    msg: str,
    *args: Any,
    exc_info: _ExcInfoType | None = None,
    stack_info: bool = False,
    stacklevel: int = 1,
    extra: StrMapping | None = None,
) -> None:
    """Log at the critical level."""
    if logger is not None:
        logger_use = to_logger(logger)
        logger_use.log(
            level,
            msg,
            *args,
            exc_info=exc_info,
            stack_info=stack_info,
            stacklevel=stacklevel + 1,
            extra=extra,
        )


##


def set_up_logging(
    logger: LoggerLike,
    /,
    *,
    root: bool = False,
    filters: MaybeIterable[_FilterType] | None = None,
    console_color: bool = True,
    console_debug: bool = False,
    files: PathLike | None = None,
    max_bytes: int = MAX_BYTES,
    backup_count: int = BACKUP_COUNT,
) -> None:
    """Setup logging."""
    setLogRecordFactory(EnhancedLogRecord)
    logger = to_logger(logger, root=root)
    logger.setLevel(DEBUG)
    if filters is not None:
        add_filters(logger, *always_iterable(filters))
    console_fmt = _ConsoleFormatter(color=console_color)
    for stream, level, filter_ in [
        (sys.stdout, DEBUG if console_debug else INFO, _StdOutFilter()),
        (sys.stderr, WARNING, None),
    ]:
        _set_up_logging_console_handlers(
            stream, console_fmt, level, logger, filter_=filter_
        )
    if files is not None:
        single_fmt = _SingleFormatter(
            fmt="{date_basic}T{time_basic}.{micros}[{time_zone}] | {hostname}:{name}:{funcName}:{lineno} | {levelname} | {process} | {message}",
            style="{",
        )
        for stem, level, bc, formatter in [
            ("live-${level}", DEBUG, 1, console_fmt),
            ("live-${level}", INFO, 1, console_fmt),
            ("log-${level}", DEBUG, backup_count, single_fmt),
            ("log-${level}", INFO, backup_count, single_fmt),
            ("log-${level}", ERROR, backup_count, single_fmt),
        ]:
            _set_up_logging_file_handlers(
                stem, level, files, bc, formatter, logger, max_bytes=max_bytes
            )


def _set_up_logging_console_handlers(
    stream: Any,
    formatter: _ConsoleFormatter,
    level: int,
    logger: Logger,
    /,
    *,
    filter_: _FilterType | None = None,
) -> None:
    handler = StreamHandler(stream=stream)
    handler.setFormatter(formatter)
    handler.setLevel(level)
    if filter_ is not None:
        handler.addFilter(filter_)
    logger.addHandler(handler)


def _set_up_logging_file_handlers(
    stem: str,
    level: int,
    path: PathLike,
    backup_count: int,
    formatter: Formatter,
    logger: Logger,
    /,
    *,
    max_bytes: int = MAX_BYTES,
) -> None:
    stem = substitute(stem, safe=True, level=get_logging_level_name(level).lower())
    filename = Path(path, f"{stem}.txt")
    filename.parent.mkdir(parents=True, exist_ok=True)
    handler = RotatingFileHandler(
        filename, maxBytes=max_bytes, backupCount=backup_count
    )
    handler.setFormatter(formatter)
    handler.setLevel(level)
    logger.addHandler(handler)


class _ConsoleFormatter(Formatter):
    @override
    def __init__(
        self,
        fmt: str | None = None,
        datefmt: str | None = None,
        style: _FormatStyle = "%",
        validate: bool = True,
        *,
        defaults: Mapping[str, Any] | None = None,
        color: bool = False,
    ) -> None:
        super().__init__(fmt, datefmt, style, validate, defaults=defaults)
        header = "{date} {time}.{micros}[{time_zone}] │ {hostname} ❯ {name} ❯ {funcName} ❯ {lineno} │ {levelname} │ {process}"
        self._empty = MaybeColoredFormatter(fmt=header, color=color)
        self._non_empty = MaybeColoredFormatter(
            fmt=f"{header}\n{{message}}", color=color
        )

    @override
    def format(self, record: LogRecord) -> str:
        if len(record.msg) >= 1:
            return indent_non_head(self._non_empty.format(record), "  ")
        return self._empty.format(record)


class _StdOutFilter(Filter):
    @override
    def filter(self, record: LogRecord) -> bool | LogRecord:
        return record.levelno <= INFO


class _SingleFormatter(Formatter):
    @override
    def format(self, record: LogRecord) -> str:
        return super().format(record).replace("\n", " ")


##


def to_logger(logger: LoggerLike, /, *, root: bool = False) -> Logger:
    """Convert to a logger."""
    match logger, root:
        case Logger(), False:
            return logger
        case Logger(), True:
            first, *_ = logger.name.split(".")
            return getLogger(name=first)
        case str(), _:
            return to_logger(getLogger(name=logger), root=root)
        case never:
            assert_never(never)


##


class EnhancedLogRecord(LogRecord):
    """Enhanced log record."""

    hostname: str
    zoned_date_time: ZonedDateTime
    date: str
    date_basic: str
    time: str
    time_basic: str
    micros: str
    time_zone: str

    @override
    def __init__(
        self,
        name: str,
        level: int,
        pathname: str,
        lineno: int,
        msg: object,
        args: Any,
        exc_info: Any,
        func: str | None = None,
        sinfo: str | None = None,
    ) -> None:
        super().__init__(
            name, level, pathname, lineno, msg, args, exc_info, func, sinfo
        )
        self.hostname = HOSTNAME
        zoned_date_time = self.zoned_date_time = get_now_local()
        date = zoned_date_time.date()
        self.date = date.format_iso()
        self.date_basic = date.format_iso(basic=True)
        time = zoned_date_time.time().replace(nanosecond=0)
        self.time = time.format_iso()
        self.time_basic = time.format_iso(basic=True)
        self.micros = format(zoned_date_time.nanosecond // 1000, "06d")
        self.time_zone = LOCAL_TIME_ZONE_NAME


ENHANCED_LOG_RECORD_EXTRA_ATTRS = {
    "hostname",
    "zoned_date_time",
    "date",
    "date_basic",
    "time",
    "time_basic",
    "micros",
    "time_zone",
}


###############################################################################
#### math #####################################################################
###############################################################################


def is_close(
    x: float, y: float, /, *, rel_tol: float = REL_TOL, abs_tol: float = ABS_TOL
) -> bool:
    """Check if x == y."""
    return math.isclose(
        x,
        y,
        **({} if rel_tol is None else {"rel_tol": rel_tol}),
        **({} if abs_tol is None else {"abs_tol": abs_tol}),
    )


##


###############################################################################
#### os #######################################################################
###############################################################################


def chmod(
    path: PathLike, perms: PermissionsLike, /, *, recursive: bool = False
) -> None:
    """Change file mode."""
    path = Path(path)
    paths = list(path.rglob("**/*")) if recursive else [path]
    for p in paths:
        p.chmod(int(Permissions.new(perms)))


##


def copy(
    src: PathLike,
    dest: PathLike,
    /,
    *,
    overwrite: bool = False,
    perms: PermissionsLike | None = None,
    owner: str | int | None = None,
    group: str | int | None = None,
) -> None:
    """Copy a file atomically."""
    src, dest = map(Path, [src, dest])
    try:
        _copy_or_move(
            src,
            dest,
            "copy",
            overwrite=overwrite,
            perms=perms,
            owner=owner,
            group=group,
        )
    except _CopyOrMoveSourceNotFoundError as error:
        raise CopySourceNotFoundError(src=error.src) from None
    except _CopyOrMoveDestinationExistsError as error:
        raise CopyDestinationExistsError(src=error.src, dest=error.dest) from None


def move(
    src: PathLike,
    dest: PathLike,
    /,
    *,
    overwrite: bool = False,
    perms: PermissionsLike | None = None,
    owner: str | int | None = None,
    group: str | int | None = None,
) -> None:
    """Move a file atomically."""
    src, dest = map(Path, [src, dest])
    try:
        _copy_or_move(
            src,
            dest,
            "move",
            overwrite=overwrite,
            perms=perms,
            owner=owner,
            group=group,
        )
    except _CopyOrMoveSourceNotFoundError as error:
        raise MoveSourceNotFoundError(src=error.src) from None
    except _CopyOrMoveDestinationExistsError as error:
        raise MoveDestinationExistsError(src=error.src, dest=error.dest) from None


def _copy_or_move(
    src: Path,
    dest: Path,
    mode: CopyOrMove,
    /,
    *,
    overwrite: bool = False,
    perms: PermissionsLike | None = None,
    owner: str | int | None = None,
    group: str | int | None = None,
) -> None:
    match file_or_dir(src), file_or_dir(dest), overwrite:
        case None, _, _:
            raise _CopyOrMoveSourceNotFoundError(src=src)
        case "file" | "dir", "file" | "dir", False:
            raise _CopyOrMoveDestinationExistsError(src=src, dest=dest)
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
    if perms is not None:
        chmod(dest, perms)
    if (owner is not None) or (group is not None):
        chown(dest, user=owner, group=group)


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
            _ = copyfile(src, temp)
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


##


def has_env(key: str, /, *, case_sensitive: bool = False) -> bool:
    """Check if an environment variable is define."""
    try:
        _ = get_env(key, case_sensitive=case_sensitive)
    except GetEnvError:
        return False
    return True


##


def is_debug() -> bool:
    """Check if we are in `DEBUG` mode."""
    return has_env("DEBUG")


def is_pytest() -> bool:
    """Check if `pytest` is running."""
    return has_env("PYTEST_VERSION")


##


def move_many(
    *paths: Pair[PathLike],
    overwrite: bool = False,
    perms: PermissionsLike | None = None,
    owner: str | int | None = None,
    group: str | int | None = None,
) -> None:
    """Move a set of files concurrently."""
    with ExitStack() as stack:
        for src, dest in paths:
            temp = stack.enter_context(yield_write_path(dest, overwrite=overwrite))
            move(src, temp, overwrite=overwrite, perms=perms, owner=owner, group=group)


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
            raise FileOrDirMissingError(path=path)
        case False, False, False, False:
            return None
        case _:
            raise FileOrDirTypeError(path=path)


##


def first_non_directory_parent(path: PathLike, /) -> Path:
    """Get the first non-directory parent."""
    path = Path(path)
    for p in reversed(path.parents):
        if not p.is_dir():
            return p
    raise FirstNonDirectoryParentError(path=path)


##


def read_text_if_existing_file(path_or_text: PathLike, /) -> str:
    """Read a text file if it exists."""
    try:
        return read_text(path_or_text)
    except ReadTextFileNotFoundError:
        return str(path_or_text)
    except ReadTextIsADirectoryError as error:
        raise ReadTextIfExistingFileIsADirectoryError(path=error.path) from None
    except ReadTextNotADirectoryError as error:
        raise ReadTextIfExistingFileNotADirectoryError(
            path=error.path, parent=error.parent
        ) from None


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
#### permissions ##############################################################
###############################################################################


type PermissionsLike = Permissions | int | str


@dataclass(order=True, unsafe_hash=True, kw_only=True, slots=True)
class Permissions:
    """A set of file permissions."""

    user_read: bool = False
    user_write: bool = False
    user_execute: bool = False
    group_read: bool = False
    group_write: bool = False
    group_execute: bool = False
    others_read: bool = False
    others_write: bool = False
    others_execute: bool = False

    def __int__(self) -> int:
        flags: list[int] = [
            S_IRUSR if self.user_read else 0,
            S_IWUSR if self.user_write else 0,
            S_IXUSR if self.user_execute else 0,
            S_IRGRP if self.group_read else 0,
            S_IWGRP if self.group_write else 0,
            S_IXGRP if self.group_execute else 0,
            S_IROTH if self.others_read else 0,
            S_IWOTH if self.others_write else 0,
            S_IXOTH if self.others_execute else 0,
        ]
        return reduce(or_, flags)

    @override
    def __repr__(self) -> str:
        return ",".join([
            self._repr_parts(
                "u",
                read=self.user_read,
                write=self.user_write,
                execute=self.user_execute,
            ),
            self._repr_parts(
                "g",
                read=self.group_read,
                write=self.group_write,
                execute=self.group_execute,
            ),
            self._repr_parts(
                "o",
                read=self.others_read,
                write=self.others_write,
                execute=self.others_execute,
            ),
        ])

    def _repr_parts(
        self,
        prefix: Literal["u", "g", "o"],
        /,
        *,
        read: bool = False,
        write: bool = False,
        execute: bool = False,
    ) -> str:
        parts: list[str] = [
            "r" if read else "",
            "w" if write else "",
            "x" if execute else "",
        ]
        return f"{prefix}={''.join(parts)}"

    @override
    def __str__(self) -> str:
        return repr(self)

    @classmethod
    def new(cls, perms: PermissionsLike, /) -> Self:
        match perms:
            case Permissions():
                return cast("Self", perms)
            case int():
                return cls.from_int(perms)
            case str():
                return cls.from_text(perms)
            case never:
                assert_never(never)

    @classmethod
    def from_human_int(cls, n: int, /) -> Self:
        if not (0 <= n <= 777):
            raise PermissionsFromHumanIntRangeError(n=n)
        user_read, user_write, user_execute = cls._from_human_int(n, (n // 100) % 10)
        group_read, group_write, group_execute = cls._from_human_int(n, (n // 10) % 10)
        others_read, others_write, others_execute = cls._from_human_int(n, n % 10)
        return cls(
            user_read=user_read,
            user_write=user_write,
            user_execute=user_execute,
            group_read=group_read,
            group_write=group_write,
            group_execute=group_execute,
            others_read=others_read,
            others_write=others_write,
            others_execute=others_execute,
        )

    @classmethod
    def _from_human_int(cls, n: int, digit: int, /) -> Triple[bool]:
        if not (0 <= digit <= 7):
            raise PermissionsFromHumanIntDigitError(n=n, digit=digit)
        return bool(4 & digit), bool(2 & digit), bool(1 & digit)

    @classmethod
    def from_int(cls, n: int, /) -> Self:
        if 0o0 <= n <= 0o777:
            return cls(
                user_read=bool(n & S_IRUSR),
                user_write=bool(n & S_IWUSR),
                user_execute=bool(n & S_IXUSR),
                group_read=bool(n & S_IRGRP),
                group_write=bool(n & S_IWGRP),
                group_execute=bool(n & S_IXGRP),
                others_read=bool(n & S_IROTH),
                others_write=bool(n & S_IWOTH),
                others_execute=bool(n & S_IXOTH),
            )
        raise PermissionsFromIntError(n=n)

    @classmethod
    def from_path(cls, path: PathLike, /) -> Self:
        return cls.from_int(S_IMODE(Path(path).stat().st_mode))

    @classmethod
    def from_text(cls, text: str, /) -> Self:
        try:
            user, group, others = extract_groups(
                r"^u=(r?w?x?),g=(r?w?x?),o=(r?w?x?)$", text
            )
        except ExtractGroupsError:
            raise PermissionsFromTextError(text=text) from None
        user_read, user_write, user_execute = cls._from_text_part(user)
        group_read, group_write, group_execute = cls._from_text_part(group)
        others_read, others_write, others_execute = cls._from_text_part(others)
        return cls(
            user_read=user_read,
            user_write=user_write,
            user_execute=user_execute,
            group_read=group_read,
            group_write=group_write,
            group_execute=group_execute,
            others_read=others_read,
            others_write=others_write,
            others_execute=others_execute,
        )

    @classmethod
    def _from_text_part(cls, text: str, /) -> Triple[bool]:
        read, write, execute = extract_groups("^(r?)(w?)(x?)$", text)
        return read != "", write != "", execute != ""

    @property
    def human_int(self) -> int:
        return (
            100
            * self._human_int(
                read=self.user_read, write=self.user_write, execute=self.user_execute
            )
            + 10
            * self._human_int(
                read=self.group_read, write=self.group_write, execute=self.group_execute
            )
            + self._human_int(
                read=self.others_read,
                write=self.others_write,
                execute=self.others_execute,
            )
        )

    def _human_int(
        self, *, read: bool = False, write: bool = False, execute: bool = False
    ) -> int:
        return (4 if read else 0) + (2 if write else 0) + (1 if execute else 0)

    def replace(
        self,
        *,
        user_read: bool | Sentinel = sentinel,
        user_write: bool | Sentinel = sentinel,
        user_execute: bool | Sentinel = sentinel,
        group_read: bool | Sentinel = sentinel,
        group_write: bool | Sentinel = sentinel,
        group_execute: bool | Sentinel = sentinel,
        others_read: bool | Sentinel = sentinel,
        others_write: bool | Sentinel = sentinel,
        others_execute: bool | Sentinel = sentinel,
    ) -> Self:
        return replace_non_sentinel(
            self,
            user_read=user_read,
            user_write=user_write,
            user_execute=user_execute,
            group_read=group_read,
            group_write=group_write,
            group_execute=group_execute,
            others_read=others_read,
            others_write=others_write,
            others_execute=others_execute,
        )


###############################################################################
#### pwd ######################################################################
###############################################################################


get_uid_name = utilities.constants._get_uid_name  # noqa: SLF001


def get_file_owner(path: PathLike, /) -> str | None:
    """Get the owner of a file."""
    uid = Path(path).stat().st_uid
    return get_uid_name(uid)


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
            raise ExtractGroupNoCaptureGroupsError(pattern=pattern_use, text=text)
        case 1:
            matches: list[str] = pattern_use.findall(text)
            match len(matches):
                case 0:
                    raise ExtractGroupNoMatchesError(
                        pattern=pattern_use, text=text
                    ) from None
                case 1:
                    return matches[0]
                case _:
                    raise ExtractGroupMultipleMatchesError(
                        pattern=pattern_use, text=text, matches=matches
                    ) from None
        case _:
            raise ExtractGroupMultipleCaptureGroupsError(pattern=pattern_use, text=text)


##


def extract_groups(pattern: PatternLike, text: str, /, *, flags: int = 0) -> list[str]:
    """Extract multiple groups.

    The regex may have any number of capture groups, and they must collectively
    match exactly once.
    """
    pattern_use = _to_pattern(pattern, flags=flags)
    if (n_groups := pattern_use.groups) == 0:
        raise ExtractGroupsNoCaptureGroupsError(pattern=pattern_use, text=text)
    matches: list[str] = pattern_use.findall(text)
    match len(matches), n_groups:
        case 0, _:
            raise ExtractGroupsNoMatchesError(pattern=pattern_use, text=text)
        case 1, 1:
            return matches
        case 1, _:
            return list(matches[0])
        case _:
            raise ExtractGroupsMultipleMatchesError(
                pattern=pattern_use, text=text, matches=matches
            )


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
#### readers/writers ##########################################################
###############################################################################


def read_bytes(path: PathLike, /, *, decompress: bool = False) -> bytes:
    """Read data from a file."""
    path = Path(path)
    if decompress:
        try:
            with yield_gzip(path) as temp:
                return temp.read_bytes()
        except YieldGzipFileNotFoundError as error:
            raise ReadBytesFileNotFoundError(path=error.path) from None
        except YieldGzipIsADirectoryError as error:
            raise ReadBytesIsADirectoryError(path=error.path) from None
        except YieldGzipNotADirectoryError as error:
            raise ReadBytesNotADirectoryError(
                path=error.path, parent=first_non_directory_parent(error.path)
            ) from None
    else:
        try:
            return path.read_bytes()
        except FileNotFoundError:
            raise ReadBytesFileNotFoundError(path=path) from None
        except IsADirectoryError:
            raise ReadBytesIsADirectoryError(path=path) from None
        except NotADirectoryError:
            raise ReadBytesNotADirectoryError(
                path=path, parent=first_non_directory_parent(path)
            ) from None


def write_bytes(
    path: PathLike,
    data: bytes,
    /,
    *,
    compress: bool = False,
    overwrite: bool = False,
    perms: PermissionsLike | None = None,
    owner: str | int | None = None,
    group: str | int | None = None,
    json: bool = False,
) -> None:
    """Write data to a file."""
    try:
        with yield_write_path(
            path,
            compress=compress,
            overwrite=overwrite,
            perms=perms,
            owner=owner,
            group=group,
        ) as temp:
            if json:  # pragma: no cover
                with suppress(FileNotFoundError):
                    data = check_output(["prettier", "--parser=json"], input=data)
            _ = temp.write_bytes(data)
    except YieldWritePathError as error:
        raise WriteBytesError(path=error.path) from None


##


def read_pickle(path: PathLike, /) -> Any:
    """Read an object from disk."""
    path = Path(path)
    try:
        with gzip.open(path, mode="rb") as gz:
            return pickle.load(gz)  # noqa: S301
    except FileNotFoundError:
        raise ReadPickleFileNotFoundError(path=path) from None
    except IsADirectoryError:
        raise ReadPickleIsADirectoryError(path=path) from None
    except NotADirectoryError:
        raise ReadPickleNotADirectoryError(
            path=path, parent=first_non_directory_parent(path)
        ) from None


def write_pickle(path: PathLike, obj: Any, /, *, overwrite: bool = False) -> None:
    """Write an object to disk."""
    try:
        with (
            yield_write_path(path, overwrite=overwrite) as temp,
            gzip.open(temp, mode="wb") as gz,
        ):
            pickle.dump(obj, gz)
    except YieldWritePathError as error:
        raise WritePickleError(path=error.path) from None


##


def read_text(path: PathLike, /, *, decompress: bool = False) -> str:
    """Read text from a file."""
    path = Path(path)
    if decompress:
        try:
            with yield_gzip(path) as temp:
                return temp.read_text()
        except YieldGzipFileNotFoundError as error:
            raise ReadTextFileNotFoundError(path=error.path) from None
        except YieldGzipIsADirectoryError as error:
            raise ReadTextIsADirectoryError(path=error.path) from None
        except YieldGzipNotADirectoryError as error:
            raise ReadTextNotADirectoryError(
                path=error.path, parent=first_non_directory_parent(error.path)
            ) from None
    else:
        try:
            return path.read_text()
        except FileNotFoundError:
            raise ReadTextFileNotFoundError(path=path) from None
        except IsADirectoryError:
            raise ReadTextIsADirectoryError(path=path) from None
        except NotADirectoryError:
            raise ReadTextNotADirectoryError(
                path=path, parent=first_non_directory_parent(path)
            ) from None


def write_text(
    path: PathLike,
    text: str,
    /,
    *,
    compress: bool = False,
    overwrite: bool = False,
    perms: PermissionsLike | None = None,
    owner: str | int | None = None,
    group: str | int | None = None,
) -> None:
    """Write text to a file."""
    try:
        with yield_write_path(
            path,
            compress=compress,
            overwrite=overwrite,
            perms=perms,
            owner=owner,
            group=group,
        ) as temp:
            _ = temp.write_text(normalize_str(text))
    except YieldWritePathError as error:
        raise WriteTextError(path=error.path) from None


###############################################################################
#### rich #####################################################################
###############################################################################


@overload
def repr_mapping(
    mapping: StrMapping,
    /,
    *,
    header: SequenceStr | None = None,
    show_edge: bool = RICH_SHOW_EDGE,
    show_lines: bool = RICH_SHOW_LINES,
    max_width: int = RICH_MAX_WIDTH,
    indent_size: int = RICH_INDENT_SIZE,
    max_length: int | None = RICH_MAX_LENGTH,
    max_string: int | None = RICH_MAX_STRING,
    max_depth: int | None = RICH_MAX_DEPTH,
    expand_all: bool = RICH_EXPAND_ALL,
    table: Literal[True],
) -> Table: ...
@overload
def repr_mapping(
    mapping: StrMapping,
    /,
    *,
    header: SequenceStr | None = None,
    show_edge: bool = RICH_SHOW_EDGE,
    show_lines: bool = RICH_SHOW_LINES,
    max_width: int = RICH_MAX_WIDTH,
    indent_size: int = RICH_INDENT_SIZE,
    max_length: int | None = RICH_MAX_LENGTH,
    max_string: int | None = RICH_MAX_STRING,
    max_depth: int | None = RICH_MAX_DEPTH,
    expand_all: bool = RICH_EXPAND_ALL,
    table: Literal[False] = False,
) -> str: ...
@overload
def repr_mapping(
    mapping: StrMapping,
    /,
    *,
    header: SequenceStr | None = None,
    show_edge: bool = RICH_SHOW_EDGE,
    show_lines: bool = RICH_SHOW_LINES,
    max_width: int = RICH_MAX_WIDTH,
    indent_size: int = RICH_INDENT_SIZE,
    max_length: int | None = RICH_MAX_LENGTH,
    max_string: int | None = RICH_MAX_STRING,
    max_depth: int | None = RICH_MAX_DEPTH,
    expand_all: bool = RICH_EXPAND_ALL,
    table: bool = False,
) -> TableLike: ...
def repr_mapping(
    mapping: StrMapping,
    /,
    *,
    header: SequenceStr | None = None,
    show_edge: bool = RICH_SHOW_EDGE,
    show_lines: bool = RICH_SHOW_LINES,
    max_width: int = RICH_MAX_WIDTH,
    indent_size: int = RICH_INDENT_SIZE,
    max_length: int | None = RICH_MAX_LENGTH,
    max_string: int | None = RICH_MAX_STRING,
    max_depth: int | None = RICH_MAX_DEPTH,
    expand_all: bool = RICH_EXPAND_ALL,
    table: bool = False,
) -> TableLike:
    """Get the representation of a mapping as a table."""
    return repr_table(
        *mapping.items(),
        header=header,
        show_edge=show_edge,
        show_lines=show_lines,
        max_width=max_width,
        indent_size=indent_size,
        max_length=max_length,
        max_string=max_string,
        max_depth=max_depth,
        expand_all=expand_all,
        table=table,
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
    return pretty_repr(
        str(obj),
        max_width=max_width,
        indent_size=indent_size,
        max_length=max_length,
        max_string=max_string,
        max_depth=max_depth,
        expand_all=expand_all,
    )


##


@overload
def repr_table(
    *items: tuple[Any, ...],
    header: SequenceStr | None = None,
    show_edge: bool = RICH_SHOW_EDGE,
    show_lines: bool = RICH_SHOW_LINES,
    max_width: int = RICH_MAX_WIDTH,
    indent_size: int = RICH_INDENT_SIZE,
    max_length: int | None = RICH_MAX_LENGTH,
    max_string: int | None = RICH_MAX_STRING,
    max_depth: int | None = RICH_MAX_DEPTH,
    expand_all: bool = RICH_EXPAND_ALL,
    table: Literal[True],
) -> Table: ...
@overload
def repr_table(
    *items: tuple[Any, ...],
    header: SequenceStr | None = None,
    show_edge: bool = RICH_SHOW_EDGE,
    show_lines: bool = RICH_SHOW_LINES,
    max_width: int = RICH_MAX_WIDTH,
    indent_size: int = RICH_INDENT_SIZE,
    max_length: int | None = RICH_MAX_LENGTH,
    max_string: int | None = RICH_MAX_STRING,
    max_depth: int | None = RICH_MAX_DEPTH,
    expand_all: bool = RICH_EXPAND_ALL,
    table: Literal[False] = False,
) -> str: ...
@overload
def repr_table(
    *items: tuple[Any, ...],
    header: SequenceStr | None = None,
    show_edge: bool = RICH_SHOW_EDGE,
    show_lines: bool = RICH_SHOW_LINES,
    max_width: int = RICH_MAX_WIDTH,
    indent_size: int = RICH_INDENT_SIZE,
    max_length: int | None = RICH_MAX_LENGTH,
    max_string: int | None = RICH_MAX_STRING,
    max_depth: int | None = RICH_MAX_DEPTH,
    expand_all: bool = RICH_EXPAND_ALL,
    table: bool = False,
) -> TableLike: ...
def repr_table(
    *items: tuple[Any, ...],
    header: SequenceStr | None = None,
    show_edge: bool = RICH_SHOW_EDGE,
    show_lines: bool = RICH_SHOW_LINES,
    max_width: int = RICH_MAX_WIDTH,
    indent_size: int = RICH_INDENT_SIZE,
    max_length: int | None = RICH_MAX_LENGTH,
    max_string: int | None = RICH_MAX_STRING,
    max_depth: int | None = RICH_MAX_DEPTH,
    expand_all: bool = RICH_EXPAND_ALL,
    table: bool = False,
) -> TableLike:
    """Get the representation of a table."""
    header_use = [] if header is None else header
    tab = Table(
        *header_use,
        show_header=header is not None,
        show_edge=show_edge,
        show_lines=show_lines,
    )
    try:
        n = one(sorted({len(i) for i in items}))
    except OneEmptyError:
        n = None
    except OneNonUniqueError as error:
        raise ReprTableItemsError(
            items=list(items), first=error.first, second=error.second
        ) from None
    if (header is not None) and (n is not None) and (len(header) != n):
        raise ReprTableHeaderError(
            header=header, header_len=len(header), item_len=n
        ) from None
    for row in items:
        row_strs = _repr_table_row(
            row,
            max_width=max_width,
            indent_size=indent_size,
            max_length=max_length,
            max_string=max_string,
            max_depth=max_depth,
            expand_all=expand_all,
        )
        tab.add_row(*row_strs)
    if table:
        return tab
    console = Console(width=max_width, record=True)
    with console.capture() as capture:
        console.print(tab)
    return capture.get()


def _repr_table_row(
    row: Iterable[Any],
    /,
    *,
    max_width: int = RICH_MAX_WIDTH,
    indent_size: int = RICH_INDENT_SIZE,
    max_length: int | None = RICH_MAX_LENGTH,
    max_string: int | None = RICH_MAX_STRING,
    max_depth: int | None = RICH_MAX_DEPTH,
    expand_all: bool = RICH_EXPAND_ALL,
) -> list[Any]:
    return [
        i
        if isinstance(i, (Table, str))
        else pretty_repr(
            i,
            max_width=max_width,
            indent_size=indent_size,
            max_length=max_length,
            max_string=max_string,
            max_depth=max_depth,
            expand_all=expand_all,
        )
        for i in row
    ]


###############################################################################
#### shutil ###################################################################
###############################################################################


def chown(
    path: PathLike,
    /,
    *,
    recursive: bool = False,
    user: str | int | None = None,
    group: str | int | None = None,
) -> None:
    """Change file owner and/or group."""
    path = Path(path)
    paths = list(path.rglob("**/*")) if recursive else [path]
    for p in paths:
        match user, group:
            case None, None:
                ...
            case str() | int(), None:
                shutil.chown(p, user, group)
            case None, str() | int():
                shutil.chown(p, user, group)
            case str() | int(), str() | int():
                shutil.chown(p, user, group)
            case never:
                assert_never(never)


##


def which(cmd: str, /) -> Path:
    """Get the path of a command."""
    path = shutil.which(cmd)
    if path is None:
        raise WhichError(cmd=cmd)
    return Path(path)


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
    stripped1 = text.strip("\n")
    dedented = dedent(stripped1)
    stripped2 = "\n".join(line.rstrip(" ") for line in dedented.splitlines())
    return stripped2 + "\n"


def normalize_str(text: str, /) -> str:
    """Normalize a string."""
    return text.strip(" \n") + "\n"


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
    text = read_text_if_existing_file(path_or_text)
    template = Template(text)
    mapping_use: StrMapping = {} if mapping is None else mapping
    kwargs_use: StrDict = (os.environ if environ else {}) | kwargs
    if safe:
        return template.safe_substitute(mapping_use, **kwargs_use)
    try:
        return template.substitute(mapping_use, **kwargs_use)
    except KeyError as error:
        raise SubstituteError(key=error.args[0]) from None


##


def unique_str() -> str:
    """Generate a unique string."""
    now = time_ns()
    pid = getpid()
    ident = get_ident()
    key = str(uuid4()).replace("-", "")
    return f"{now}_{pid}_{ident}_{key}"


###############################################################################
#### textwrap #################################################################
###############################################################################


def indent_non_head(text: str, prefix: str, /) -> str:
    """Indent all non-head lines."""
    if text == "":
        return ""
    first, *rest = text.splitlines(keepends=True)
    indented = indent("".join(rest), prefix)
    return f"{first}{indented}"


###############################################################################
#### time #####################################################################
###############################################################################


def sync_sleep(duration: Duration | None = None, /) -> None:
    """Sleep which accepts durations."""
    match duration:
        case int() | float():
            time.sleep(duration)
        case DateDelta() | TimeDelta() | DateTimeDelta():
            time.sleep(num_nanoseconds(duration) / NANOSECONDS_PER_SECOND)
        case None:
            ...
        case never:
            assert_never(never)


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


def delta_components(delta: Delta, /) -> _DeltaComponentsOutput:
    """Decompose a delta into its components."""
    match delta:
        case DateDelta():
            years, months, days = delta.in_years_months_days()
            return _DeltaComponentsOutput(years=years, months=months, days=days)
        case TimeDelta():
            hours, minutes, seconds, nanoseconds = delta.in_hrs_mins_secs_nanos()
            return _DeltaComponentsOutput(
                hours=hours, minutes=minutes, seconds=seconds, nanoseconds=nanoseconds
            )
        case DateTimeDelta():
            months, days, seconds, nanoseconds = delta.in_months_days_secs_nanos()
            return _DeltaComponentsOutput(
                months=months, days=days, seconds=seconds, nanoseconds=nanoseconds
            )
        case never:
            assert_never(never)


@dataclass(order=True, unsafe_hash=True, kw_only=True, slots=True)
class _DeltaComponentsOutput:
    years: int = 0
    months: int = 0
    weeks: int = 0
    days: int = 0
    hours: int = 0
    minutes: int = 0
    seconds: int = 0
    milliseconds: int = 0
    microseconds: int = 0
    nanoseconds: int = 0

    def __post_init__(self) -> None:
        while not self._normalize():
            ...
        if (
            ((self.years > 0) and (self.days < 0))
            or ((self.months > 0) and (self.days < 0))
            or ((self.years < 0) and (self.days > 0))
            or ((self.months < 0) and (self.days > 0))
        ):
            raise DeltaComponentsError(
                years=self.years, months=self.months, days=self.days
            )

    def __mul__(self, n: int, /) -> Self:
        return self.replace(
            years=n * self.years,
            months=n * self.months,
            weeks=n * self.weeks,
            days=n * self.days,
            hours=n * self.hours,
            minutes=n * self.minutes,
            seconds=n * self.seconds,
            milliseconds=n * self.milliseconds,
            microseconds=n * self.microseconds,
            nanoseconds=n * self.nanoseconds,
        )

    def __rmul__(self, n: int, /) -> Self:
        return self.__mul__(n)

    def replace(
        self,
        *,
        years: int | Sentinel = sentinel,
        months: int | Sentinel = sentinel,
        weeks: int | Sentinel = sentinel,
        days: int | Sentinel = sentinel,
        hours: int | Sentinel = sentinel,
        minutes: int | Sentinel = sentinel,
        seconds: int | Sentinel = sentinel,
        milliseconds: int | Sentinel = sentinel,
        microseconds: int | Sentinel = sentinel,
        nanoseconds: int | Sentinel = sentinel,
    ) -> Self:
        return replace_non_sentinel(
            self,
            years=years,
            months=months,
            weeks=weeks,
            days=days,
            hours=hours,
            minutes=minutes,
            seconds=seconds,
            milliseconds=milliseconds,
            microseconds=microseconds,
            nanoseconds=nanoseconds,
        )

    def _normalize(self) -> bool:
        return (
            self._normalize_months_to_years()
            and self._normalize_days_to_weeks()
            and self._normalize_hours_to_days()
            and self._normalize_minutes_to_hours()
            and self._normalize_seconds_to_minutes()
            and self._normalize_milliseconds_to_seconds()
            and self._normalize_microseconds_to_millseconds()
            and self._normalize_nanoseconds_to_millseconds()
        )

    def _normalize_months_to_years(self) -> bool:
        factor = MONTHS_PER_YEAR
        if (self.years >= 0) and (self.months >= factor):
            years, self.months = divmod(self.months, factor)
            self.years += years
            return False
        if (self.years <= 0) and (self.months <= -factor):
            years, self.months = divmod(self.months, -factor)
            self.years -= years
            return False
        return True

    def _normalize_days_to_weeks(self) -> bool:
        factor = DAYS_PER_WEEK
        if (self.weeks >= 0) and (self.days >= factor):
            weeks, self.days = divmod(self.days, factor)
            self.weeks += weeks
            return False
        if (self.weeks <= 0) and (self.days <= -factor):
            weeks, self.days = divmod(self.days, -factor)
            self.weeks -= weeks
            return False
        return True

    def _normalize_hours_to_days(self) -> bool:
        factor = HOURS_PER_DAY
        if (self.days >= 0) and (self.hours >= factor):
            days, self.hours = divmod(self.hours, factor)
            self.days += days
            return False
        if (self.days <= 0) and (self.hours <= -factor):
            days, self.hours = divmod(self.hours, -factor)
            self.days -= days
            return False
        return True

    def _normalize_minutes_to_hours(self) -> bool:
        factor = MINUTES_PER_HOUR
        if (self.hours >= 0) and (self.minutes >= factor):
            hours, self.minutes = divmod(self.minutes, factor)
            self.hours += hours
            return False
        if (self.hours <= 0) and (self.minutes <= -factor):
            hours, self.minutes = divmod(self.minutes, -factor)
            self.hours -= hours
            return False
        return True

    def _normalize_seconds_to_minutes(self) -> bool:
        factor = SECONDS_PER_MINUTE
        if (self.minutes >= 0) and (self.seconds >= factor):
            minutes, self.seconds = divmod(self.seconds, factor)
            self.minutes += minutes
            return False
        if (self.minutes <= 0) and (self.seconds <= -factor):
            minutes, self.seconds = divmod(self.seconds, -factor)
            self.minutes -= minutes
            return False
        return True

    def _normalize_milliseconds_to_seconds(self) -> bool:
        factor = MILLISECONDS_PER_SECOND
        if (self.seconds >= 0) and (self.milliseconds >= factor):
            seconds, self.milliseconds = divmod(self.milliseconds, factor)
            self.seconds += seconds
            return False
        if (self.seconds <= 0) and (self.milliseconds <= -factor):
            seconds, self.milliseconds = divmod(self.milliseconds, -factor)
            self.seconds -= seconds
            return False
        return True

    def _normalize_microseconds_to_millseconds(self) -> bool:
        factor = MICROSECONDS_PER_MILLISECOND
        if (self.milliseconds >= 0) and (self.microseconds >= factor):
            milliseconds, self.microseconds = divmod(self.microseconds, factor)
            self.milliseconds += milliseconds
            return False
        if (self.milliseconds <= 0) and (self.microseconds <= -factor):
            milliseconds, self.microseconds = divmod(self.microseconds, -factor)
            self.milliseconds -= milliseconds
            return False
        return True

    def _normalize_nanoseconds_to_millseconds(self) -> bool:
        factor = NANOSECONDS_PER_MICROSECOND
        if (self.microseconds >= 0) and (self.nanoseconds >= factor):
            microseconds, self.nanoseconds = divmod(self.nanoseconds, factor)
            self.microseconds += microseconds
            return False
        if (self.microseconds <= 0) and (self.nanoseconds <= -factor):
            microseconds, self.nanoseconds = divmod(self.nanoseconds, -factor)
            self.microseconds -= microseconds
            return False
        return True


##


def duration_to_milliseconds(duration: Duration, /) -> Number:
    """Convert a duration to a number of milliseconds."""
    match duration:
        case int() | float():
            return duration
        case DateDelta() | TimeDelta() | DateTimeDelta():
            return num_nanoseconds(duration) / NANOSECONDS_PER_MILLISECOND
        case never:
            assert_never(never)


def duration_to_seconds(duration: Duration, /) -> Number:
    """Convert a duration to a number of seconds."""
    match duration:
        case int() | float():
            return duration
        case DateDelta() | TimeDelta() | DateTimeDelta():
            return num_nanoseconds(duration) / NANOSECONDS_PER_SECOND
        case never:
            assert_never(never)


##


get_now_local = utilities.constants._get_now_local  # noqa: SLF001


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


def num_years(delta: Delta, /) -> int:
    """Compute the number of years in a delta."""
    components = delta_components(delta)
    if (
        (components.months != 0)
        or (components.weeks != 0)
        or (components.days != 0)
        or (components.hours != 0)
        or (components.minutes != 0)
        or (components.seconds != 0)
        or (components.milliseconds != 0)
        or (components.microseconds != 0)
        or (components.nanoseconds != 0)
    ):
        raise NumYearsError(
            months=components.months,
            weeks=components.weeks,
            days=components.days,
            hours=components.hours,
            minutes=components.minutes,
            seconds=components.seconds,
            milliseconds=components.milliseconds,
            microseconds=components.microseconds,
            nanoseconds=components.nanoseconds,
        )
    return components.years


def num_months(delta: Delta, /) -> int:
    """Compute the number of months in a delta."""
    components = delta_components(delta)
    if (
        (components.weeks != 0)
        or (components.days != 0)
        or (components.hours != 0)
        or (components.minutes != 0)
        or (components.seconds != 0)
        or (components.milliseconds != 0)
        or (components.microseconds != 0)
        or (components.nanoseconds != 0)
    ):
        raise NumMonthsError(
            weeks=components.weeks,
            days=components.days,
            hours=components.hours,
            minutes=components.minutes,
            seconds=components.seconds,
            milliseconds=components.milliseconds,
            microseconds=components.microseconds,
            nanoseconds=components.nanoseconds,
        )
    return MONTHS_PER_YEAR * components.years + components.months


def num_weeks(delta: Delta, /) -> int:
    """Compute the number of weeks in a delta."""
    components = delta_components(delta)
    if (
        (components.years != 0)
        or (components.months != 0)
        or (components.days != 0)
        or (components.hours != 0)
        or (components.minutes != 0)
        or (components.seconds != 0)
        or (components.milliseconds != 0)
        or (components.microseconds != 0)
        or (components.nanoseconds != 0)
    ):
        raise NumWeeksError(
            years=components.years,
            months=components.months,
            days=components.days,
            hours=components.hours,
            minutes=components.minutes,
            seconds=components.seconds,
            milliseconds=components.milliseconds,
            microseconds=components.microseconds,
            nanoseconds=components.nanoseconds,
        )
    return components.weeks


def num_days(delta: Delta, /) -> int:
    """Compute the number of days in a delta."""
    components = delta_components(delta)
    if (
        (components.years != 0)
        or (components.months != 0)
        or (components.hours != 0)
        or (components.minutes != 0)
        or (components.seconds != 0)
        or (components.milliseconds != 0)
        or (components.microseconds != 0)
        or (components.nanoseconds != 0)
    ):
        raise NumDaysError(
            years=components.years,
            months=components.months,
            hours=components.hours,
            minutes=components.minutes,
            seconds=components.seconds,
            milliseconds=components.milliseconds,
            microseconds=components.microseconds,
            nanoseconds=components.nanoseconds,
        )
    return DAYS_PER_WEEK * components.weeks + components.days


def num_hours(delta: Delta, /) -> int:
    """Compute the number of hours in a delta."""
    components = delta_components(delta)
    if (
        (components.years != 0)
        or (components.months != 0)
        or (components.minutes != 0)
        or (components.seconds != 0)
        or (components.milliseconds != 0)
        or (components.microseconds != 0)
        or (components.nanoseconds != 0)
    ):
        raise NumHoursError(
            years=components.years,
            months=components.months,
            minutes=components.minutes,
            seconds=components.seconds,
            milliseconds=components.milliseconds,
            microseconds=components.microseconds,
            nanoseconds=components.nanoseconds,
        )
    return (
        HOURS_PER_WEEK * components.weeks
        + HOURS_PER_DAY * components.days
        + components.hours
    )


def num_minutes(delta: Delta, /) -> int:
    """Compute the number of minutes in a delta."""
    components = delta_components(delta)
    if (
        (components.years != 0)
        or (components.months != 0)
        or (components.seconds != 0)
        or (components.milliseconds != 0)
        or (components.microseconds != 0)
        or (components.nanoseconds != 0)
    ):
        raise NumMinutesError(
            years=components.years,
            months=components.months,
            seconds=components.seconds,
            milliseconds=components.milliseconds,
            microseconds=components.microseconds,
            nanoseconds=components.nanoseconds,
        )
    return (
        MINUTES_PER_WEEK * components.weeks
        + MINUTES_PER_DAY * components.days
        + MINUTES_PER_HOUR * components.hours
        + components.minutes
    )


def num_seconds(delta: Delta, /) -> int:
    """Compute the number of seconds in a delta."""
    components = delta_components(delta)
    if (
        (components.years != 0)
        or (components.months != 0)
        or (components.milliseconds != 0)
        or (components.microseconds != 0)
        or (components.nanoseconds != 0)
    ):
        raise NumSecondsError(
            years=components.years,
            months=components.months,
            milliseconds=components.milliseconds,
            microseconds=components.microseconds,
            nanoseconds=components.nanoseconds,
        )
    return (
        SECONDS_PER_WEEK * components.weeks
        + SECONDS_PER_DAY * components.days
        + SECONDS_PER_HOUR * components.hours
        + SECONDS_PER_MINUTE * components.minutes
        + components.seconds
    )


def num_milliseconds(delta: Delta, /) -> int:
    """Compute the number of milliseconds in a delta."""
    components = delta_components(delta)
    if (
        (components.years != 0)
        or (components.months != 0)
        or (components.microseconds != 0)
        or (components.nanoseconds != 0)
    ):
        raise NumMilliSecondsError(
            years=components.years,
            months=components.months,
            microseconds=components.microseconds,
            nanoseconds=components.nanoseconds,
        )
    return (
        MILLISECONDS_PER_WEEK * components.weeks
        + MILLISECONDS_PER_DAY * components.days
        + MILLISECONDS_PER_HOUR * components.hours
        + MILLISECONDS_PER_MINUTE * components.minutes
        + MILLISECONDS_PER_SECOND * components.seconds
        + components.milliseconds
    )


def num_microseconds(delta: Delta, /) -> int:
    """Compute the number of microseconds in a delta."""
    components = delta_components(delta)
    if (
        (components.years != 0)
        or (components.months != 0)
        or (components.nanoseconds != 0)
    ):
        raise NumMicroSecondsError(
            years=components.years,
            months=components.months,
            nanoseconds=components.nanoseconds,
        )
    return (
        MICROSECONDS_PER_WEEK * components.weeks
        + MICROSECONDS_PER_DAY * components.days
        + MICROSECONDS_PER_HOUR * components.hours
        + MICROSECONDS_PER_MINUTE * components.minutes
        + MICROSECONDS_PER_SECOND * components.seconds
        + MICROSECONDS_PER_MILLISECOND * components.milliseconds
        + components.microseconds
    )


def num_nanoseconds(delta: Delta, /) -> int:
    """Compute the number of nanoseconds in a delta."""
    components = delta_components(delta)
    if (components.years != 0) or (components.months != 0):
        raise NumNanoSecondsError(years=components.years, months=components.months)
    return (
        NANOSECONDS_PER_WEEK * components.weeks
        + NANOSECONDS_PER_DAY * components.days
        + NANOSECONDS_PER_HOUR * components.hours
        + NANOSECONDS_PER_MINUTE * components.minutes
        + NANOSECONDS_PER_SECOND * components.seconds
        + NANOSECONDS_PER_MILLISECOND * components.milliseconds
        + NANOSECONDS_PER_MICROSECOND * components.microseconds
        + components.nanoseconds
    )


##


@overload
def to_date(date: Sentinel, /, *, time_zone: TimeZoneLike = UTC) -> Sentinel: ...
@overload
def to_date(
    date: MaybeCallableDateLike | None | dt.date = get_today,
    /,
    *,
    time_zone: TimeZoneLike = UTC,
) -> Date: ...
def to_date(
    date: MaybeCallableDateLike | dt.date | None | Sentinel = get_today,
    /,
    *,
    time_zone: TimeZoneLike = UTC,
) -> Date | Sentinel:
    """Convert to a date."""
    match date:
        case Date() | Sentinel():
            return date
        case None:
            return get_today(time_zone)
        case str():
            return Date.parse_iso(date)
        case dt.date():
            return Date.from_py_date(date)
        case Callable() as func:
            return to_date(func(), time_zone=time_zone)
        case never:
            assert_never(never)


###############################################################################
#### writers ##################################################################
###############################################################################


@contextmanager
def yield_write_path(
    path: PathLike,
    /,
    *,
    compress: bool = False,
    overwrite: bool = False,
    perms: PermissionsLike | None = None,
    owner: str | int | None = None,
    group: str | int | None = None,
) -> Iterator[Path]:
    """Yield a temporary path for atomically writing files to disk."""
    with yield_adjacent_temp_file(path) as temp:
        yield temp
        if compress:
            try:
                compress_gzip(temp, path, overwrite=overwrite)
            except CompressGzipError as error:
                raise YieldWritePathError(path=error.dest) from None
        else:
            try:
                move(temp, path, overwrite=overwrite)
            except MoveDestinationExistsError as error:
                raise YieldWritePathError(path=error.dest) from None
    if perms is not None:
        chmod(path, perms)
    if (owner is not None) or (group is not None):
        chown(path, user=owner, group=group)


###############################################################################
#### zoneinfo #################################################################
###############################################################################


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
            raise ToTimeZoneNameInvalidKeyError(time_zone=time_zone)
        case dt.tzinfo() as tzinfo:
            if tzinfo is dt.UTC:
                return cast("TimeZone", UTC.key)
            raise ToTimeZoneNameInvalidTZInfoError(time_zone=obj)
        case dt.datetime() as date_time:
            if date_time.tzinfo is None:
                raise ToTimeZoneNamePlainDateTimeError(date_time=date_time)
            return to_time_zone_name(date_time.tzinfo)
        case never:
            assert_never(never)


##


def to_zone_info(obj: TimeZoneLike, /) -> ZoneInfo:
    """Convert an object to a time-zone."""
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
            raise ToZoneInfoInvalidTZInfoError(time_zone=obj)
        case dt.datetime() as date_time:
            if date_time.tzinfo is None:
                raise ToZoneInfoPlainDateTimeError(date_time=date_time)
            return to_zone_info(date_time.tzinfo)
        case never:
            assert_never(never)


__all__ = [
    "ENHANCED_LOG_RECORD_EXTRA_ATTRS",
    "CheckUniqueError",
    "CompressBZ2Error",
    "CompressGzipError",
    "CompressLZMAError",
    "CompressZipError",
    "CopyDestinationExistsError",
    "CopyError",
    "CopySourceNotFoundError",
    "EnhancedLogRecord",
    "ExtractGroupError",
    "ExtractGroupMultipleCaptureGroupsError",
    "ExtractGroupMultipleMatchesError",
    "ExtractGroupNoCaptureGroupsError",
    "ExtractGroupNoMatchesError",
    "ExtractGroupsError",
    "ExtractGroupsMultipleMatchesError",
    "ExtractGroupsNoCaptureGroupsError",
    "ExtractGroupsNoMatchesError",
    "FileOrDirError",
    "FileOrDirMissingError",
    "FileOrDirTypeError",
    "FirstNonDirectoryParentError",
    "GetEnvError",
    "GetLoggingLevelNameError",
    "GetLoggingLevelNumberError",
    "MaxNullableError",
    "MaybeColoredFormatterError",
    "MinNullableError",
    "MoveDestinationExistsError",
    "MoveError",
    "MoveSourceNotFoundError",
    "NumDaysError",
    "NumHoursError",
    "NumMicroSecondsError",
    "NumMilliSecondsError",
    "NumMinutesError",
    "NumMonthsError",
    "NumNanoSecondsError",
    "NumSecondsError",
    "NumWeeksError",
    "NumYearsError",
    "OneEmptyError",
    "OneError",
    "OneNonUniqueError",
    "OneStrEmptyError",
    "OneStrError",
    "OneStrNonUniqueError",
    "Permissions",
    "PermissionsError",
    "PermissionsFromHumanIntDigitError",
    "PermissionsFromHumanIntRangeError",
    "PermissionsFromIntError",
    "PermissionsFromIntError",
    "PermissionsFromTextError",
    "PermissionsLike",
    "ReadBytesError",
    "ReadBytesFileNotFoundError",
    "ReadBytesIsADirectoryError",
    "ReadBytesNotADirectoryError",
    "ReadPickleError",
    "ReadPickleFileNotFoundError",
    "ReadPickleIsADirectoryError",
    "ReadPickleNotADirectoryError",
    "ReadTextError",
    "ReadTextFileNotFoundError",
    "ReadTextIfExistingFileError",
    "ReadTextIfExistingFileIsADirectoryError",
    "ReadTextIfExistingFileNotADirectoryError",
    "ReadTextIsADirectoryError",
    "ReadTextNotADirectoryError",
    "ReprTableError",
    "ReprTableHeaderError",
    "ReprTableItemsError",
    "SubstituteError",
    "TemporaryDirectory",
    "TemporaryFile",
    "ToTimeZoneNameError",
    "ToTimeZoneNameInvalidKeyError",
    "ToTimeZoneNameInvalidTZInfoError",
    "ToTimeZoneNamePlainDateTimeError",
    "ToZoneInfoError",
    "ToZoneInfoInvalidTZInfoError",
    "ToZoneInfoPlainDateTimeError",
    "WhichError",
    "WriteBytesError",
    "WritePickleError",
    "WriteTextError",
    "YieldBZ2Error",
    "YieldBZ2FileNotFoundError",
    "YieldBZ2IsADirectoryError",
    "YieldBZ2NotADirectoryError",
    "YieldGzipError",
    "YieldGzipFileNotFoundError",
    "YieldGzipIsADirectoryError",
    "YieldGzipNotADirectoryError",
    "YieldLZMAError",
    "YieldLZMAFileNotFoundError",
    "YieldLZMAIsADirectoryError",
    "YieldLZMANotADirectoryError",
    "YieldUncompressedError",
    "YieldUncompressedFileNotFoundError",
    "YieldUncompressedIsADirectoryError",
    "YieldUncompressedNotADirectoryError",
    "YieldZipError",
    "YieldZipFileNotFoundError",
    "YieldZipIsADirectoryError",
    "YieldZipNotADirectoryError",
    "add_adapter",
    "add_filters",
    "always_iterable",
    "async_sleep",
    "check_unique",
    "chmod",
    "chown",
    "chunked",
    "compress_bz2",
    "compress_gzip",
    "compress_lzma",
    "compress_zip",
    "delta_components",
    "duration_to_milliseconds",
    "duration_to_seconds",
    "extract_group",
    "extract_groups",
    "file_or_dir",
    "first_non_directory_parent",
    "get_class",
    "get_class_name",
    "get_env",
    "get_file_group",
    "get_file_owner",
    "get_func_name",
    "get_gid_name",
    "get_logging_level_name",
    "get_logging_level_number",
    "get_now",
    "get_now_local",
    "get_now_local_plain",
    "get_now_plain",
    "get_time",
    "get_time_local",
    "get_today",
    "get_today_local",
    "get_uid_name",
    "has_env",
    "indent_non_head",
    "is_close",
    "is_debug",
    "is_none",
    "is_not_none",
    "is_pytest",
    "is_sentinel",
    "log_critical",
    "log_debug",
    "log_error",
    "log_exception",
    "log_info",
    "log_warning",
    "max_nullable",
    "min_nullable",
    "move_many",
    "normalize_multi_line_str",
    "normalize_str",
    "not_func",
    "num_days",
    "num_hours",
    "num_microseconds",
    "num_milliseconds",
    "num_minutes",
    "num_months",
    "num_nanoseconds",
    "num_seconds",
    "num_weeks",
    "num_years",
    "one",
    "one_str",
    "read_bytes",
    "read_pickle",
    "read_text",
    "replace_non_sentinel",
    "repr_error",
    "repr_str",
    "repr_table",
    "set_up_logging",
    "substitute",
    "suppress_super_attribute_error",
    "suppress_warnings",
    "sync_sleep",
    "take",
    "to_date",
    "to_logger",
    "to_time_zone_name",
    "to_zone_info",
    "transpose",
    "unique_everseen",
    "unique_str",
    "which",
    "write_bytes",
    "write_pickle",
    "write_text",
    "yield_adjacent_temp_dir",
    "yield_adjacent_temp_file",
    "yield_bz2",
    "yield_gzip",
    "yield_lzma",
    "yield_temp_cwd",
    "yield_temp_environ",
    "yield_warnings_as_errors",
    "yield_zip",
]
