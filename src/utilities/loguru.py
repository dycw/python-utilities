from __future__ import annotations

import asyncio
import logging
import sys
import time
from asyncio import AbstractEventLoop
from collections.abc import Callable, Hashable
from dataclasses import dataclass
from enum import StrEnum, unique
from functools import partial, wraps
from inspect import iscoroutinefunction
from logging import Handler, LogRecord
from sys import __excepthook__, _getframe
from typing import TYPE_CHECKING, Any, TextIO, TypedDict, TypeVar, cast, overload

from loguru import logger
from typing_extensions import override

from utilities.datetime import duration_to_timedelta
from utilities.functions import get_func_name
from utilities.inspect import bind_args_custom_repr
from utilities.iterables import resolve_include_and_exclude
from utilities.sys import is_pytest

if TYPE_CHECKING:
    import datetime as dt
    from multiprocessing.context import BaseContext
    from types import TracebackType

    from loguru import (
        CompressionFunction,
        ExcInfo,
        FilterDict,
        FilterFunction,
        FormatFunction,
        Logger,
        Message,
        Record,
        RetentionFunction,
        RotationFunction,
        Writable,
    )

    from utilities.asyncio import MaybeCoroutine1
    from utilities.iterables import MaybeIterable
    from utilities.types import Duration, PathLike, StrMapping


_F = TypeVar("_F", bound=Callable[..., Any])


class HandlerConfiguration(TypedDict, total=False):
    """A handler configuration."""

    sink: (
        TextIO
        | Writable
        | Callable[[Message], MaybeCoroutine1[None]]
        | Handler
        | PathLike
    )
    level: int | str
    format: str | FormatFunction
    filter: str | FilterFunction | FilterDict | None
    colorize: bool | None
    serialize: bool
    backtrace: bool
    diagnose: bool
    enqueue: bool
    context: str | BaseContext | None
    catch: bool
    loop: AbstractEventLoop
    rotation: str | int | dt.time | dt.timedelta | RotationFunction | None
    retention: str | int | dt.timedelta | RetentionFunction | None
    compression: str | CompressionFunction | None
    delay: bool
    watch: bool
    mode: str
    buffering: int
    encoding: str
    kwargs: StrMapping


class InterceptHandler(Handler):
    """Handler for intercepting standard logging messages.

    https://github.com/Delgan/loguru#entirely-compatible-with-standard-logging
    """

    @override
    def emit(self, record: LogRecord) -> None:
        # Get corresponding Loguru level if it exists.
        try:  # pragma: no cover
            level = logger.level(record.levelname).name
        except ValueError:  # pragma: no cover
            level = record.levelno

        # Find caller from where originated the logged message.
        frame, depth = _getframe(6), 6  # pragma: no cover
        while (  # pragma: no cover
            frame and frame.f_code.co_filename == logging.__file__
        ):
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(  # pragma: no cover
            level, record.getMessage()
        )


@unique
class LogLevel(StrEnum):
    """An enumeration of the logging levels."""

    TRACE = "TRACE"
    DEBUG = "DEBUG"
    INFO = "INFO"
    SUCCESS = "SUCCESS"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


def get_logging_level(level: str, /) -> int:
    """Get the logging level."""
    try:
        return logger.level(level).no
    except ValueError:
        raise GetLoggingLevelError(level=level) from None


@dataclass(kw_only=True)
class GetLoggingLevelError(Exception):
    level: str

    @override
    def __str__(self) -> str:
        return f"Invalid logging level: {self.level!r}"


@overload
def log_call(func: _F, /, *, level: LogLevel = ...) -> _F: ...
@overload
def log_call(func: None = None, /, *, level: LogLevel = ...) -> Callable[[_F], _F]: ...
def log_call(
    func: _F | None = None, /, *, level: LogLevel = LogLevel.TRACE
) -> _F | Callable[[_F], _F]:
    """Log the function call."""
    if func is None:
        return partial(log_call, level=level)

    if iscoroutinefunction(func):

        @wraps(func)
        async def wrapped_async(*args: Any, **kwargs: Any) -> Any:
            _log_call_bind_and_log(func, level, *args, **kwargs)
            return await func(*args, **kwargs)

        return cast(_F, wrapped_async)

    @wraps(func)
    def wrapped_sync(*args: Any, **kwargs: Any) -> Any:
        _log_call_bind_and_log(func, level, *args, **kwargs)
        return func(*args, **kwargs)

    return cast(_F, wrapped_sync)


def _log_call_bind_and_log(
    func: Callable[..., Any], level: LogLevel, /, *args: Any, **kwargs: Any
) -> None:
    func_name = get_func_name(func)
    key = f"<{func_name}>"
    bound_args = bind_args_custom_repr(func, *args, **kwargs)
    logger.opt(depth=2).log(level, "", **{key: bound_args})


def logged_sleep_sync(
    duration: Duration, /, *, level: LogLevel = LogLevel.INFO, depth: int = 1
) -> None:
    """Log a sleep operation, synchronously."""
    timedelta = duration_to_timedelta(duration)
    logger.opt(depth=depth).log(
        level, "Sleeping for {timedelta}...", timedelta=timedelta
    )
    time.sleep(timedelta.total_seconds())


async def logged_sleep_async(
    duration: Duration, /, *, level: LogLevel = LogLevel.INFO, depth: int = 1
) -> None:
    """Log a sleep operation, asynchronously."""
    timedelta = duration_to_timedelta(duration)
    logger.opt(depth=depth).log(
        level, "Sleeping for {timedelta}...", timedelta=timedelta
    )
    await asyncio.sleep(timedelta.total_seconds())


def make_catch_hook(**kwargs: Any) -> Callable[[BaseException], None]:
    """Make a `logger.catch` hook."""

    def callback(error: BaseException, /) -> None:
        logger.bind(**kwargs).opt(depth=3, exception=error, record=True).error(
            "Uncaught {record[exception].value!r}"
        )

    return callback


def make_except_hook(
    **kwargs: Any,
) -> Callable[[type[BaseException], BaseException, TracebackType | None], None]:
    """Make an `excepthook` which uses `loguru`."""
    callback = make_catch_hook(**kwargs)

    def except_hook(
        exc_type: type[BaseException],
        exc_value: BaseException,
        exc_traceback: TracebackType | None,
        /,
    ) -> None:
        """Exception hook which uses `loguru`."""
        if issubclass(exc_type, KeyboardInterrupt):  # pragma: no cover
            __excepthook__(exc_type, exc_value, exc_traceback)
            return
        callback(exc_value)  # pragma: no cover
        sys.exit(1)  # pragma: no cover

    return except_hook


def make_filter(
    *,
    level: LogLevel | None = None,
    min_level: LogLevel | None = None,
    max_level: LogLevel | None = None,
    name_include: MaybeIterable[str] | None = None,
    name_exclude: MaybeIterable[str] | None = None,
    extra_include_all: MaybeIterable[Hashable] | None = None,
    extra_include_any: MaybeIterable[Hashable] | None = None,
    extra_exclude_all: MaybeIterable[Hashable] | None = None,
    extra_exclude_any: MaybeIterable[Hashable] | None = None,
    _is_testing: bool = False,
) -> FilterFunction:
    """Make a filter."""
    either_is_testing = _is_testing or is_pytest()

    def filter_func(record: Record, /) -> bool:
        rec_level_no = record["level"].no
        if (level is not None) and (rec_level_no != get_logging_level(level)):
            return False
        if (min_level is not None) and (rec_level_no < get_logging_level(min_level)):
            return False
        if (max_level is not None) and (rec_level_no > get_logging_level(max_level)):
            return False
        name = record["name"]
        if name is not None:
            name_inc, name_exc = resolve_include_and_exclude(
                include=name_include, exclude=name_exclude
            )
            if (name_inc is not None) and not any(name.startswith(n) for n in name_inc):
                return False
            if (name_exc is not None) and any(name.startswith(n) for n in name_exc):
                return False
        rec_extra_keys = set(record["extra"])
        extra_inc_all, extra_exc_any = resolve_include_and_exclude(
            include=extra_include_all, exclude=extra_exclude_any
        )
        if (extra_inc_all is not None) and not extra_inc_all.issubset(rec_extra_keys):
            return False
        if (extra_exc_any is not None) and (len(rec_extra_keys & extra_exc_any) >= 1):
            return False
        extra_inc_any, extra_exc_all = resolve_include_and_exclude(
            include=extra_include_any, exclude=extra_exclude_all
        )
        if (extra_inc_any is not None) and (len(rec_extra_keys & extra_inc_any) == 0):
            return False
        if (extra_exc_all is not None) and extra_exc_all.issubset(rec_extra_keys):
            return False
        return either_is_testing

    return filter_func


def _log_from_depth_up(
    logger: Logger,
    depth: int,
    level: LogLevel,
    message: str,
    /,
    *args: Any,
    exception: bool | ExcInfo | BaseException | None = None,
    **kwargs: Any,
) -> None:
    """Log from a given depth up to 0, in case it would fail otherwise."""
    if depth >= 0:
        try:
            logger.opt(exception=exception, record=True, depth=depth).log(
                level, message, *args, **kwargs
            )
        except ValueError:  # pragma: no cover
            return _log_from_depth_up(
                logger, depth - 1, level, message, *args, exception=exception, **kwargs
            )
        return None
    raise _LogFromDepthUpError(depth=depth)


@dataclass(kw_only=True)
class _LogFromDepthUpError(Exception):
    depth: int

    @override
    def __str__(self) -> str:
        return f"Depth must be non-negative; got {self.depth}"


__all__ = [
    "GetLoggingLevelError",
    "HandlerConfiguration",
    "InterceptHandler",
    "LogLevel",
    "get_logging_level",
    "log_call",
    "logged_sleep_async",
    "logged_sleep_sync",
    "make_catch_hook",
    "make_except_hook",
    "make_filter",
]
