from __future__ import annotations

import asyncio
import logging
import sys
import time
from asyncio import AbstractEventLoop
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum, unique
from functools import partial, wraps
from inspect import iscoroutinefunction, signature
from logging import Handler, LogRecord
from sys import __excepthook__, _getframe
from typing import TYPE_CHECKING, Any, TextIO, TypedDict, TypeVar, cast, overload

from loguru import logger
from typing_extensions import override

from utilities.datetime import duration_to_timedelta

if TYPE_CHECKING:
    import datetime as dt
    from multiprocessing.context import BaseContext
    from types import TracebackType

    from loguru import (
        CompressionFunction,
        FilterDict,
        FilterFunction,
        FormatFunction,
        Message,
        RetentionFunction,
        RotationFunction,
        Writable,
    )

    from utilities.asyncio import MaybeCoroutine1
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

    sig = signature(func)

    if iscoroutinefunction(func):

        @wraps(func)
        async def wrapped_async(*args: Any, **kwargs: Any) -> Any:
            arguments = sig.bind(*args, **kwargs).arguments
            logger.opt(depth=1).log(level, "", **arguments)
            return await func(*args, **kwargs)

        return cast(_F, wrapped_async)

    @wraps(func)
    def wrapped_sync(*args: Any, **kwargs: Any) -> Any:
        arguments = sig.bind(*args, **kwargs).arguments
        logger.opt(depth=1).log(level, "", **arguments)
        return func(*args, **kwargs)

    return cast(_F, wrapped_sync)


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
]
