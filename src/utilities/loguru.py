from __future__ import annotations

import asyncio
import logging
import sys
import time
from asyncio import AbstractEventLoop
from dataclasses import dataclass
from enum import StrEnum, unique
from logging import Handler, LogRecord
from sys import __excepthook__, _getframe, stderr
from typing import TYPE_CHECKING, Any, TypedDict, overload

from loguru import logger
from typing_extensions import override

from utilities.datetime import duration_to_timedelta
from utilities.iterables import always_iterable
from utilities.reprlib import custom_mapping_repr
from utilities.sentinel import Sentinel, sentinel

if TYPE_CHECKING:
    import datetime as dt
    from collections.abc import Callable, Hashable
    from types import TracebackType

    from loguru import (
        FilterFunction,
        FormatFunction,
        Message,
        Record,
        RetentionFunction,
    )

    from utilities.asyncio import Coroutine1, MaybeCoroutine1
    from utilities.iterables import MaybeIterable
    from utilities.types import Duration


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


class HandlerConfiguration(TypedDict, total=False):
    """A handler configuration."""

    sink: Any
    level: int | str
    format: FormatFunction
    filter: FilterFunction
    colorize: bool
    serialize: bool
    backtrace: bool
    diagnose: bool
    enqueue: bool
    catch: bool
    loop: AbstractEventLoop
    rotation: int
    retention: str | int | dt.timedelta | RetentionFunction | None


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


catch_message = "Uncaught {record[exception].value!r}"


def make_except_hook(
    **kwargs: Any,
) -> Callable[[type[BaseException], BaseException, TracebackType | None], None]:
    """Make an `excepthook` which uses `loguru`."""

    def except_hook(
        exc_type: type[BaseException],
        exc_value: BaseException,
        exc_traceback: TracebackType | None,
        /,
    ) -> None:
        """Exception hook which uses `loguru`."""
        if issubclass(exc_type, KeyboardInterrupt):
            __excepthook__(exc_type, exc_value, exc_traceback)
            return
        logger.bind(**kwargs).opt(exception=exc_value, record=True).error(catch_message)
        sys.exit(1)

    return except_hook


def format_record(record: Record, /, *, exception: bool = True) -> str:
    """Format a record."""
    parts = [
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green>{time:.SSS zz/ddd}",
        "<cyan>{name}</cyan>.<cyan>{function}</cyan>:{line}",
        "<level>{level}</level>",
    ]
    if record["message"]:
        parts.append("<level>{message}</level>")
    try:
        cr = record["extra"]["custom_repr"]
    except KeyError:
        pass
    else:
        if cr:
            parts.append("{extra[custom_repr]}")
    fmt = " | ".join(parts) + "\n"
    if (record["exception"] is not None) and exception:
        fmt += "{exception}\n"
    return fmt


def format_record_json(record: Record, /) -> str:
    """Format a record for JSON."""
    parts = []
    if "json" in record["extra"]:
        parts.append("```{extra[json]}```")
    return " | ".join(parts) + "\n"


def format_record_slack(record: Record, /) -> str:
    """Format a record for Slack."""
    fmt = format_record(record, exception=False)
    return f"```{fmt}```"


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
        return f"There is no level {self.level!r}"


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


def make_filter(
    *,
    level: LogLevel | None = None,
    min_level: LogLevel | None = None,
    max_level: LogLevel | None = None,
    name_include: MaybeIterable[str] | Sentinel = sentinel,
    name_exclude: MaybeIterable[str] | Sentinel = sentinel,
    extra_include: MaybeIterable[Hashable] | Sentinel = sentinel,
    extra_exclude: MaybeIterable[Hashable] | Sentinel = sentinel,
) -> FilterFunction:
    """Make a filter."""

    def filter_func(record: Record, /) -> bool:
        rec_level_no = record["level"].no
        if not (
            ((level is None) or (rec_level_no == get_logging_level(level)))
            and ((min_level is None) or (rec_level_no >= get_logging_level(min_level)))
            and ((max_level is None) or (rec_level_no <= get_logging_level(max_level)))
        ):
            return False
        name = record["name"]
        if (name is not None) and not (
            (
                isinstance(name_include, Sentinel)
                or any(name.startswith(k) for k in always_iterable(name_include))
            )
            and (
                isinstance(name_exclude, Sentinel)
                or all(not name.startswith(k) for k in always_iterable(name_exclude))
            )
        ):
            return False
        rec_extra_keys = set(record["extra"])
        return (
            isinstance(extra_include, Sentinel)
            or any(k in rec_extra_keys for k in always_iterable(extra_include))
        ) and (
            isinstance(extra_exclude, Sentinel)
            or all(k not in rec_extra_keys for k in always_iterable(extra_exclude))
        )

    return filter_func


@overload
def make_slack_sink(
    url: str, /, *, loop: AbstractEventLoop
) -> Callable[..., Coroutine1[None]]: ...
@overload
def make_slack_sink(url: str, /, *, loop: None = ...) -> Callable[..., None]: ...
def make_slack_sink(
    url: str, /, *, loop: AbstractEventLoop | None = None
) -> Callable[..., MaybeCoroutine1[None]]:
    """Make a `slack` sink."""
    from utilities.slack_sdk import SendSlackError, send_slack_async, send_slack_sync

    if loop is None:

        def sink_sync(message: Message, /) -> None:
            try:
                send_slack_sync(message, url=url)
            except SendSlackError as error:
                _ = stderr.write(str(error))

        return sink_sync

    async def sink_async(message: Message, /) -> None:
        try:
            await send_slack_async(message, url=url)
        except SendSlackError as error:
            _ = stderr.write(str(error))

    return sink_async


def patch_record(record: Record, /) -> None:
    """Apply all patchers."""
    _patch_custom_repr(record)
    _patch_json(record)


def _patch_custom_repr(record: Record, /) -> None:
    """Add the `custom_repr` field to the extras."""
    mapping = {
        k: v for k, v in record["extra"].items() if k not in {"json", "custom_repr"}
    }
    record["extra"]["custom_repr"] = custom_mapping_repr(mapping)


def _patch_json(record: Record, /) -> None:
    """Add the `json` field to the extras."""
    record["extra"]["json"] = _serialize_record(record)


def _serialize_record(record: Record, /) -> str:
    """Serialize a record."""
    from orjson import dumps

    use = {}
    use["time"] = record["time"]
    use["name"] = record["name"]
    use["module"] = record["module"]
    use["function"] = record["function"]
    use["line"] = record["line"]
    use["message"] = record["message"]
    use |= record["extra"]
    if record["exception"] is not None:
        use["exception"] = {"type": str(record["exception"])}
    return dumps(use, default=str).decode()


__all__ = [
    "GetLoggingLevelError",
    "HandlerConfiguration",
    "InterceptHandler",
    "LogLevel",
    "catch_message",
    "format_record",
    "format_record_json",
    "format_record_slack",
    "get_logging_level",
    "logged_sleep_async",
    "logged_sleep_sync",
    "make_except_hook",
    "make_filter",
    "patch_record",
]
