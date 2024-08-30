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
from typing import TYPE_CHECKING, overload

from loguru import logger
from typing_extensions import override

from utilities.datetime import duration_to_timedelta
from utilities.reprlib import custom_mapping_repr

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType

    from loguru import Message, Record

    from utilities.asyncio import Coroutine1, MaybeCoroutine1
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


catch_message = "Uncaught {record[exception].value!r} ({record[process].name}/{record[process].id} | {record[thread].name}/{record[thread].id})"


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
    logger.opt(exception=exc_value, record=True).error(catch_message)
    sys.exit(1)


def format_record(record: Record, /) -> str:
    """Format a record."""
    parts = [
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green>{time:.SSS zz/ddd}",
        "<cyan>{name}</cyan>.<cyan>{function}</cyan>:{line}",
        "<level>{level}</level>",
    ]
    if record["message"]:
        parts.append("<level>{message}</level>")
    if "custom_repr" in record["extra"]:
        parts.append("{extra[custom_repr]}")
    fmt = " | ".join(parts) + "\n"
    if record["exception"] is not None:
        fmt += "{exception}\n"
    return fmt


def format_record_json(record: Record, /) -> str:
    """Format a record for JSON."""
    parts = []
    if "json" in record["extra"]:
        parts.append("{extra[json]}")
    return " | ".join(parts) + "\n"


def format_record_slack(record: Record, /) -> str:
    """Format a record for Slack."""
    fmt = format_record(record)
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

        def _send_slack_sync(message: Message, /) -> None:
            try:
                send_slack_sync(message, url=url)
            except SendSlackError as error:
                _ = stderr.write(str(error))

        return _send_slack_sync

    async def _send_slack_async(message: Message, /) -> None:
        try:
            await send_slack_async(message, url=url)
        except SendSlackError as error:
            _ = stderr.write(str(error))

    return _send_slack_async


def _patch_custom_repr(record: Record, /) -> None:
    """Add the `custom_repr` field to the extras."""
    mapping = {
        k: v for k, v in record["extra"].items() if k not in {"json", "custom_repr"}
    }
    record["extra"]["custom_repr"] = custom_mapping_repr(mapping)


def _patch_json(record: Record, /) -> None:
    """Add the `json` field to the extras."""
    record["extra"]["json"] = _serialize_record(record)


# def _patch_slack(record: Record, /) -> None:
#     """Add the `slack` field to the extras."""
#     msg = "```{record[message]}\n```"
#     record["extra"]["slack"] = msg


patched_logger = logger.patch(_patch_custom_repr).patch(
    _patch_json
)  # .patch(_patch_slack)


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
    "InterceptHandler",
    "LogLevel",
    "catch_message",
    "except_hook",
    "format_record",
    "format_record_json",
    "format_record_slack",
    "get_logging_level",
    "logged_sleep_async",
    "logged_sleep_sync",
    "patched_logger",
]
