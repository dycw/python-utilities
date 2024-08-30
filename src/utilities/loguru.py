from __future__ import annotations

import asyncio
import logging
import sys
import time
from enum import StrEnum, unique
from logging import Handler, LogRecord
from sys import __excepthook__, _getframe
from typing import TYPE_CHECKING, Any

from loguru import logger
from typing_extensions import override

from utilities.datetime import duration_to_timedelta

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType

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
        logger.bind(**kwargs).opt(exception=error, record=True).error(
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
        if issubclass(exc_type, KeyboardInterrupt):
            __excepthook__(exc_type, exc_value, exc_traceback)
            return
        callback(exc_value)
        sys.exit(1)

    return except_hook


__all__ = [
    "InterceptHandler",
    "logged_sleep_async",
    "logged_sleep_sync",
    "make_catch_hook",
    "make_except_hook",
]
