from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum, unique
from logging import Handler, LogRecord
from sys import _getframe
from typing import TYPE_CHECKING, Any, TypeVar, overload

from loguru import logger
from typing_extensions import override

from utilities.datetime import duration_to_timedelta

if TYPE_CHECKING:
    from utilities.types import Duration


_F = TypeVar("_F", bound=Callable[..., Any])


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


def contexualize_and_log() -> None:
    pass


@overload
def contexualize_and_log(
    func: _F, /, *, max_size: int = ..., typed: bool = ...
) -> _F: ...
@overload
def contexualize_and_log(
    func: None = None, /, *, max_size: int = ..., typed: bool = ...
) -> Callable[[_F], _F]: ...
def contexualize_and_log(
    func: _F | None = None, /, *, level: LogLevel
) -> _F | Callable[[_F], _F]:
    """Contextualize the logger and log upon entry."""


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


__all__ = [
    "GetLoggingLevelError",
    "InterceptHandler",
    "LogLevel",
    "get_logging_level",
    "logged_sleep_async",
    "logged_sleep_sync",
]
