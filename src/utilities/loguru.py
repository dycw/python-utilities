from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import StrEnum, unique
from logging import Handler, LogRecord
from sys import _getframe
from typing import TYPE_CHECKING, TextIO, TypedDict, cast

from loguru import logger
from typing_extensions import override

from utilities.iterables import OneEmptyError, OneNonUniqueError, one

if TYPE_CHECKING:
    import datetime as dt
    from asyncio import AbstractEventLoop
    from collections.abc import Callable, Sequence
    from multiprocessing.context import BaseContext

    from loguru import (
        CompressionFunction,
        FilterDict,
        FilterFunction,
        FormatFunction,
        LevelConfig,
        Message,
        RetentionFunction,
        RotationFunction,
        Writable,
    )

    from utilities.asyncio import MaybeCoroutine1
    from utilities.types import PathLike, StrMapping


_RECORD_EXCEPTION_VALUE = "{record[exception].value!r}"
LEVEL_CONFIGS: Sequence[LevelConfig] = [
    {"name": "TRACE", "color": "<blue><bold>"},
    {"name": "DEBUG", "color": "<cyan><bold>"},
    {"name": "INFO", "color": "<green><bold>"},
    {"name": "SUCCESS", "color": "<magenta><bold>"},
    {"name": "WARNING", "color": "<yellow><bold>"},
    {"name": "ERROR", "color": "<red><bold>"},
    {"name": "CRITICAL", "color": "<red><bold><blink>"},
]


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


def get_logging_level_name(level: int, /) -> str:
    """Get the logging level name."""
    core = logger._core  # noqa: SLF001 # pyright: ignore[reportAttributeAccessIssue]
    try:
        return one(k for k, v in core.levels.items() if v.no == level)
    except OneEmptyError:
        raise _GetLoggingLevelNameEmptyError(level=level) from None
    except OneNonUniqueError as error:
        error = cast(OneNonUniqueError[str], error)
        raise _GetLoggingLevelNameNonUniqueError(
            level=level, first=error.first, second=error.second
        ) from None


@dataclass(kw_only=True, slots=True)
class GetLoggingLevelNameError(Exception):
    level: int


@dataclass(kw_only=True, slots=True)
class _GetLoggingLevelNameEmptyError(GetLoggingLevelNameError):
    @override
    def __str__(self) -> str:
        return f"There is no level with severity {self.level}"


@dataclass(kw_only=True, slots=True)
class _GetLoggingLevelNameNonUniqueError(GetLoggingLevelNameError):
    first: str
    second: str

    @override
    def __str__(self) -> str:
        return f"There must be exactly one level with severity {self.level}; got {self.first!r}, {self.second!r} and perhaps more"


def get_logging_level_number(level: str, /) -> int:
    """Get the logging level number."""
    try:
        return logger.level(level).no
    except ValueError:
        raise GetLoggingLevelNumberError(level=level) from None


@dataclass(kw_only=True, slots=True)
class GetLoggingLevelNumberError(Exception):
    level: str

    @override
    def __str__(self) -> str:
        return f"Invalid logging level: {self.level!r}"


__all__ = [
    "LEVEL_CONFIGS",
    "GetLoggingLevelNameError",
    "GetLoggingLevelNumberError",
    "HandlerConfiguration",
    "InterceptHandler",
    "LogLevel",
    "get_logging_level_name",
    "get_logging_level_number",
]
