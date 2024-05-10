from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, unique
from logging import basicConfig

from typing_extensions import override

from utilities.datetime import maybe_sub_pct_y


def basic_config(
    *,
    format: str = "{asctime} | {name} | {levelname:8} | {message}",  # noqa: A002
) -> None:
    """Do the basic config."""
    basicConfig(
        format=format,
        datefmt=maybe_sub_pct_y("%Y-%m-%d %H:%M:%S"),
        style="{",
        level=LogLevel.DEBUG.name,
    )


def get_logging_level(level: str, /) -> int:
    """Get the logging level.

    Hard-coded mapping only needed for Python 3.10.
    """
    try:
        from logging import getLevelNamesMapping  # type: ignore[]
    except ImportError:  # pragma: version-ge-311
        mapping = {
            "CRITICAL": 50,
            "FATAL": 50,
            "ERROR": 40,
            "WARN": 30,
            "WARNING": 30,
            "INFO": 20,
            "DEBUG": 10,
            "NOTSET": 0,
            "VERBOSE": 19,
            "TRACE": 9,
        }
    else:  # pragma: no cover
        mapping = getLevelNamesMapping()
    try:
        return mapping[level]
    except KeyError:
        raise GetLoggingLevelError(level=level) from None


@dataclass(kw_only=True)
class GetLoggingLevelError(Exception):
    level: str

    @override
    def __str__(self) -> str:
        return f"Logging level {self.level!r} must be valid"


@unique
class LogLevel(str, Enum):
    """An enumeration of the logging levels."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


__all__ = ["GetLoggingLevelError", "LogLevel", "basic_config", "get_logging_level"]
