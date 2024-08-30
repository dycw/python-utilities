from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum, unique
from logging import basicConfig, getLevelNamesMapping

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
    """Get the logging level."""
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
        return f"Invalid logging level: {self.level!r}"


@unique
class LogLevel(StrEnum):
    """An enumeration of the logging levels."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


__all__ = ["GetLoggingLevelError", "LogLevel", "basic_config", "get_logging_level"]
