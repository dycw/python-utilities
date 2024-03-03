from __future__ import annotations

from enum import Enum, unique
from logging import basicConfig

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


@unique
class LogLevel(str, Enum):
    """An enumeration of the logging levels."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


__all__ = ["LogLevel", "basic_config"]
