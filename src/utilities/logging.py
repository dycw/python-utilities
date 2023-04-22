from enum import Enum, unique
from logging import basicConfig

from beartype import beartype

from utilities.platform.datetime import maybe_sub_pct_y


@beartype
def basic_config() -> None:
    """Do the basic config."""
    basicConfig(
        format="{asctime} | {name} | {levelname:8} | {message}",
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
