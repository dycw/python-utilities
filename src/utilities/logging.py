from enum import auto, unique
from logging import basicConfig

from beartype import beartype

from utilities.enum import StrEnum


@beartype
def basic_config() -> None:
    """Do the basic config."""
    basicConfig(
        format="{asctime} | {name} | {levelname:8} | {message}",
        datefmt="%4Y-%m-%d %H:%M:%S",
        style="{",
        level="DEBUG",
    )


@unique
class LogLevel(StrEnum):
    """An enumeration of the logging levels."""

    DEBUG = auto()
    INFO = auto()
    WARNING = auto()
    ERROR = auto()
    CRITICAL = auto()
