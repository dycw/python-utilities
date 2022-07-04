from enum import auto
from enum import unique

from utilities.enum import StrEnum


@unique
class LogLevel(StrEnum):
    """An enumeration of the logging levels."""

    DEBUG = auto()
    INFO = auto()
    WARNING = auto()
    ERROR = auto()
    CRITICAL = auto()
