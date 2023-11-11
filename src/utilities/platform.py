from __future__ import annotations

from collections.abc import Iterator
from enum import Enum, unique
from platform import system

from utilities.typing import IterableStrs, never


@unique
class System(str, Enum):
    """An enumeration of the systems."""

    windows = "windows"
    mac = "mac"
    linux = "linux"


def get_system() -> System:
    """Get the system/OS name."""
    if (sys := system()) == "Windows":  # pragma: os-ne-windows
        return System.windows
    if sys == "Darwin":  # pragma: os-ne-macos
        return System.mac
    if sys == "Linux":  # pragma: os-ne-linux
        return System.linux
    raise UnableToDetermineSystemError  # pragma: no cover


class UnableToDetermineSystemError(ValueError):
    """Raised when unable to determine the system."""


class UnsupportedSystemError(RuntimeWarning):
    """Raised when the system is unsupported."""


SYSTEM = get_system()
IS_WINDOWS = SYSTEM is System.windows
IS_MAC = SYSTEM is System.mac
IS_LINUX = SYSTEM is System.linux
IS_NOT_WINDOWS = not IS_WINDOWS
IS_NOT_MAC = not IS_MAC
IS_NOT_LINUX = not IS_LINUX


def maybe_yield_lower_case(text: IterableStrs, /) -> Iterator[str]:
    """Yield lower-cased text if the platform is case-insentive."""
    if SYSTEM is System.windows:  # noqa: SIM114 # pragma: os-ne-windows
        yield from (t.lower() for t in text)
    elif SYSTEM is System.mac:  # pragma: os-ne-macos
        yield from (t.lower() for t in text)
    elif SYSTEM is System.linux:  # pragma: os-ne-linux
        yield from text
    else:  # pragma: no cover
        return never(SYSTEM)


__all__ = [
    "get_system",
    "IS_LINUX",
    "IS_MAC",
    "IS_NOT_LINUX",
    "IS_NOT_MAC",
    "IS_NOT_WINDOWS",
    "IS_WINDOWS",
    "maybe_yield_lower_case",
    "System",
    "SYSTEM",
    "UnableToDetermineSystemError",
    "UnsupportedSystemError",
]
