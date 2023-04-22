from collections.abc import Iterator
from enum import Enum, unique
from platform import system

from beartype import beartype

from utilities.beartype import IterableStrs
from utilities.typing import never


@unique
class System(str, Enum):
    """An enumeration of the systems."""

    windows = "windows"
    mac_os = "mac"
    linux = "linux"


@beartype
def _get_system() -> System:
    """Get the system/OS name."""
    if (sys := system()) == "Windows":  # pragma: os-ne-windows
        return System.windows
    if sys == "Darwin":  # pragma: os-ne-macos
        return System.mac_os
    if sys == "Linux":  # pragma: os-ne-linux
        return System.linux
    raise UnableToDetermineSystemError  # pragma: no cover


class UnableToDetermineSystemError(ValueError):
    """Raised when unable to determine the system."""


SYSTEM = _get_system()


@beartype
def maybe_yield_lower_case(text: IterableStrs, /) -> Iterator[str]:
    """Yield lower-cased text if the platform is case-insentive."""
    if SYSTEM is System.windows:  # noqa: SIM114 # pragma: os-ne-windows
        yield from (t.lower() for t in text)
    elif SYSTEM is System.mac_os:  # pragma: os-ne-macos
        yield from (t.lower() for t in text)
    elif SYSTEM is System.linux:  # pragma: os-ne-linux
        yield from text
    else:  # pragma: no cover
        return never(SYSTEM)
