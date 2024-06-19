from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, unique
from platform import system
from typing import TYPE_CHECKING

from typing_extensions import assert_never, override

if TYPE_CHECKING:
    from collections.abc import Iterator

    from utilities.types import IterableStrs


@unique
class System(str, Enum):
    """An enumeration of the systems."""

    windows = "windows"
    mac = "mac"
    linux = "linux"


def get_system() -> System:
    """Get the system/OS name."""
    sys = system()
    if sys == "Windows":  # pragma: os-ne-windows
        return System.windows
    if sys == "Darwin":  # pragma: os-ne-macos
        return System.mac
    if sys == "Linux":  # pragma: os-ne-linux
        return System.linux
    raise GetSystemError(sys=sys)  # pragma: no cover


@dataclass(kw_only=True)
class GetSystemError(Exception):
    sys: str

    @override
    def __str__(self) -> str:
        return (  # pragma: no cover
            f"System must be one of Windows, Darwin, Linux; got {self.sys!r} instead"
        )


SYSTEM = get_system()
IS_WINDOWS = SYSTEM is System.windows
IS_MAC = SYSTEM is System.mac
IS_LINUX = SYSTEM is System.linux
IS_NOT_WINDOWS = not IS_WINDOWS
IS_NOT_MAC = not IS_MAC
IS_NOT_LINUX = not IS_LINUX


def maybe_yield_lower_case(text: IterableStrs, /) -> Iterator[str]:
    """Yield lower-cased text if the platform is case-insentive."""
    match SYSTEM:
        case System.windows:  # pragma: os-ne-windows
            yield from (t.lower() for t in text)
        case System.mac:  # pragma: os-ne-macos
            yield from (t.lower() for t in text)
        case System.linux:  # pragma: os-ne-linux
            yield from text
        case _ as never:  # type: ignore[]
            assert_never(never)


__all__ = [
    "GetSystemError",
    "IS_LINUX",
    "IS_MAC",
    "IS_NOT_LINUX",
    "IS_NOT_MAC",
    "IS_NOT_WINDOWS",
    "IS_WINDOWS",
    "SYSTEM",
    "System",
    "get_system",
    "maybe_yield_lower_case",
]
