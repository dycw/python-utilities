from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from platform import system
from re import sub
from typing import TYPE_CHECKING, Literal, assert_never, override

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

System = Literal["windows", "mac", "linux"]


def get_system() -> System:
    """Get the system/OS name."""
    sys = system()
    if sys == "Windows":  # skipif-not-windows
        return "windows"
    if sys == "Darwin":  # skipif-not-macos
        return "mac"
    if sys == "Linux":  # skipif-not-linux
        return "linux"
    raise GetSystemError(sys=sys)  # pragma: no cover


@dataclass(kw_only=True, slots=True)
class GetSystemError(Exception):
    sys: str

    @override
    def __str__(self) -> str:
        return (  # pragma: no cover
            f"System must be one of Windows, Darwin, Linux; got {self.sys!r} instead"
        )


SYSTEM = get_system()
IS_WINDOWS = SYSTEM == "windows"
IS_MAC = SYSTEM == "mac"
IS_LINUX = SYSTEM == "linux"
IS_NOT_WINDOWS = not IS_WINDOWS
IS_NOT_MAC = not IS_MAC
IS_NOT_LINUX = not IS_LINUX


##


def get_max_pid() -> int | None:
    """Get the maximum process ID."""
    match SYSTEM:
        case "windows":  # pragma: no cover
            return None
        case "mac":  # skipif-not-macos
            return 99999
        case "linux":  # skipif-not-linux
            path = Path("/proc/sys/kernel/pid_max")
            try:
                return int(path.read_text())
            except FileNotFoundError:  # pragma: no cover
                return None
        case _ as never:
            assert_never(never)


MAX_PID = get_max_pid()


##


def get_strftime(text: str, /) -> str:
    """Get a platform-specific format string."""
    match SYSTEM:
        case "windows":  # skipif-not-windows
            return text
        case "mac":  # skipif-not-macos
            return text
        case "linux":  # skipif-not-linux
            return sub("%Y", "%4Y", text)
        case _ as never:
            assert_never(never)


##


def maybe_yield_lower_case(text: Iterable[str], /) -> Iterator[str]:
    """Yield lower-cased text if the platform is case-insentive."""
    match SYSTEM:
        case "windows":  # skipif-not-windows
            yield from (t.lower() for t in text)
        case "mac":  # skipif-not-macos
            yield from (t.lower() for t in text)
        case "linux":  # skipif-not-linux
            yield from text
        case _ as never:
            assert_never(never)


__all__ = [
    "IS_LINUX",
    "IS_MAC",
    "IS_NOT_LINUX",
    "IS_NOT_MAC",
    "IS_NOT_WINDOWS",
    "IS_WINDOWS",
    "MAX_PID",
    "SYSTEM",
    "GetSystemError",
    "System",
    "get_max_pid",
    "get_strftime",
    "get_system",
    "maybe_yield_lower_case",
]
