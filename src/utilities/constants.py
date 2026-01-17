from __future__ import annotations

from getpass import getuser
from os import cpu_count
from pathlib import Path
from platform import system
from random import SystemRandom
from typing import TYPE_CHECKING, assert_never

from whenever import DateDelta, TimeDelta

if TYPE_CHECKING:
    from utilities.types import System


# getpass


USER = getuser()


# os


def _get_cpu_count() -> int:
    """Get the CPU count."""
    count = cpu_count()
    if count is None:  # pragma: no cover
        raise ValueError(count)
    return count


CPU_COUNT = _get_cpu_count()


# platform


def _get_system() -> System:
    sys = system()
    if sys == "Windows":  # skipif-not-windows
        return "windows"
    if sys == "Darwin":  # skipif-not-macos
        return "mac"
    if sys == "Linux":  # skipif-not-linux
        return "linux"
    raise ValueError(sys)  # pragma: no cover


SYSTEM = _get_system()
IS_WINDOWS = SYSTEM == "windows"
IS_MAC = SYSTEM == "mac"
IS_LINUX = SYSTEM == "linux"
IS_NOT_WINDOWS = not IS_WINDOWS
IS_NOT_MAC = not IS_MAC
IS_NOT_LINUX = not IS_LINUX


def _get_max_pid() -> int | None:
    match SYSTEM:
        case "windows":  # skipif-not-windows
            return None
        case "mac":  # skipif-not-macos
            return 99999
        case "linux":  # skipif-not-linux
            path = Path("/proc/sys/kernel/pid_max")
            try:
                return int(path.read_text())
            except FileNotFoundError:  # pragma: no cover
                return None
        case never:
            assert_never(never)


MAX_PID = _get_max_pid()


# platform -> os


def _get_effective_group_id() -> int | None:
    """Get the effective group ID."""
    match SYSTEM:
        case "windows":  # skipif-not-windows
            return None
        case "mac" | "linux":  # skipif-windows
            from os import getegid

            return getegid()
        case never:
            assert_never(never)


def _get_effective_user_id() -> int | None:
    match SYSTEM:
        case "windows":  # skipif-not-windows
            return None
        case "mac" | "linux":  # skipif-windows
            from os import geteuid

            return geteuid()
        case never:
            assert_never(never)


EFFECTIVE_USER_ID = _get_effective_user_id()
EFFECTIVE_GROUP_ID = _get_effective_group_id()


# platform -> os -> grp


def _get_gid_name(gid: int, /) -> str | None:
    match SYSTEM:
        case "windows":  # skipif-not-windows
            return None
        case "mac" | "linux":
            from grp import getgrgid

            return getgrgid(gid).gr_name
        case never:
            assert_never(never)


ROOT_GROUP_NAME = _get_gid_name(0)
EFFECTIVE_GROUP_NAME = (
    None if EFFECTIVE_GROUP_ID is None else _get_gid_name(EFFECTIVE_GROUP_ID)
)


# platform -> os -> pwd


def _get_uid_name(uid: int, /) -> str | None:
    match SYSTEM:
        case "windows":  # skipif-not-windows
            return None
        case "mac" | "linux":  # skipif-windows
            from pwd import getpwuid

            return getpwuid(uid).pw_name
        case never:
            assert_never(never)


ROOT_USER_NAME = _get_uid_name(0)
EFFECTIVE_USER_NAME = (
    None if EFFECTIVE_USER_ID is None else _get_uid_name(EFFECTIVE_USER_ID)
)


# random


SYSTEM_RANDOM = SystemRandom()


# whenever


ZERO_DAYS = DateDelta()
ZERO_TIME = TimeDelta()
MICROSECOND = TimeDelta(microseconds=1)
MILLISECOND = TimeDelta(milliseconds=1)
SECOND = TimeDelta(seconds=1)
MINUTE = TimeDelta(minutes=1)
HOUR = TimeDelta(hours=1)
DAY = DateDelta(days=1)
WEEK = DateDelta(weeks=1)
MONTH = DateDelta(months=1)
YEAR = DateDelta(years=1)


__all__ = [
    "CPU_COUNT",
    "DAY",
    "EFFECTIVE_GROUP_ID",
    "EFFECTIVE_GROUP_NAME",
    "EFFECTIVE_USER_ID",
    "EFFECTIVE_USER_NAME",
    "HOUR",
    "IS_LINUX",
    "IS_MAC",
    "IS_NOT_LINUX",
    "IS_NOT_MAC",
    "IS_NOT_WINDOWS",
    "IS_WINDOWS",
    "MAX_PID",
    "MICROSECOND",
    "MILLISECOND",
    "MINUTE",
    "MONTH",
    "ROOT_GROUP_NAME",
    "ROOT_USER_NAME",
    "SECOND",
    "SYSTEM",
    "SYSTEM_RANDOM",
    "USER",
    "WEEK",
    "YEAR",
    "ZERO_DAYS",
    "ZERO_TIME",
]
