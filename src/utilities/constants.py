from __future__ import annotations

from getpass import getuser
from logging import getLogger
from os import cpu_count, environ
from pathlib import Path
from platform import system
from random import SystemRandom
from tempfile import gettempdir
from typing import TYPE_CHECKING, assert_never, cast
from zoneinfo import ZoneInfo

from tzlocal import get_localzone
from whenever import DateDelta, PlainDateTime, TimeDelta

if TYPE_CHECKING:
    from utilities.types import System, TimeZone


# getpass


USER = getuser()


# math


MIN_FLOAT32, MAX_FLOAT32 = -3.4028234663852886e38, 3.4028234663852886e38
MIN_FLOAT64, MAX_FLOAT64 = -1.7976931348623157e308, 1.7976931348623157e308
MIN_INT8, MAX_INT8 = -(2 ** (8 - 1)), 2 ** (8 - 1) - 1
MIN_INT16, MAX_INT16 = -(2 ** (16 - 1)), 2 ** (16 - 1) - 1
MIN_INT32, MAX_INT32 = -(2 ** (32 - 1)), 2 ** (32 - 1) - 1
MIN_INT64, MAX_INT64 = -(2 ** (64 - 1)), 2 ** (64 - 1) - 1
MIN_UINT8, MAX_UINT8 = 0, 2**8 - 1
MIN_UINT16, MAX_UINT16 = 0, 2**16 - 1
MIN_UINT32, MAX_UINT32 = 0, 2**32 - 1
MIN_UINT64, MAX_UINT64 = 0, 2**64 - 1


# os


IS_CI = "CI" in environ


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
IS_CI_AND_WINDOWS = IS_CI and IS_WINDOWS
IS_CI_AND_MAC = IS_CI and IS_MAC
IS_CI_AND_LINUX = IS_CI and IS_LINUX
IS_CI_AND_NOT_WINDOWS = IS_CI and IS_NOT_WINDOWS
IS_CI_AND_NOT_MAC = IS_CI and IS_NOT_MAC
IS_CI_AND_NOT_LINUX = IS_CI and IS_NOT_LINUX


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


# tempfile


TEMP_DIR = Path(gettempdir())


# tzlocal


def _get_local_time_zone() -> ZoneInfo:
    logger = getLogger("tzlocal")  # avoid import cycle
    init_disabled = logger.disabled
    logger.disabled = True
    time_zone = get_localzone()
    logger.disabled = init_disabled
    return time_zone


LOCAL_TIME_ZONE = _get_local_time_zone()
LOCAL_TIME_ZONE_NAME = cast("TimeZone", LOCAL_TIME_ZONE.key)


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


# zoneinfo


UTC = ZoneInfo("UTC")


# zoneinfo -> whenever


ZONED_DATE_TIME_MIN = PlainDateTime.MIN.assume_tz(UTC.key)
ZONED_DATE_TIME_MAX = PlainDateTime.MAX.assume_tz(UTC.key)


__all__ = [
    "CPU_COUNT",
    "DAY",
    "EFFECTIVE_GROUP_ID",
    "EFFECTIVE_GROUP_NAME",
    "EFFECTIVE_USER_ID",
    "EFFECTIVE_USER_NAME",
    "HOUR",
    "IS_CI",
    "IS_CI_AND_LINUX",
    "IS_CI_AND_MAC",
    "IS_CI_AND_NOT_LINUX",
    "IS_CI_AND_NOT_MAC",
    "IS_CI_AND_NOT_WINDOWS",
    "IS_CI_AND_WINDOWS",
    "IS_LINUX",
    "IS_MAC",
    "IS_NOT_LINUX",
    "IS_NOT_MAC",
    "IS_NOT_WINDOWS",
    "IS_WINDOWS",
    "LOCAL_TIME_ZONE",
    "LOCAL_TIME_ZONE_NAME",
    "MAX_FLOAT32",
    "MAX_FLOAT64",
    "MAX_INT8",
    "MAX_INT16",
    "MAX_INT32",
    "MAX_INT64",
    "MAX_PID",
    "MAX_UINT8",
    "MAX_UINT16",
    "MAX_UINT32",
    "MAX_UINT64",
    "MICROSECOND",
    "MILLISECOND",
    "MINUTE",
    "MIN_FLOAT32",
    "MIN_FLOAT64",
    "MIN_INT8",
    "MIN_INT16",
    "MIN_INT32",
    "MIN_INT64",
    "MIN_UINT8",
    "MIN_UINT16",
    "MIN_UINT32",
    "MIN_UINT64",
    "MONTH",
    "ROOT_GROUP_NAME",
    "ROOT_USER_NAME",
    "SECOND",
    "SYSTEM",
    "SYSTEM_RANDOM",
    "TEMP_DIR",
    "USER",
    "UTC",
    "WEEK",
    "YEAR",
    "ZERO_DAYS",
    "ZERO_TIME",
    "ZONED_DATE_TIME_MAX",
    "ZONED_DATE_TIME_MIN",
]
