from __future__ import annotations

import re
from dataclasses import dataclass
from getpass import getuser
from logging import Logger, getLogger
from os import cpu_count, environ
from pathlib import Path
from platform import system
from random import SystemRandom
from re import IGNORECASE
from socket import gethostname
from tempfile import gettempdir
from typing import TYPE_CHECKING, Any, assert_never, cast, override
from zoneinfo import ZoneInfo

from tzlocal import get_localzone
from whenever import (
    Date,
    DateDelta,
    DateTimeDelta,
    PlainDateTime,
    Time,
    TimeDelta,
    ZonedDateTime,
)

from utilities.types import FieldStyleDict, FieldStyles, TimeZone

if TYPE_CHECKING:
    from utilities.types import System, TimeZone


###############################################################################
#### coloredlogs ##############################################################
###############################################################################


COLOREDLOGS_FIELD_STYLES = FieldStyles(
    asctime=FieldStyleDict(color="green"),
    hostname=FieldStyleDict(color="magenta"),
    levelname=FieldStyleDict(color="black", bold=True),
    name=FieldStyleDict(color="blue"),
    programname=FieldStyleDict(color="cyan"),
    username=FieldStyleDict(color="yellow"),
)
CUSTOM_FIELD_STYLES = {
    "asctime": COLOREDLOGS_FIELD_STYLES["asctime"],
    "hostname": COLOREDLOGS_FIELD_STYLES["name"],  # changed
    "levelname": COLOREDLOGS_FIELD_STYLES["levelname"],
    "name": COLOREDLOGS_FIELD_STYLES["name"],
    "programname": COLOREDLOGS_FIELD_STYLES["programname"],
    "username": COLOREDLOGS_FIELD_STYLES["username"],
    # custom
    "date": COLOREDLOGS_FIELD_STYLES["asctime"],
    "date_basic": COLOREDLOGS_FIELD_STYLES["asctime"],
    "time": COLOREDLOGS_FIELD_STYLES["asctime"],
    "time_basic": COLOREDLOGS_FIELD_STYLES["asctime"],
    "millis": FieldStyleDict(color="white"),
    "time_zone": FieldStyleDict(color="white"),
    "funcName": COLOREDLOGS_FIELD_STYLES["name"],
    "lineno": FieldStyleDict(color="white"),
    "process": COLOREDLOGS_FIELD_STYLES["hostname"],  # changed
}


###############################################################################
#### getpass ##################################################################
###############################################################################


USER: str = getuser()


###############################################################################
#### logging ##################################################################
###############################################################################


BACKUP_COUNT: int = 100
MAX_BYTES: int = 10 * 1024**2
ROOT_LOGGER: Logger = getLogger()


###############################################################################
#### math #####################################################################
###############################################################################


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


REL_TOL: float = 1e-09
ABS_TOL: float = 0.0


###############################################################################
#### os #######################################################################
###############################################################################


IS_CI: bool = "CI" in environ


def _get_cpu_count() -> int:
    """Get the CPU count."""
    count = cpu_count()
    if count is None:  # pragma: no cover
        raise ValueError(count)
    return count


CPU_COUNT: int = _get_cpu_count()


###############################################################################
#### platform #################################################################
###############################################################################


def _get_system() -> System:
    """Get the system/OS name."""
    sys = system()
    if sys == "Windows":  # skipif-not-windows
        return "windows"
    if sys == "Darwin":  # skipif-not-macos
        return "mac"
    if sys == "Linux":  # skipif-not-linux
        return "linux"
    raise ValueError(sys)  # pragma: no cover


SYSTEM: System = _get_system()
IS_WINDOWS: bool = SYSTEM == "windows"
IS_MAC: bool = SYSTEM == "mac"
IS_LINUX: bool = SYSTEM == "linux"
IS_NOT_WINDOWS: bool = not IS_WINDOWS
IS_NOT_MAC: bool = not IS_MAC
IS_NOT_LINUX: bool = not IS_LINUX
IS_CI_AND_WINDOWS: bool = IS_CI and IS_WINDOWS
IS_CI_AND_MAC: bool = IS_CI and IS_MAC
IS_CI_AND_LINUX: bool = IS_CI and IS_LINUX
IS_CI_AND_NOT_WINDOWS: bool = IS_CI and IS_NOT_WINDOWS
IS_CI_AND_NOT_MAC: bool = IS_CI and IS_NOT_MAC
IS_CI_AND_NOT_LINUX: bool = IS_CI and IS_NOT_LINUX


def _get_max_pid() -> int | None:
    """Get the system max process ID."""
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


MAX_PID: int | None = _get_max_pid()


###############################################################################
#### pathlib ##################################################################
###############################################################################


HOME: Path = Path.home()
PWD: Path = Path.cwd()


###############################################################################
#### platform -> os ###########################################################
###############################################################################


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


EFFECTIVE_GROUP_ID: int | None = _get_effective_group_id()


def _get_effective_user_id() -> int | None:
    """Get the effective user ID."""
    match SYSTEM:
        case "windows":  # skipif-not-windows
            return None
        case "mac" | "linux":  # skipif-windows
            from os import geteuid

            return geteuid()
        case never:
            assert_never(never)


EFFECTIVE_USER_ID: int | None = _get_effective_user_id()


###############################################################################
#### platform -> os -> grp ####################################################
###############################################################################


def _get_gid_name(gid: int, /) -> str | None:
    """Get the name of a group ID."""
    match SYSTEM:
        case "windows":  # skipif-not-windows
            return None
        case "mac" | "linux":
            from grp import getgrgid

            return getgrgid(gid).gr_name
        case never:
            assert_never(never)


ROOT_GROUP_NAME: str | None = _get_gid_name(0)
EFFECTIVE_GROUP_NAME: str | None = (
    None if EFFECTIVE_GROUP_ID is None else _get_gid_name(EFFECTIVE_GROUP_ID)
)


###############################################################################
#### platform -> os -> pwd ####################################################
###############################################################################


def _get_uid_name(uid: int, /) -> str | None:
    """Get the name of a user ID."""
    match SYSTEM:
        case "windows":  # skipif-not-windows
            return None
        case "mac" | "linux":  # skipif-windows
            from pwd import getpwuid

            return getpwuid(uid).pw_name
        case never:
            assert_never(never)


ROOT_USER_NAME: str | None = _get_uid_name(0)
EFFECTIVE_USER_NAME: str | None = (
    None if EFFECTIVE_USER_ID is None else _get_uid_name(EFFECTIVE_USER_ID)
)


###############################################################################
#### random ###################################################################
###############################################################################


SYSTEM_RANDOM: SystemRandom = SystemRandom()


###############################################################################
#### rich #####################################################################
###############################################################################


RICH_SHOW_EDGE: bool = True
RICH_SHOW_LINES: bool = False
RICH_MAX_WIDTH: int = 80
RICH_INDENT_SIZE: int = 4
RICH_MAX_LENGTH: int | None = 20
RICH_MAX_STRING: int | None = None
RICH_MAX_DEPTH: int | None = None
RICH_EXPAND_ALL: bool = False


###############################################################################
#### sentinel #################################################################
###############################################################################


class _SentinelMeta(type):
    """Metaclass for the sentinel."""

    instance: Any = None

    @override
    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        if cls.instance is None:
            cls.instance = super().__call__(*args, **kwargs)
        return cls.instance


class Sentinel(metaclass=_SentinelMeta):
    """Base class for the sentinel object."""

    @override
    def __repr__(self) -> str:
        return _SENTINEL_REPR

    @override
    def __str__(self) -> str:
        return repr(self)

    @classmethod
    def parse(cls, text: str, /) -> Sentinel:
        """Parse a string into the Sentinel value."""
        if _SENTINEL_PATTERN.search(text):
            return sentinel
        raise SentinelParseError(text=text)


_SENTINEL_PATTERN = re.compile("^(|sentinel|<sentinel>)$", flags=IGNORECASE)
_SENTINEL_REPR = "<sentinel>"


@dataclass(kw_only=True, slots=True)
class SentinelParseError(Exception):
    text: str

    @override
    def __str__(self) -> str:
        return f"Unable to parse sentinel; got {self.text!r}"


sentinel = Sentinel()


###############################################################################
#### socket ###################################################################
###############################################################################


HOSTNAME = gethostname()


###############################################################################
#### tempfile #################################################################
###############################################################################


TEMP_DIR: Path = Path(gettempdir())


###############################################################################
#### text #####################################################################
###############################################################################


LIST_SEPARATOR: str = ","
PAIR_SEPARATOR: str = "="
BRACKETS: set[tuple[str, str]] = {("(", ")"), ("[", "]"), ("{", "}")}


###############################################################################
#### tzlocal ##################################################################
###############################################################################


def _get_local_time_zone() -> ZoneInfo:
    """Get the local time zone, with the logging disabled."""
    logger = getLogger("tzlocal")  # avoid import cycle
    init_disabled = logger.disabled
    logger.disabled = True
    time_zone = get_localzone()
    logger.disabled = init_disabled
    return time_zone


LOCAL_TIME_ZONE: ZoneInfo = _get_local_time_zone()
LOCAL_TIME_ZONE_NAME: TimeZone = cast("TimeZone", LOCAL_TIME_ZONE.key)


###############################################################################
#### tzlocal -> whenever ######################################################
###############################################################################


def _get_now_local() -> ZonedDateTime:
    """Get the current zoned date-time in the local time-zone."""
    return ZonedDateTime.now(LOCAL_TIME_ZONE_NAME)


NOW_LOCAL: ZonedDateTime = _get_now_local()
TODAY_LOCAL: Date = NOW_LOCAL.date()
TIME_LOCAL: Time = NOW_LOCAL.time()
NOW_LOCAL_PLAIN: PlainDateTime = NOW_LOCAL.to_plain()


###############################################################################
#### whenever #################################################################
###############################################################################


ZERO_DAYS: DateDelta = DateDelta()
ZERO_TIME: TimeDelta = TimeDelta()
NANOSECOND: TimeDelta = TimeDelta(nanoseconds=1)
MICROSECOND: TimeDelta = TimeDelta(microseconds=1)
MILLISECOND: TimeDelta = TimeDelta(milliseconds=1)
SECOND: TimeDelta = TimeDelta(seconds=1)
MINUTE: TimeDelta = TimeDelta(minutes=1)
HOUR: TimeDelta = TimeDelta(hours=1)
DAY: DateDelta = DateDelta(days=1)
WEEK: DateDelta = DateDelta(weeks=1)
MONTH: DateDelta = DateDelta(months=1)
YEAR: DateDelta = DateDelta(years=1)


DATE_DELTA_MIN: DateDelta = DateDelta(weeks=-521722, days=-5)
DATE_DELTA_MAX: DateDelta = DateDelta(weeks=521722, days=5)
TIME_DELTA_MIN: TimeDelta = TimeDelta(hours=-87831216)
TIME_DELTA_MAX: TimeDelta = TimeDelta(hours=87831216)
DATE_TIME_DELTA_MIN: DateTimeDelta = DateTimeDelta(
    weeks=-521722,
    days=-5,
    hours=-23,
    minutes=-59,
    seconds=-59,
    milliseconds=-999,
    microseconds=-999,
    nanoseconds=-999,
)
DATE_TIME_DELTA_MAX: DateTimeDelta = DateTimeDelta(
    weeks=521722,
    days=5,
    hours=23,
    minutes=59,
    seconds=59,
    milliseconds=999,
    microseconds=999,
    nanoseconds=999,
)


MONTHS_PER_YEAR: int = 12
DAYS_PER_WEEK: int = 7
HOURS_PER_DAY: int = 24
HOURS_PER_WEEK: int = HOURS_PER_DAY * DAYS_PER_WEEK
MINUTES_PER_HOUR: int = 60
MINUTES_PER_DAY: int = MINUTES_PER_HOUR * HOURS_PER_DAY
MINUTES_PER_WEEK: int = MINUTES_PER_HOUR * HOURS_PER_WEEK
SECONDS_PER_MINUTE: int = 60
SECONDS_PER_HOUR: int = SECONDS_PER_MINUTE * MINUTES_PER_HOUR
SECONDS_PER_DAY: int = SECONDS_PER_MINUTE * MINUTES_PER_DAY
SECONDS_PER_WEEK: int = SECONDS_PER_MINUTE * MINUTES_PER_WEEK
MILLISECONDS_PER_SECOND: int = 1000
MILLISECONDS_PER_MINUTE: int = MILLISECONDS_PER_SECOND * SECONDS_PER_MINUTE
MILLISECONDS_PER_HOUR: int = MILLISECONDS_PER_SECOND * SECONDS_PER_HOUR
MILLISECONDS_PER_DAY: int = MILLISECONDS_PER_SECOND * SECONDS_PER_DAY
MILLISECONDS_PER_WEEK: int = MILLISECONDS_PER_SECOND * SECONDS_PER_WEEK
MICROSECONDS_PER_MILLISECOND: int = 1000
MICROSECONDS_PER_SECOND: int = MICROSECONDS_PER_MILLISECOND * MILLISECONDS_PER_SECOND
MICROSECONDS_PER_MINUTE: int = MICROSECONDS_PER_MILLISECOND * MILLISECONDS_PER_MINUTE
MICROSECONDS_PER_HOUR: int = MICROSECONDS_PER_MILLISECOND * MILLISECONDS_PER_HOUR
MICROSECONDS_PER_DAY: int = MICROSECONDS_PER_MILLISECOND * MILLISECONDS_PER_DAY
MICROSECONDS_PER_WEEK: int = MICROSECONDS_PER_MILLISECOND * MILLISECONDS_PER_WEEK
NANOSECONDS_PER_MICROSECOND: int = 1000
NANOSECONDS_PER_MILLISECOND: int = (
    NANOSECONDS_PER_MICROSECOND * MICROSECONDS_PER_MILLISECOND
)
NANOSECONDS_PER_SECOND: int = NANOSECONDS_PER_MICROSECOND * MICROSECONDS_PER_SECOND
NANOSECONDS_PER_MINUTE: int = NANOSECONDS_PER_MICROSECOND * MICROSECONDS_PER_MINUTE
NANOSECONDS_PER_HOUR: int = NANOSECONDS_PER_MICROSECOND * MICROSECONDS_PER_HOUR
NANOSECONDS_PER_DAY: int = NANOSECONDS_PER_MICROSECOND * MICROSECONDS_PER_DAY
NANOSECONDS_PER_WEEK: int = NANOSECONDS_PER_MICROSECOND * MICROSECONDS_PER_WEEK


###############################################################################
#### zoneinfo #################################################################
###############################################################################


UTC: ZoneInfo = ZoneInfo("UTC")
HongKong: ZoneInfo = ZoneInfo("Asia/Hong_Kong")
Tokyo: ZoneInfo = ZoneInfo("Asia/Tokyo")
USCentral: ZoneInfo = ZoneInfo("US/Central")
USEastern: ZoneInfo = ZoneInfo("US/Eastern")


###############################################################################
#### zoneinfo -> whenever #####################################################
###############################################################################


ZONED_DATE_TIME_MIN: ZonedDateTime = PlainDateTime.MIN.assume_tz(UTC.key)
ZONED_DATE_TIME_MAX: ZonedDateTime = PlainDateTime.MAX.assume_tz(UTC.key)


def _get_now(time_zone: str = UTC.key, /) -> ZonedDateTime:
    """Get the current zoned date-time."""
    return ZonedDateTime.now(time_zone)


NOW_UTC: ZonedDateTime = _get_now()
TODAY_UTC: Date = NOW_UTC.date()
TIME_UTC: Time = NOW_UTC.time()
NOW_UTC_PLAIN: PlainDateTime = NOW_UTC.to_plain()


__all__ = [
    "ABS_TOL",
    "BACKUP_COUNT",
    "BRACKETS",
    "COLOREDLOGS_FIELD_STYLES",
    "CPU_COUNT",
    "CUSTOM_FIELD_STYLES",
    "DATE_DELTA_MAX",
    "DATE_DELTA_MIN",
    "DATE_TIME_DELTA_MAX",
    "DATE_TIME_DELTA_MIN",
    "DAY",
    "DAYS_PER_WEEK",
    "EFFECTIVE_GROUP_ID",
    "EFFECTIVE_GROUP_NAME",
    "EFFECTIVE_USER_ID",
    "EFFECTIVE_USER_NAME",
    "HOME",
    "HOSTNAME",
    "HOUR",
    "HOURS_PER_DAY",
    "HOURS_PER_WEEK",
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
    "LIST_SEPARATOR",
    "LOCAL_TIME_ZONE",
    "LOCAL_TIME_ZONE_NAME",
    "MAX_BYTES",
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
    "MICROSECONDS_PER_DAY",
    "MICROSECONDS_PER_HOUR",
    "MICROSECONDS_PER_MILLISECOND",
    "MICROSECONDS_PER_MINUTE",
    "MICROSECONDS_PER_SECOND",
    "MICROSECONDS_PER_WEEK",
    "MILLISECOND",
    "MILLISECONDS_PER_DAY",
    "MILLISECONDS_PER_HOUR",
    "MILLISECONDS_PER_MINUTE",
    "MILLISECONDS_PER_SECOND",
    "MILLISECONDS_PER_WEEK",
    "MINUTE",
    "MINUTES_PER_DAY",
    "MINUTES_PER_HOUR",
    "MINUTES_PER_WEEK",
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
    "MONTHS_PER_YEAR",
    "NANOSECOND",
    "NANOSECONDS_PER_DAY",
    "NANOSECONDS_PER_HOUR",
    "NANOSECONDS_PER_MICROSECOND",
    "NANOSECONDS_PER_MILLISECOND",
    "NANOSECONDS_PER_MINUTE",
    "NANOSECONDS_PER_SECOND",
    "NANOSECONDS_PER_WEEK",
    "NOW_LOCAL",
    "NOW_LOCAL_PLAIN",
    "NOW_UTC",
    "NOW_UTC_PLAIN",
    "PAIR_SEPARATOR",
    "PWD",
    "REL_TOL",
    "RICH_SHOW_EDGE",
    "RICH_SHOW_LINES",
    "ROOT_GROUP_NAME",
    "ROOT_LOGGER",
    "ROOT_USER_NAME",
    "SECOND",
    "SECONDS_PER_DAY",
    "SECONDS_PER_HOUR",
    "SECONDS_PER_MINUTE",
    "SECONDS_PER_WEEK",
    "SYSTEM",
    "SYSTEM_RANDOM",
    "TEMP_DIR",
    "TIME_DELTA_MAX",
    "TIME_DELTA_MIN",
    "TIME_LOCAL",
    "TIME_UTC",
    "TODAY_LOCAL",
    "TODAY_UTC",
    "USER",
    "UTC",
    "WEEK",
    "YEAR",
    "ZERO_DAYS",
    "ZERO_TIME",
    "ZONED_DATE_TIME_MAX",
    "ZONED_DATE_TIME_MIN",
    "HongKong",
    "Sentinel",
    "Tokyo",
    "USCentral",
    "USEastern",
    "sentinel",
]
