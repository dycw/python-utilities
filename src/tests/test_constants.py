from __future__ import annotations

from pathlib import Path
from random import SystemRandom
from typing import assert_never
from zoneinfo import ZoneInfo

from pytest import mark, param, raises
from whenever import (
    Date,
    DateDelta,
    DateTimeDelta,
    PlainDateTime,
    Time,
    TimeDelta,
    ZonedDateTime,
)

from utilities.constants import (
    CPU_COUNT,
    DATE_DELTA_MAX,
    DATE_DELTA_MIN,
    EFFECTIVE_GROUP_ID,
    EFFECTIVE_GROUP_NAME,
    EFFECTIVE_USER_ID,
    EFFECTIVE_USER_NAME,
    HOME,
    IS_LINUX,
    IS_MAC,
    IS_NOT_LINUX,
    IS_NOT_MAC,
    IS_NOT_WINDOWS,
    IS_WINDOWS,
    LOCAL_TIME_ZONE,
    LOCAL_TIME_ZONE_NAME,
    MAX_PID,
    NANOSECOND,
    NOW_LOCAL,
    NOW_LOCAL_PLAIN,
    NOW_UTC,
    NOW_UTC_PLAIN,
    PWD,
    ROOT_GROUP_NAME,
    ROOT_USER_NAME,
    SYSTEM_RANDOM,
    TEMP_DIR,
    TIME_DELTA_MAX,
    TIME_DELTA_MIN,
    TIME_LOCAL,
    TIME_UTC,
    TODAY_LOCAL,
    TODAY_UTC,
    USER,
    ZONED_DATE_TIME_MAX,
    ZONED_DATE_TIME_MIN,
)
from utilities.platform import SYSTEM
from utilities.types import System, TimeZone
from utilities.typing import get_literal_elements


class TestCPUCount:
    def test_main(self) -> None:
        assert isinstance(CPU_COUNT, int)
        assert CPU_COUNT >= 1


class TestDateDeltaMinMax:
    def test_min(self) -> None:
        with raises(ValueError, match=r"days out of range"):
            _ = DateDelta(weeks=-521722, days=-6)
        with raises(ValueError, match=r"Addition result out of bounds"):
            _ = DATE_DELTA_MIN - DateDelta(days=1)

    def test_date_delta_max(self) -> None:
        with raises(ValueError, match=r"days out of range"):
            _ = DateDelta(weeks=521722, days=6)
        with raises(ValueError, match=r"Addition result out of bounds"):
            _ = DATE_DELTA_MAX + DateDelta(days=1)


class TestDateTimeDeltaMinMax:
    def test_min(self) -> None:
        with raises(ValueError, match=r"Out of range"):
            _ = DateTimeDelta(weeks=-521722, days=-6)

    def test_max(self) -> None:
        with raises(ValueError, match=r"Out of range"):
            _ = DateTimeDelta(weeks=521722, days=6)


class TestGroupId:
    def test_main(self) -> None:
        match SYSTEM:
            case "windows":  # skipif-not-windows
                assert EFFECTIVE_GROUP_ID is None
            case "mac" | "linux":  # skipif-windows
                assert isinstance(EFFECTIVE_GROUP_ID, int)
            case never:
                assert_never(never)


class TestGroupName:
    @mark.parametrize(
        "group",
        [
            param(ROOT_GROUP_NAME, id="root"),
            param(EFFECTIVE_GROUP_NAME, id="effective"),
        ],
    )
    def test_main(self, *, group: str | None) -> None:
        match SYSTEM:
            case "windows":  # skipif-not-windows
                assert group is None
            case "mac" | "linux":  # skipif-windows
                assert isinstance(group, str)
            case never:
                assert_never(never)


class TestLocalTimeZone:
    def test_main(self) -> None:
        assert isinstance(LOCAL_TIME_ZONE, ZoneInfo)


class TestLocalTimeZoneName:
    def test_main(self) -> None:
        assert isinstance(LOCAL_TIME_ZONE_NAME, str)
        assert LOCAL_TIME_ZONE_NAME in get_literal_elements(TimeZone)


class TestMaxPID:
    def test_main(self) -> None:
        match SYSTEM:
            case "windows":  # skipif-not-windows
                assert MAX_PID is None
            case "mac":  # skipif-not-macos
                assert isinstance(MAX_PID, int)
            case "linux":  # skipif-not-linux
                assert isinstance(MAX_PID, int)
            case never:
                assert_never(never)


class TestNow:
    @mark.parametrize("date_time", [param(NOW_LOCAL), param(NOW_UTC)])
    def test_now(self, *, date_time: ZonedDateTime) -> None:
        assert isinstance(date_time, ZonedDateTime)

    @mark.parametrize("date", [param(TODAY_LOCAL), param(TODAY_UTC)])
    def test_today(self, *, date: Date) -> None:
        assert isinstance(date, Date)

    @mark.parametrize("time", [param(TIME_LOCAL), param(TIME_UTC)])
    def test_time(self, *, time: Time) -> None:
        assert isinstance(time, Time)

    @mark.parametrize("date_time", [param(NOW_LOCAL_PLAIN), param(NOW_UTC_PLAIN)])
    def test_plain(self, *, date_time: PlainDateTime) -> None:
        assert isinstance(date_time, PlainDateTime)


class TestPaths:
    @mark.parametrize("path", [param(HOME), param(PWD)])
    def test_main(self, *, path: Path) -> None:
        assert isinstance(path, Path)
        assert path.is_dir()


class TestSystemRandom:
    def test_main(self) -> None:
        assert isinstance(SYSTEM_RANDOM, SystemRandom)


class TestSystem:
    def test_main(self) -> None:
        assert isinstance(SYSTEM, str)
        assert SYSTEM in get_literal_elements(System)

    @mark.parametrize(
        "predicate",
        [
            param(IS_WINDOWS, id="IS_WINDOWS"),
            param(IS_MAC, id="IS_MAC"),
            param(IS_LINUX, id="IS_LINUX"),
            param(IS_NOT_WINDOWS, id="IS_NOT_WINDOWS"),
            param(IS_NOT_MAC, id="IS_NOT_MAC"),
            param(IS_NOT_LINUX, id="IS_NOT_LINUX"),
        ],
    )
    def test_predicates(self, *, predicate: bool) -> None:
        assert isinstance(predicate, bool)


class TestTempDir:
    def test_main(self) -> None:
        assert isinstance(TEMP_DIR, Path)


class TestTimeDeltaMinMax:
    def test_min(self) -> None:
        with raises(ValueError, match=r"hours out of range"):
            _ = TimeDelta(hours=-87831217)
        with raises(ValueError, match=r"TimeDelta out of range"):
            _ = TimeDelta(nanoseconds=TIME_DELTA_MIN.in_nanoseconds() - 1)
        with raises(ValueError, match=r"Addition result out of range"):
            _ = TIME_DELTA_MIN - NANOSECOND

    def test_max(self) -> None:
        with raises(ValueError, match=r"hours out of range"):
            _ = TimeDelta(hours=87831217)
        with raises(ValueError, match=r"TimeDelta out of range"):
            _ = TimeDelta(nanoseconds=TIME_DELTA_MAX.in_nanoseconds() + 1)
        _ = TIME_DELTA_MAX + NANOSECOND


class TestUser:
    def test_main(self) -> None:
        assert isinstance(USER, str)


class TestUserId:
    def test_main(self) -> None:
        match SYSTEM:
            case "windows":  # skipif-not-windows
                assert EFFECTIVE_USER_ID is None
            case "mac" | "linux":  # skipif-windows
                assert isinstance(EFFECTIVE_USER_ID, int)
            case never:
                assert_never(never)


class TestUserName:
    @mark.parametrize(
        "user",
        [param(ROOT_USER_NAME, id="root"), param(EFFECTIVE_USER_NAME, id="effective")],
    )
    def test_main(self, *, user: str | None) -> None:
        match SYSTEM:
            case "windows":  # skipif-not-windows
                assert user is None
            case "mac" | "linux":  # skipif-windows
                assert isinstance(user, str)
            case never:
                assert_never(never)


class TestZonedDateTimeMinMax:
    def test_min(self) -> None:
        with raises(ValueError, match=r"Instant is out of range"):
            _ = ZONED_DATE_TIME_MAX.add(microseconds=1)

    def test_max(self) -> None:
        with raises(ValueError, match=r"Instant is out of range"):
            _ = ZONED_DATE_TIME_MIN.subtract(nanoseconds=1)
