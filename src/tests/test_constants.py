from __future__ import annotations

from pathlib import Path
from random import SystemRandom
from typing import assert_never
from zoneinfo import ZoneInfo

from pytest import mark, param, raises

from utilities.constants import (
    CPU_COUNT,
    EFFECTIVE_GROUP_ID,
    EFFECTIVE_GROUP_NAME,
    EFFECTIVE_USER_ID,
    EFFECTIVE_USER_NAME,
    IS_LINUX,
    IS_MAC,
    IS_NOT_LINUX,
    IS_NOT_MAC,
    IS_NOT_WINDOWS,
    IS_WINDOWS,
    LOCAL_TIME_ZONE,
    LOCAL_TIME_ZONE_NAME,
    MAX_PID,
    ROOT_GROUP_NAME,
    ROOT_USER_NAME,
    SYSTEM_RANDOM,
    TEMP_DIR,
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
