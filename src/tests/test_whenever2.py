from __future__ import annotations

from logging import DEBUG
from zoneinfo import ZoneInfo

from hypothesis import given
from hypothesis.strategies import timezones
from pytest import raises
from whenever import DateDelta, ZonedDateTime

from utilities.tzdata import HongKong, Tokyo
from utilities.whenever2 import (
    DATE_MAX,
    DATE_MIN,
    NOW_UTC,
    PLAIN_DATETIME_MAX,
    PLAIN_DATETIME_MIN,
    ZONED_DATETIME_MAX,
    ZONED_DATETIME_MIN,
    WheneverLogRecord,
    get_now,
    get_now_local,
)
from utilities.zoneinfo import UTC


class TestGetNow:
    @given(time_zone=timezones())
    def test_function(self, *, time_zone: ZoneInfo) -> None:
        now = get_now(time_zone=time_zone)
        assert isinstance(now, ZonedDateTime)
        assert now.tz == time_zone.key

    def test_constant(self) -> None:
        assert isinstance(NOW_UTC, ZonedDateTime)
        assert NOW_UTC.tz == "UTC"


class TestGetNowLocal:
    def test_function(self) -> None:
        now = get_now_local()
        assert isinstance(now, ZonedDateTime)
        ETC = ZoneInfo("Etc/UTC")  # noqa: N806
        time_zones = {ETC, HongKong, Tokyo, UTC}
        assert any(now.tz == time_zone.key for time_zone in time_zones)


class TestMinMax:
    def test_date_min(self) -> None:
        with raises(ValueError, match="Resulting date out of range"):
            _ = DATE_MIN - DateDelta(days=1)

    def test_date_max(self) -> None:
        with raises(ValueError, match="Resulting date out of range"):
            _ = DATE_MAX + DateDelta(days=1)

    def test_plain_datetime_min(self) -> None:
        with raises(ValueError, match=r"Result of subtract\(\) out of range"):
            _ = PLAIN_DATETIME_MIN.subtract(nanoseconds=1, ignore_dst=True)

    def test_plain_datetime_max(self) -> None:
        _ = PLAIN_DATETIME_MAX.add(nanoseconds=999, ignore_dst=True)
        with raises(ValueError, match=r"Result of add\(\) out of range"):
            _ = PLAIN_DATETIME_MAX.add(microseconds=1, ignore_dst=True)

    def test_zoned_datetime_min(self) -> None:
        with raises(ValueError, match="Resulting datetime is out of range"):
            _ = ZONED_DATETIME_MIN.subtract(nanoseconds=1)

    def test_zoned_datetime_max(self) -> None:
        _ = ZONED_DATETIME_MAX.add(nanoseconds=999)
        with raises(ValueError, match="Resulting datetime is out of range"):
            _ = ZONED_DATETIME_MAX.add(microseconds=1)


class TestWheneverLogRecord:
    def test_init(self) -> None:
        _ = WheneverLogRecord("name", DEBUG, "pathname", 0, None, None, None)

    def test_get_length(self) -> None:
        assert isinstance(WheneverLogRecord._get_length(), int)

    def test_get_time_zone(self) -> None:
        assert isinstance(WheneverLogRecord._get_time_zone(), ZoneInfo)

    def test_get_time_zone_key(self) -> None:
        assert isinstance(WheneverLogRecord._get_time_zone_key(), str)
