from __future__ import annotations

from zoneinfo import ZoneInfo

from hypothesis import given
from whenever import Date, PlainDateTime, Time, ZonedDateTime

from utilities.constants import UTC, HongKong, Tokyo
from utilities.core import (
    get_now,
    get_now_local,
    get_now_local_plain,
    get_now_plain,
    get_time,
    get_time_local,
    get_today,
    get_today_local,
)
from utilities.hypothesis import zone_infos


class TestGetNow:
    @given(time_zone=zone_infos())
    def test_function(self, *, time_zone: ZoneInfo) -> None:
        now = get_now(time_zone)
        assert isinstance(now, ZonedDateTime)
        assert now.tz == time_zone.key


class TestGetNowLocal:
    def test_function(self) -> None:
        now = get_now_local()
        assert isinstance(now, ZonedDateTime)
        ETC = ZoneInfo("Etc/UTC")  # noqa: N806
        time_zones = {ETC, HongKong, Tokyo, UTC}
        assert any(now.tz == time_zone.key for time_zone in time_zones)


class TestGetNowLocalPlain:
    def test_function(self) -> None:
        now = get_now_local_plain()
        assert isinstance(now, PlainDateTime)


class TestGetNowPlain:
    @given(time_zone=zone_infos())
    def test_function(self, *, time_zone: ZoneInfo) -> None:
        now = get_now_plain(time_zone)
        assert isinstance(now, PlainDateTime)


class TestGetTime:
    @given(time_zone=zone_infos())
    def test_function(self, *, time_zone: ZoneInfo) -> None:
        now = get_time(time_zone)
        assert isinstance(now, Time)


class TestGetTimeLocal:
    def test_function(self) -> None:
        now = get_time_local()
        assert isinstance(now, Time)


class TestGetToday:
    def test_function(self) -> None:
        today = get_today()
        assert isinstance(today, Date)


class TestGetTodayLocal:
    def test_function(self) -> None:
        today = get_today_local()
        assert isinstance(today, Date)
