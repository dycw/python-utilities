from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Self
from zoneinfo import ZoneInfo

from hypothesis import given
from whenever import Date, PlainDateTime, Time, ZonedDateTime

from utilities.constants import UTC, HongKong, Sentinel, Tokyo, sentinel
from utilities.core import (
    get_now,
    get_now_local,
    get_now_local_plain,
    get_now_plain,
    get_time,
    get_time_local,
    get_today,
    get_today_local,
    replace_non_sentinel,
    to_date,
)
from utilities.hypothesis import dates, pairs, zone_infos

if TYPE_CHECKING:
    from utilities.types import MaybeCallableDateLike, Pair


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


class TestToDate:
    def test_default(self) -> None:
        assert to_date() == get_today()

    @given(date=dates())
    def test_date(self, *, date: Date) -> None:
        assert to_date(date) == date

    @given(date=dates())
    def test_str(self, *, date: Date) -> None:
        assert to_date(date.format_iso()) == date

    @given(date=dates())
    def test_py_date(self, *, date: Date) -> None:
        assert to_date(date.py_date()) == date

    @given(date=dates())
    def test_callable(self, *, date: Date) -> None:
        assert to_date(lambda: date) == date

    def test_none(self) -> None:
        assert to_date(None) == get_today()

    def test_sentinel(self) -> None:
        assert to_date(sentinel) is sentinel

    @given(dates=pairs(dates()))
    def test_replace_non_sentinel(self, *, dates: Pair[Date]) -> None:
        date1, date2 = dates

        @dataclass(kw_only=True, slots=True)
        class Example:
            date: Date = field(default_factory=get_today)

            def replace(
                self, *, date: MaybeCallableDateLike | Sentinel = sentinel
            ) -> Self:
                return replace_non_sentinel(self, date=to_date(date))

        obj = Example(date=date1)
        assert obj.date == date1
        assert obj.replace().date == date1
        assert obj.replace(date=date2).date == date2
        assert obj.replace(date=get_today).date == get_today()
