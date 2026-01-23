from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Self
from zoneinfo import ZoneInfo

from hypothesis import given
from hypothesis.strategies import integers, sampled_from
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
    DAY,
    HOUR,
    MICROSECOND,
    MILLISECOND,
    MINUTE,
    MONTH,
    NANOSECOND,
    SECOND,
    UTC,
    HongKong,
    Sentinel,
    Tokyo,
    sentinel,
)
from utilities.core import (
    _DeltaComponentsOutput,
    _ToDaysMonthsError,
    _ToDaysNanosecondsError,
    delta_components,
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
    to_days,
)
from utilities.hypothesis import assume_does_not_raise, dates, pairs, zone_infos

if TYPE_CHECKING:
    from utilities.types import (
        DateOrDateTimeDelta,
        Delta,
        MaybeCallableDateLike,
        Pair,
        TimeOrDateTimeDelta,
    )


class TestDeltaComponents:
    @mark.parametrize(
        ("delta", "expected"),
        [
            param(MONTH, _DeltaComponentsOutput(months=1)),
            param(DAY, _DeltaComponentsOutput(days=1)),
            param(48 * HOUR, _DeltaComponentsOutput(days=2)),
            param(36 * HOUR, _DeltaComponentsOutput(days=1, hours=12)),
            param(24 * HOUR, _DeltaComponentsOutput(days=1)),
            param(HOUR, _DeltaComponentsOutput(hours=1)),
            param(120 * MINUTE, _DeltaComponentsOutput(hours=2)),
            param(90 * MINUTE, _DeltaComponentsOutput(hours=1, minutes=30)),
            param(60 * MINUTE, _DeltaComponentsOutput(hours=1)),
            param(120 * SECOND, _DeltaComponentsOutput(minutes=2)),
            param(SECOND, _DeltaComponentsOutput(seconds=1)),
            param(MILLISECOND, _DeltaComponentsOutput(milliseconds=1)),
            param(MICROSECOND, _DeltaComponentsOutput(microseconds=1)),
            param(NANOSECOND, _DeltaComponentsOutput(nanoseconds=1)),
        ],
    )
    def test_main(self, *, delta: Delta, expected: _DeltaComponentsOutput) -> None:
        assert delta_components(delta) == expected


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


class TestToDays:
    @given(cls=sampled_from([DateDelta, DateTimeDelta]), days=integers())
    def test_date_or_date_time_delta(
        self, *, cls: type[DateOrDateTimeDelta], days: int
    ) -> None:
        with (
            assume_does_not_raise(ValueError, match=r"Out of range"),
            assume_does_not_raise(ValueError, match=r"days out of range"),
            assume_does_not_raise(
                OverflowError, match=r"Python int too large to convert to C long"
            ),
        ):
            delta = cls(days=days)
        assert to_days(delta) == days

    @given(days=integers())
    def test_time_delta(self, *, days: int) -> None:
        with (
            assume_does_not_raise(ValueError, match=r"Out of range"),
            assume_does_not_raise(ValueError, match=r"hours out of range"),
            assume_does_not_raise(OverflowError, match=r"int too big to convert"),
            assume_does_not_raise(
                OverflowError, match=r"Python int too large to convert to C long"
            ),
        ):
            delta = TimeDelta(hours=24 * days)
        assert to_days(delta) == days

    @mark.parametrize(
        "delta", [param(DateDelta(months=1)), param(DateTimeDelta(months=1))]
    )
    def test_error_months(self, *, delta: DateOrDateTimeDelta) -> None:
        with raises(_ToDaysMonthsError, match=r"Delta must not contain months; got 1"):
            _ = to_days(delta)

    @mark.parametrize(
        "delta", [param(TimeDelta(nanoseconds=1)), param(DateTimeDelta(nanoseconds=1))]
    )
    def test_error_nanoseconds(self, *, delta: TimeOrDateTimeDelta) -> None:
        with raises(
            _ToDaysNanosecondsError,
            match=r"Delta must not contain extra nanoseconds; got .*",
        ):
            _ = to_days(delta)
