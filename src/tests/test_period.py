from __future__ import annotations

from re import search
from typing import TYPE_CHECKING

from hypothesis import HealthCheck, given, settings
from hypothesis.strategies import DataObject, data, sampled_from, timezones
from pytest import raises

from utilities.hypothesis import (
    assume_does_not_raise,
    date_deltas_whenever,
    dates_whenever,
    pairs,
    plain_datetimes_whenever,
    time_deltas_whenever,
    zoned_datetimes_whenever,
)
from utilities.period import (
    DatePeriod,
    ZonedDateTimePeriod,
    _PeriodAsDict,
    _PeriodInvalidError,
    _PeriodTimeZoneError,
)
from utilities.whenever2 import DAY
from utilities.zoneinfo import get_time_zone_name

if TYPE_CHECKING:
    from collections.abc import Callable
    from zoneinfo import ZoneInfo

    from whenever import Date, DateDelta, PlainDateTime, TimeDelta, ZonedDateTime


class TestDatePeriod:
    @given(dates=pairs(dates_whenever(), sorted=True), delta=date_deltas_whenever())
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_add(self, *, dates: tuple[Date, Date], delta: DateDelta) -> None:
        start, end = dates
        period = DatePeriod(start, end)
        with assume_does_not_raise(ValueError, match="Resulting date out of range"):
            result = period + delta
        expected = DatePeriod(start + delta, end + delta)
        assert result == expected

    @given(date=dates_whenever(), dates=pairs(dates_whenever(), sorted=True))
    def test_contains(self, *, date: Date, dates: tuple[Date, Date]) -> None:
        start, end = dates
        period = DatePeriod(start, end)
        result = date in period
        expected = start <= date <= end
        assert result is expected

    @given(dates=pairs(dates_whenever(), sorted=True))
    def test_delta(self, *, dates: tuple[Date, Date]) -> None:
        start, end = dates
        period = DatePeriod(start, end)
        assert period.delta == (end - start)

    @given(dates=pairs(dates_whenever(), sorted=True))
    def test_hashable(self, *, dates: tuple[Date, Date]) -> None:
        start, end = dates
        period = DatePeriod(start, end)
        _ = hash(period)

    @given(dates=pairs(dates_whenever(), sorted=True), func=sampled_from([repr, str]))
    def test_repr(self, *, dates: tuple[Date, Date], func: Callable[..., str]) -> None:
        start, end = dates
        period = DatePeriod(start, end)
        result = func(period)
        assert search(r"^DatePeriod\(\d{4}-\d{2}-\d{2}, \d{4}-\d{2}-\d{2}\)$", result)

    @given(
        dates1=pairs(dates_whenever(), sorted=True),
        dates2=pairs(dates_whenever(), sorted=True),
    )
    def test_sortable(
        self, *, dates1: tuple[Date, Date], dates2: tuple[Date, Date]
    ) -> None:
        start1, end1 = dates1
        start2, end2 = dates2
        period1 = DatePeriod(start1, end1)
        period2 = DatePeriod(start2, end2)
        _ = sorted([period1, period2])

    @given(dates=pairs(dates_whenever(), sorted=True), delta=date_deltas_whenever())
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_sub(self, *, dates: tuple[Date, Date], delta: DateDelta) -> None:
        start, end = dates
        period = DatePeriod(start, end)
        with assume_does_not_raise(ValueError, match="Resulting date out of range"):
            result = period - delta
        expected = DatePeriod(start - delta, end - delta)
        assert result == expected

    @given(dates=pairs(dates_whenever(), sorted=True))
    def test_to_dict(self, *, dates: tuple[Date, Date]) -> None:
        start, end = dates
        period = DatePeriod(start, end)
        result = period.to_dict()
        expected = _PeriodAsDict(start=start, end=end)
        assert result == expected

    @given(dates=pairs(dates_whenever(), unique=True, sorted=True))
    def test_error_period_invalid(self, *, dates: tuple[Date, Date]) -> None:
        start, end = dates
        with raises(_PeriodInvalidError, match="Invalid period; got .* > .*"):
            _ = DatePeriod(end, start)


class TestZonedDateTimePeriod:
    @given(
        datetimes=pairs(zoned_datetimes_whenever(), sorted=True),
        delta=time_deltas_whenever(),
    )
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_add(
        self, *, datetimes: tuple[ZonedDateTime, ZonedDateTime], delta: TimeDelta
    ) -> None:
        start, end = datetimes
        period = ZonedDateTimePeriod(start, end)
        with assume_does_not_raise(ValueError, match="Instant is out of range"):
            result = period + delta
        expected = ZonedDateTimePeriod(start + delta, end + delta)
        assert result == expected

    @given(
        datetime=zoned_datetimes_whenever(),
        datetimes=pairs(zoned_datetimes_whenever(), sorted=True),
    )
    def test_contains(
        self, *, datetime: ZonedDateTime, datetimes: tuple[ZonedDateTime, ZonedDateTime]
    ) -> None:
        start, end = datetimes
        period = ZonedDateTimePeriod(start, end)
        result = datetime in period
        expected = start <= datetime <= end
        assert result is expected

    @given(
        datetime=zoned_datetimes_whenever(),
        datetimes=pairs(zoned_datetimes_whenever(), sorted=True),
    )
    def test_contain_datetime(
        self, *, datetime: ZonedDateTime, datetimes: tuple[ZonedDateTime, ZonedDateTime]
    ) -> None:
        start, end = datetimes
        period = ZonedDateTimePeriod(start, end)
        result = datetime in period
        expected = start <= datetime <= end
        assert result is expected

    @given(datetimes=pairs(zoned_datetimes_whenever(), sorted=True))
    def test_delta(self, *, datetimes: tuple[ZonedDateTime, ZonedDateTime]) -> None:
        start, end = datetimes
        period = ZonedDateTimePeriod(start, end)
        assert period.delta == (end - start)

    @given(datetimes=pairs(zoned_datetimes_whenever(), sorted=True))
    def test_hashable(self, *, dates: tuple[ZonedDateTime, ZonedDateTime]) -> None:
        start, end = dates
        period = ZonedDateTimePeriod(start, end)
        _ = hash(period)

    @given(
        data=data(),
        datetimes=pairs(zoned_datetimes_whenever(), sorted=True),
        func=sampled_from([repr, str]),
    )
    def test_repr(
        self,
        *,
        data: DataObject,
        datetimes: tuple[ZonedDateTime, ZonedDateTime],
        func: Callable[..., str],
    ) -> None:
        start, end = datetimes
        datetimes = data.draw(pairs(zoned_datetimes_whenever(), sorted=True))
        start, end = datetimes
        period = ZonedDateTimePeriod(start, end)
        result = func(period)
        assert search(
            r"^ZonedDateTimePeriod\(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?, \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?\[.+\]\)$",
            result,
        )

    @given(
        dates1=pairs(zoned_datetimes_whenever(), sorted=True),
        dates2=pairs(zoned_datetimes_whenever(), sorted=True),
    )
    def test_sortable(
        self,
        *,
        dates1: tuple[ZonedDateTime, ZonedDateTime],
        dates2: tuple[ZonedDateTime, ZonedDateTime],
    ) -> None:
        start1, end1 = dates1
        start2, end2 = dates2
        period1 = ZonedDateTimePeriod(start1, end1)
        period2 = ZonedDateTimePeriod(start2, end2)
        _ = sorted([period1, period2])

    @given(
        datetimes=pairs(zoned_datetimes_whenever(), sorted=True),
        delta=time_deltas_whenever(),
    )
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_sub(
        self, *, dates: tuple[ZonedDateTime, ZonedDateTime], delta: TimeDelta
    ) -> None:
        start, end = dates
        period = ZonedDateTimePeriod(start, end)
        with assume_does_not_raise(ValueError, match="Instant is out of range"):
            result = period - delta
        expected = ZonedDateTimePeriod(start - delta, end - delta)
        assert result == expected

    @given(datetimes=pairs(zoned_datetimes_whenever(), sorted=True))
    def test_to_dict(self, *, datetimes: tuple[ZonedDateTime, ZonedDateTime]) -> None:
        start, end = datetimes
        period = ZonedDateTimePeriod(start, end)
        result = period.to_dict()
        expected = _PeriodAsDict(start=start, end=end)
        assert result == expected

    @given(
        datetimes=pairs(zoned_datetimes_whenever(), sorted=True), time_zone=timezones()
    )
    def test_to_tz(
        self, *, datetimes: tuple[ZonedDateTime, ZonedDateTime], time_zone: ZoneInfo
    ) -> None:
        start, end = datetimes
        period = ZonedDateTimePeriod(start, end)
        with assume_does_not_raise(OverflowError, match="date value out of range"):
            result = period.to_tz(time_zone)
        assert result.time_zone == time_zone
        name = get_time_zone_name(time_zone)
        expected = ZonedDateTimePeriod(start.to_tz(name), end.to_tz(name))
        assert result == expected

    @given(datetimes=pairs(zoned_datetimes_whenever(), unique=True, sorted=True))
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_error_period_invalid(
        self, *, datetimes: tuple[ZonedDateTime, ZonedDateTime]
    ) -> None:
        start, end = datetimes
        with raises(_PeriodInvalidError, match="Invalid period; got .* > .*"):
            _ = ZonedDateTimePeriod(end, start)

    @given(
        datetimes=pairs(plain_datetimes_whenever(), sorted=True),
        time_zones=pairs(timezones(), unique=True),
    )
    def test_error_period_time_zone(
        self,
        *,
        datetimes: tuple[PlainDateTime, PlainDateTime],
        time_zones: tuple[ZoneInfo, ZoneInfo],
    ) -> None:
        plain_start, plain_end = datetimes
        time_zone1, time_zone2 = time_zones
        start = (plain_start - DAY).assume_tz(time_zone1.key)
        end = (plain_end + DAY).assume_tz(time_zone2.key)
        with raises(
            _PeriodTimeZoneError,
            match="Period must contain exactly one time zone; got .* and .*",
        ):
            _ = ZonedDateTimePeriod(start, end)
