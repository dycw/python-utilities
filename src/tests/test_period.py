from __future__ import annotations

from re import search
from typing import TYPE_CHECKING, Any, cast

from hypothesis import HealthCheck, given, settings
from hypothesis.strategies import DataObject, data
from pytest import mark, param, raises
from whenever import Date, ZonedDateTime

from utilities.hypothesis import (
    assume_does_not_raise,
    date_deltas,
    dates,
    pairs,
    plain_datetimes,
    time_deltas,
    zoned_datetimes,
)
from utilities.period import (
    DatePeriod,
    ZonedDateTimePeriod,
    _PeriodAsDict,
    _PeriodExactEqArgumentsError,
    _PeriodInvalidError,
    _PeriodTimeZoneError,
)
from utilities.tzdata import USCentral, USEastern
from utilities.whenever import DAY
from utilities.zoneinfo import UTC, get_time_zone_name

if TYPE_CHECKING:
    from collections.abc import Callable

    from whenever import DateDelta, PlainDateTime, TimeDelta


class TestDatePeriod:
    @given(dates=pairs(dates(), sorted=True), delta=date_deltas())
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_add(self, *, dates: tuple[Date, Date], delta: DateDelta) -> None:
        start, end = dates
        period = DatePeriod(start, end)
        with assume_does_not_raise(ValueError, match="Resulting date out of range"):
            result = period + delta
        expected = DatePeriod(start + delta, end + delta)
        assert result == expected

    @given(date=dates(), dates=pairs(dates(), sorted=True))
    def test_contains(self, *, date: Date, dates: tuple[Date, Date]) -> None:
        start, end = dates
        period = DatePeriod(start, end)
        result = date in period
        expected = start <= date <= end
        assert result is expected

    @given(dates=pairs(dates(), sorted=True))
    def test_delta(self, *, dates: tuple[Date, Date]) -> None:
        start, end = dates
        period = DatePeriod(start, end)
        assert period.delta == (end - start)

    @mark.parametrize(
        ("end", "expected"),
        [
            param(Date(2000, 1, 1), "20000101="),
            param(Date(2000, 1, 2), "20000101-02"),
            param(Date(2000, 1, 31), "20000101-31"),
            param(Date(2000, 2, 1), "20000101-0201"),
            param(Date(2000, 2, 29), "20000101-0229"),
            param(Date(2000, 12, 31), "20000101-1231"),
            param(Date(2001, 1, 1), "20000101-20010101"),
        ],
    )
    def test_format_compact(self, *, end: Date, expected: str) -> None:
        period = DatePeriod(Date(2000, 1, 1), end)
        assert period.format_compact() == expected

    @given(dates=pairs(dates(), sorted=True))
    def test_hashable(self, *, dates: tuple[Date, Date]) -> None:
        start, end = dates
        period = DatePeriod(start, end)
        _ = hash(period)

    @given(dates=pairs(dates(), sorted=True))
    @mark.parametrize("func", [param(repr), param(str)])
    def test_repr(self, *, dates: tuple[Date, Date], func: Callable[..., str]) -> None:
        start, end = dates
        period = DatePeriod(start, end)
        result = func(period)
        assert search(r"^DatePeriod\(\d{4}-\d{2}-\d{2}, \d{4}-\d{2}-\d{2}\)$", result)

    @given(dates1=pairs(dates(), sorted=True), dates2=pairs(dates(), sorted=True))
    def test_sortable(
        self, *, dates1: tuple[Date, Date], dates2: tuple[Date, Date]
    ) -> None:
        start1, end1 = dates1
        start2, end2 = dates2
        period1 = DatePeriod(start1, end1)
        period2 = DatePeriod(start2, end2)
        _ = sorted([period1, period2])

    @given(dates=pairs(dates(), sorted=True), delta=date_deltas())
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_sub(self, *, dates: tuple[Date, Date], delta: DateDelta) -> None:
        start, end = dates
        period = DatePeriod(start, end)
        with assume_does_not_raise(ValueError, match="Resulting date out of range"):
            result = period - delta
        expected = DatePeriod(start - delta, end - delta)
        assert result == expected

    @given(dates=pairs(dates(), sorted=True))
    def test_to_dict(self, *, dates: tuple[Date, Date]) -> None:
        start, end = dates
        period = DatePeriod(start, end)
        result = period.to_dict()
        expected = _PeriodAsDict(start=start, end=end)
        assert result == expected

    @given(dates=pairs(dates(), unique=True, sorted=True))
    def test_error_period_invalid(self, *, dates: tuple[Date, Date]) -> None:
        start, end = dates
        with raises(_PeriodInvalidError, match="Invalid period; got .* > .*"):
            _ = DatePeriod(end, start)


class TestZonedDateTimePeriod:
    @given(datetimes=pairs(zoned_datetimes(), sorted=True), delta=time_deltas())
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

    @given(datetime=zoned_datetimes(), datetimes=pairs(zoned_datetimes(), sorted=True))
    def test_contains(
        self, *, datetime: ZonedDateTime, datetimes: tuple[ZonedDateTime, ZonedDateTime]
    ) -> None:
        start, end = datetimes
        period = ZonedDateTimePeriod(start, end)
        result = datetime in period
        expected = start <= datetime <= end
        assert result is expected

    @given(datetime=zoned_datetimes(), datetimes=pairs(zoned_datetimes(), sorted=True))
    def test_contain_datetime(
        self, *, datetime: ZonedDateTime, datetimes: tuple[ZonedDateTime, ZonedDateTime]
    ) -> None:
        start, end = datetimes
        period = ZonedDateTimePeriod(start, end)
        result = datetime in period
        expected = start <= datetime <= end
        assert result is expected

    @given(datetimes=pairs(zoned_datetimes(), sorted=True))
    def test_delta(self, *, datetimes: tuple[ZonedDateTime, ZonedDateTime]) -> None:
        start, end = datetimes
        period = ZonedDateTimePeriod(start, end)
        assert period.delta == (end - start)

    @given(datetimes=pairs(zoned_datetimes(), sorted=True))
    def test_exact_eq(self, *, datetimes: tuple[ZonedDateTime, ZonedDateTime]) -> None:
        start, end = datetimes
        period = ZonedDateTimePeriod(start, end)
        assert period.exact_eq(period)
        assert period.exact_eq(period.start, period.end)
        assert period.exact_eq(
            period.start.to_plain(), period.end.to_plain(), period.time_zone
        )

    @mark.parametrize(
        ("end", "expected"),
        [
            param(
                ZonedDateTime(2000, 1, 1, 10, 20, 30, tz=UTC.key),
                "20000101T102030[UTC]=",
            ),
            param(
                ZonedDateTime(2000, 1, 1, 10, 20, 31, tz=UTC.key),
                "20000101T102030-102031[UTC]",
            ),
            param(
                ZonedDateTime(2000, 1, 1, 10, 20, 59, tz=UTC.key),
                "20000101T102030-102059[UTC]",
            ),
            param(
                ZonedDateTime(2000, 1, 1, 10, 21, tz=UTC.key),
                "20000101T102030-1021[UTC]",
            ),
            param(
                ZonedDateTime(2000, 1, 1, 10, 21, 1, tz=UTC.key),
                "20000101T102030-102101[UTC]",
            ),
            param(
                ZonedDateTime(2000, 1, 1, 10, 59, 59, tz=UTC.key),
                "20000101T102030-105959[UTC]",
            ),
            param(ZonedDateTime(2000, 1, 1, 11, tz=UTC.key), "20000101T102030-11[UTC]"),
            param(
                ZonedDateTime(2000, 1, 1, 11, 0, 1, tz=UTC.key),
                "20000101T102030-110001[UTC]",
            ),
            param(
                ZonedDateTime(2000, 1, 1, 23, 59, 59, tz=UTC.key),
                "20000101T102030-235959[UTC]",
            ),
            param(ZonedDateTime(2000, 1, 2, tz=UTC.key), "20000101T102030-02T00[UTC]"),
            param(
                ZonedDateTime(2000, 1, 2, 0, 0, 1, tz=UTC.key),
                "20000101T102030-02T000001[UTC]",
            ),
            param(
                ZonedDateTime(2000, 1, 2, 0, 0, 59, tz=UTC.key),
                "20000101T102030-02T000059[UTC]",
            ),
            param(
                ZonedDateTime(2000, 1, 2, 0, 1, tz=UTC.key),
                "20000101T102030-02T0001[UTC]",
            ),
            param(
                ZonedDateTime(2000, 1, 31, 23, 59, 59, tz=UTC.key),
                "20000101T102030-31T235959[UTC]",
            ),
            param(
                ZonedDateTime(2000, 2, 1, tz=UTC.key), "20000101T102030-0201T00[UTC]"
            ),
            param(
                ZonedDateTime(2000, 2, 1, 0, 0, 1, tz=UTC.key),
                "20000101T102030-0201T000001[UTC]",
            ),
            param(
                ZonedDateTime(2000, 2, 1, 0, 0, 59, tz=UTC.key),
                "20000101T102030-0201T000059[UTC]",
            ),
            param(
                ZonedDateTime(2000, 2, 1, 0, 1, tz=UTC.key),
                "20000101T102030-0201T0001[UTC]",
            ),
            param(
                ZonedDateTime(2000, 12, 31, 23, 59, 59, tz=UTC.key),
                "20000101T102030-1231T235959[UTC]",
            ),
            param(
                ZonedDateTime(2001, 1, 1, tz=UTC.key),
                "20000101T102030-20010101T00[UTC]",
            ),
            param(
                ZonedDateTime(2001, 1, 1, 0, 0, 1, tz=UTC.key),
                "20000101T102030-20010101T000001[UTC]",
            ),
            param(
                ZonedDateTime(2001, 1, 1, 0, 0, 59, tz=UTC.key),
                "20000101T102030-20010101T000059[UTC]",
            ),
            param(
                ZonedDateTime(2001, 1, 1, 0, 1, tz=UTC.key),
                "20000101T102030-20010101T0001[UTC]",
            ),
        ],
    )
    def test_format_compact(self, *, end: ZonedDateTime, expected: str) -> None:
        start = ZonedDateTime(2000, 1, 1, 10, 20, 30, tz=UTC.key)
        period = ZonedDateTimePeriod(start, end)
        assert period.format_compact() == expected

    @mark.parametrize(
        ("datetime", "expected"),
        [
            param(
                ZonedDateTime(2000, 1, 1, 10, 20, 30, tz=UTC.key),
                "20000101T102030[UTC]=",
            ),
            param(ZonedDateTime(2000, 1, 1, 10, 20, tz=UTC.key), "20000101T1020[UTC]="),
            param(ZonedDateTime(2000, 1, 1, 10, tz=UTC.key), "20000101T10[UTC]="),
        ],
    )
    def test_format_compact_extra(
        self, *, datetime: ZonedDateTime, expected: str
    ) -> None:
        period = ZonedDateTimePeriod(datetime, datetime)
        assert period.format_compact() == expected

    @given(datetimes=pairs(zoned_datetimes(), sorted=True))
    def test_hashable(self, *, datetimes: tuple[ZonedDateTime, ZonedDateTime]) -> None:
        start, end = datetimes
        period = ZonedDateTimePeriod(start, end)
        _ = hash(period)

    @given(data=data(), datetimes=pairs(zoned_datetimes(), sorted=True))
    @mark.parametrize("func", [param(repr), param(str)])
    def test_repr(
        self,
        *,
        data: DataObject,
        datetimes: tuple[ZonedDateTime, ZonedDateTime],
        func: Callable[..., str],
    ) -> None:
        start, end = datetimes
        datetimes = data.draw(pairs(zoned_datetimes(), sorted=True))
        start, end = datetimes
        period = ZonedDateTimePeriod(start, end)
        result = func(period)
        assert search(
            r"^ZonedDateTimePeriod\(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?, \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?\[.+\]\)$",
            result,
        )

    @given(
        dates1=pairs(zoned_datetimes(), sorted=True),
        dates2=pairs(zoned_datetimes(), sorted=True),
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

    @given(datetimes=pairs(zoned_datetimes(), sorted=True), delta=time_deltas())
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_sub(
        self, *, datetimes: tuple[ZonedDateTime, ZonedDateTime], delta: TimeDelta
    ) -> None:
        start, end = datetimes
        period = ZonedDateTimePeriod(start, end)
        with assume_does_not_raise(ValueError, match="Instant is out of range"):
            result = period - delta
        expected = ZonedDateTimePeriod(start - delta, end - delta)
        assert result == expected

    @given(datetimes=pairs(zoned_datetimes(), sorted=True))
    def test_to_dict(self, *, datetimes: tuple[ZonedDateTime, ZonedDateTime]) -> None:
        start, end = datetimes
        period = ZonedDateTimePeriod(start, end)
        result = period.to_dict()
        expected = _PeriodAsDict(start=start, end=end)
        assert result == expected

    @given(datetimes=pairs(zoned_datetimes(), sorted=True))
    def test_to_tz(self, *, datetimes: tuple[ZonedDateTime, ZonedDateTime]) -> None:
        start, end = datetimes
        period = ZonedDateTimePeriod(start, end)
        with assume_does_not_raise(OverflowError, match="date value out of range"):
            result = period.to_tz(UTC)
        assert result.time_zone == UTC
        name = get_time_zone_name(UTC)
        expected = ZonedDateTimePeriod(start.to_tz(name), end.to_tz(name))
        assert result == expected

    @given(datetimes=pairs(zoned_datetimes(), unique=True, sorted=True))
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_error_period_invalid(
        self, *, datetimes: tuple[ZonedDateTime, ZonedDateTime]
    ) -> None:
        start, end = datetimes
        with raises(_PeriodInvalidError, match="Invalid period; got .* > .*"):
            _ = ZonedDateTimePeriod(end, start)

    @given(datetimes=pairs(plain_datetimes(), sorted=True))
    def test_error_period_time_zone(
        self, *, datetimes: tuple[PlainDateTime, PlainDateTime]
    ) -> None:
        plain_start, plain_end = datetimes
        with assume_does_not_raise(OverflowError, match="date value out of range"):
            start = (plain_start - DAY).assume_tz(USCentral.key)
            end = (plain_end + DAY).assume_tz(USEastern.key)
        with raises(
            _PeriodTimeZoneError,
            match="Period must contain exactly one time zone; got .* and .*",
        ):
            _ = ZonedDateTimePeriod(start, end)

    @given(datetimes=pairs(zoned_datetimes(), sorted=True))
    def test_error_exact_eq(
        self, *, datetimes: tuple[ZonedDateTime, ZonedDateTime]
    ) -> None:
        start, end = datetimes
        period = ZonedDateTimePeriod(start, end)
        with raises(
            _PeriodExactEqArgumentsError, match=r"Invalid arguments; got \(.*\)"
        ):
            _ = period.exact_eq(cast("Any", start))
