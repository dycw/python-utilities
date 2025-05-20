from __future__ import annotations

from dataclasses import dataclass
from re import search
from typing import TYPE_CHECKING

from hypothesis import HealthCheck, assume, given, settings
from hypothesis.strategies import (
    DataObject,
    booleans,
    data,
    dates,
    datetimes,
    just,
    permutations,
    sampled_from,
    timedeltas,
    timezones,
    tuples,
)
from pytest import raises

from utilities.datetime import ZERO_TIME
from utilities.hypothesis import assume_does_not_raise, pairs, zoned_datetimes
from utilities.period import (
    Period,
    _DateOrDateTime,
    _PeriodAsTimeZoneInapplicableError,
    _PeriodDateAndDateTimeMixedError,
    _PeriodDateContainsDateTimeError,
    _PeriodDateTimeContainsDateError,
    _PeriodInvalidError,
    _PeriodMaxDurationError,
    _PeriodMinDurationError,
    _PeriodNaiveDateTimeError,
    _PeriodReqDurationError,
    _PeriodTimeZoneInapplicableError,
    _PeriodTimeZoneNonUniqueError,
    _TPeriod,
)
from utilities.whenever import SerializeZonedDateTimeError

if TYPE_CHECKING:
    import datetime as dt
    from collections.abc import Callable
    from zoneinfo import ZoneInfo


class TestPeriod:
    @given(dates=pairs(dates(), sorted=True), duration=timedeltas())
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_add(
        self, *, dates: tuple[dt.date, dt.date], duration: dt.timedelta
    ) -> None:
        start, end = dates
        period = Period(start, end)
        with assume_does_not_raise(OverflowError, match="date value out of range"):
            result = period + duration
            adj_start, adj_end = [d + duration for d in dates]
        expected = Period(adj_start, adj_end)
        assert result == expected

    @given(
        datetimes=pairs(zoned_datetimes(), sorted=True),
        time_zone1=timezones(),
        time_zone2=timezones(),
    )
    def test_astimezone(
        self,
        *,
        datetimes: tuple[dt.datetime, dt.datetime],
        time_zone1: ZoneInfo,
        time_zone2: ZoneInfo,
    ) -> None:
        with assume_does_not_raise(OverflowError, match="date value out of range"):
            start, end = [d.astimezone(time_zone1) for d in datetimes]
        period = Period(start, end)
        with assume_does_not_raise(OverflowError, match="date value out of range"):
            result = period.astimezone(time_zone2)
            adj_start, adj_end = [d.astimezone(time_zone2) for d in datetimes]
        expected = Period(adj_start, adj_end)
        assert result == expected

    @given(date=dates(), dates=pairs(dates(), sorted=True))
    def test_contain_date(
        self, *, date: dt.date, dates: tuple[dt.date, dt.date]
    ) -> None:
        start, end = dates
        period = Period(start, end)
        result = date in period
        expected = start <= date <= end
        assert result is expected

    @given(
        datetime=zoned_datetimes(time_zone=timezones()),
        datetimes=pairs(zoned_datetimes(time_zone=timezones()), sorted=True),
    )
    def test_contain_datetime(
        self, *, datetime: dt.datetime, datetimes: tuple[dt.datetime, dt.datetime]
    ) -> None:
        start, end = datetimes
        period = Period(start, end)
        result = datetime in period
        expected = start <= datetime <= end
        assert result is expected

    @given(dates=pairs(dates(), sorted=True))
    def test_dates(self, *, dates: tuple[dt.date, dt.date]) -> None:
        start, end = dates
        _ = Period(start, end)

    @given(datetimes=pairs(zoned_datetimes(time_zone=timezones()), sorted=True))
    def test_datetimes(self, *, datetimes: tuple[dt.datetime, dt.datetime]) -> None:
        start, end = datetimes
        _ = Period(start, end)

    @given(dates=pairs(dates(), sorted=True))
    def test_duration(self, *, dates: tuple[dt.date, dt.date]) -> None:
        start, end = dates
        period = Period(start, end)
        assert period.duration == (end - start)

    @given(dates=pairs(dates(), sorted=True))
    def test_hashable(self, *, dates: tuple[dt.date, dt.date]) -> None:
        start, end = dates
        period = Period(start, end)
        _ = hash(period)

    @given(
        case=tuples(dates(), just("date")) | tuples(zoned_datetimes(), just("datetime"))
    )
    def test_kind(self, *, case: tuple[dt.date, _DateOrDateTime]) -> None:
        date_or_datetime, kind = case
        period = Period(date_or_datetime, date_or_datetime)
        assert period.kind == kind

    @given(dates=pairs(dates(), sorted=True), func=sampled_from([repr, str]))
    def test_repr_date(
        self, *, dates: tuple[dt.date, dt.date], func: Callable[..., str]
    ) -> None:
        start, end = dates
        period = Period(start, end)
        result = func(period)
        assert search(r"^Period\(\d{4}-\d{2}-\d{2}, \d{4}-\d{2}-\d{2}\)$", result)

    @given(data=data(), time_zone=timezones(), func=sampled_from([repr, str]))
    def test_repr_datetime_same_time_zone(
        self, *, data: DataObject, time_zone: ZoneInfo, func: Callable[..., str]
    ) -> None:
        datetimes = data.draw(pairs(zoned_datetimes(time_zone=time_zone), sorted=True))
        start, end = datetimes
        period = Period(start, end)
        with assume_does_not_raise(SerializeZonedDateTimeError):
            result = func(period)
        assert search(
            r"^Period\(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?, \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?, .+\)$",
            result,
        )

    @given(
        datetimes=pairs(zoned_datetimes(time_zone=timezones()), sorted=True),
        time_zones=pairs(timezones(), unique=True),
        func=sampled_from([repr, str]),
    )
    def test_repr_datetime_different_time_zone(
        self,
        *,
        datetimes: tuple[dt.datetime, dt.datetime],
        time_zones: tuple[ZoneInfo, ZoneInfo],
        func: Callable[..., str],
    ) -> None:
        start, end = datetimes
        time_zone1, time_zone2 = time_zones
        with assume_does_not_raise(OverflowError, match="date value out of range"):
            period = Period(start.astimezone(time_zone1), end.astimezone(time_zone2))
        with assume_does_not_raise(SerializeZonedDateTimeError):
            result = func(period)
        assert search(
            r"^Period\(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?[\+-]\d{2}:\d{2}(:\d{2})?\[.+\], \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?[\+-]\d{2}:\d{2}(:\d{2})?\[.+\]\)$",
            result,
        )

    @given(dates=pairs(dates(), sorted=True), extra=booleans())
    def test_repr_sub_classes(
        self, *, dates: tuple[dt.date, dt.date], extra: bool
    ) -> None:
        start, end = dates

        @dataclass
        class SubPeriod(Period[_TPeriod]):
            extra: bool

        period = SubPeriod(start, end, extra)
        result = repr(period)
        assert search(
            r"SubPeriod\(start=datetime\.date\(\d{1,4}, \d{1,2}, \d{1,2}\), end=datetime\.date\(\d{1,4}, \d{1,2}, \d{1,2}\), extra=(?:True|False)\)$",
            result,
        )

    @given(dates1=pairs(dates(), sorted=True), dates2=pairs(dates(), sorted=True))
    def test_sortable(
        self, *, dates1: tuple[dt.date, dt.date], dates2: tuple[dt.date, dt.date]
    ) -> None:
        start1, end1 = dates1
        start2, end2 = dates2
        period1 = Period(start1, end1)
        period2 = Period(start2, end2)
        _ = sorted([period1, period2])

    @given(dates=pairs(dates(), sorted=True), duration=timedeltas())
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_sub(
        self, *, dates: tuple[dt.date, dt.date], duration: dt.timedelta
    ) -> None:
        start, end = dates
        with assume_does_not_raise(OverflowError):
            adj_start, adj_end = start - duration, end - duration
        period = Period(start, end)
        result = period - duration
        expected = Period(adj_start, adj_end)
        assert result == expected

    @given(data=data(), time_zone=timezones())
    def test_time_zone(self, *, data: DataObject, time_zone: ZoneInfo) -> None:
        datetimes = data.draw(pairs(zoned_datetimes(time_zone=time_zone), sorted=True))
        start, end = datetimes
        period = Period(start, end)
        assert period.time_zone is time_zone

    @given(dates=pairs(dates(), sorted=True))
    def test_to_dict(self, *, dates: tuple[dt.date, dt.date]) -> None:
        start, end = dates
        period = Period(start, end)
        result = period.to_dict()
        expected = {"start": start, "end": end}
        assert result == expected

    @given(dates=pairs(dates(), sorted=True), time_zone=timezones())
    def test_error_as_time_zone_inapplicable(
        self, *, dates: tuple[dt.date, dt.date], time_zone: ZoneInfo
    ) -> None:
        start, end = dates
        period = Period(start, end)
        with raises(
            _PeriodAsTimeZoneInapplicableError,
            match="Period of dates does not have a timezone attribute",
        ):
            _ = period.astimezone(time_zone)

    @given(data=data(), date=dates(), datetime=zoned_datetimes(time_zone=timezones()))
    def test_error_date_and_datetime_mix(
        self, *, data: DataObject, date: dt.date, datetime: dt.datetime
    ) -> None:
        start, end = data.draw(permutations([date, datetime]))
        with raises(
            _PeriodDateAndDateTimeMixedError,
            match=r"Invalid period; got date and datetime mix \(.*, .*\)",
        ):
            _ = Period(start, end)

    @given(datetimes=pairs(datetimes() | zoned_datetimes(time_zone=timezones())))
    def test_error_naive_datetime(
        self, *, datetimes: tuple[dt.datetime, dt.datetime]
    ) -> None:
        start, end = datetimes
        _ = assume((start.tzinfo is None) or (end.tzinfo is None))
        with raises(
            _PeriodNaiveDateTimeError,
            match=r"Invalid period; got naive datetime\(s\) \(.*, .*\)",
        ):
            _ = Period(start, end)

    @given(dates=pairs(dates(), unique=True, sorted=True))
    def test_error_invalid_dates(self, *, dates: tuple[dt.date, dt.date]) -> None:
        start, end = dates
        with raises(_PeriodInvalidError, match="Invalid period; got .* > .*"):
            _ = Period(end, start)

    @given(datetimes=pairs(zoned_datetimes(), unique=True, sorted=True))
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_error_invalid_datetimes(
        self, *, datetimes: tuple[dt.datetime, dt.datetime]
    ) -> None:
        start, end = datetimes
        with raises(_PeriodInvalidError, match="Invalid period; got .* > .*"):
            _ = Period(end, start)

    @given(dates=pairs(dates(), sorted=True), duration=timedeltas(min_value=ZERO_TIME))
    def test_error_req_duration(
        self, *, dates: tuple[dt.date, dt.date], duration: dt.timedelta
    ) -> None:
        start, end = dates
        _ = assume(end - start != duration)
        with raises(
            _PeriodReqDurationError, match="Period must have duration .*; got .*"
        ):
            _ = Period(start, end, req_duration=duration)

    @given(
        dates=pairs(dates(), sorted=True), min_duration=timedeltas(min_value=ZERO_TIME)
    )
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_error_min_duration(
        self, *, dates: tuple[dt.date, dt.date], min_duration: dt.timedelta
    ) -> None:
        start, end = dates
        _ = assume(end - start < min_duration)
        with raises(
            _PeriodMinDurationError, match="Period must have min duration .*; got .*"
        ):
            _ = Period(start, end, min_duration=min_duration)

    @given(
        dates=pairs(dates(), sorted=True), max_duration=timedeltas(max_value=ZERO_TIME)
    )
    def test_error_max_duration(
        self, *, dates: tuple[dt.date, dt.date], max_duration: dt.timedelta
    ) -> None:
        start, end = dates
        _ = assume(end - start > max_duration)
        with raises(
            _PeriodMaxDurationError,
            match="Period must have duration at most .*; got .*",
        ):
            _ = Period(start, end, max_duration=max_duration)

    @given(
        datetime=zoned_datetimes(time_zone=timezones()),
        dates=pairs(dates(), sorted=True),
    )
    def test_error_date_contains_datetime(
        self, *, datetime: dt.datetime, dates: tuple[dt.date, dt.date]
    ) -> None:
        start, end = dates
        period = Period(start, end)
        with raises(
            _PeriodDateContainsDateTimeError,
            match="Period of dates cannot contain datetimes",
        ):
            _ = datetime in period

    @given(
        date=dates(),
        datetimes=pairs(zoned_datetimes(time_zone=timezones()), sorted=True),
    )
    def test_error_datetime_contains_date(
        self, *, date: dt.datetime, datetimes: tuple[dt.date, dt.date]
    ) -> None:
        start, end = datetimes
        period = Period(start, end)
        with raises(
            _PeriodDateTimeContainsDateError,
            match="Period of datetimes cannot contain dates",
        ):
            _ = date in period

    @given(dates=pairs(dates(), sorted=True))
    def test_error_time_zone_inapplicable(
        self, *, dates: tuple[dt.date, dt.date]
    ) -> None:
        start, end = dates
        period = Period(start, end)
        with raises(
            _PeriodTimeZoneInapplicableError,
            match="Period of dates does not have a timezone attribute",
        ):
            _ = period.time_zone

    @given(
        datetimes=pairs(zoned_datetimes(time_zone=timezones()), sorted=True),
        time_zones=pairs(timezones(), unique=True),
    )
    def test_error_time_zone_non_unique(
        self,
        *,
        datetimes: tuple[dt.datetime, dt.datetime],
        time_zones: tuple[ZoneInfo, ZoneInfo],
    ) -> None:
        start, end = datetimes
        time_zone1, time_zone2 = time_zones
        with assume_does_not_raise(OverflowError, match="date value out of range"):
            period = Period(start.astimezone(time_zone1), end.astimezone(time_zone2))
        with raises(
            _PeriodTimeZoneNonUniqueError,
            match="Period must contain exactly one time zone; got .* and .*",
        ):
            _ = period.time_zone
