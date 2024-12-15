from __future__ import annotations

import datetime as dt
from re import search
from typing import TYPE_CHECKING

from hypothesis import assume, given
from hypothesis.strategies import (
    DataObject,
    data,
    dates,
    datetimes,
    just,
    none,
    permutations,
    sampled_from,
    sets,
    timedeltas,
    timezones,
    tuples,
)
from pytest import raises

from utilities.datetime import ZERO_TIME
from utilities.hypothesis import assume_does_not_raise, zoned_datetimes
from utilities.period import (
    Period,
    _DateOrDatetime,
    _PeriodAsTimeZoneInapplicableError,
    _PeriodDateAndDatetimeMixedError,
    _PeriodInvalidError,
    _PeriodMaxDurationError,
    _PeriodMinDurationError,
    _PeriodNaiveDatetimeError,
    _PeriodReqDurationError,
    _PeriodTimeZoneInapplicableError,
    _PeriodTimeZoneNonUniqueError,
)
from utilities.zoneinfo import UTC, HongKong

if TYPE_CHECKING:
    from collections.abc import Callable
    from zoneinfo import ZoneInfo


class TestPeriod:
    @given(dates=tuples(dates(), dates()), duration=timedeltas())
    def test_add(
        self, *, dates: tuple[dt.date, dt.date], duration: dt.timedelta
    ) -> None:
        start, end = sorted(dates)
        with assume_does_not_raise(OverflowError):
            adj_start, adj_end = start + duration, end + duration
        period = Period(start, end)
        result = period + duration
        expected = Period(adj_start, adj_end)
        assert result == expected

    @given(
        datetimes=tuples(
            zoned_datetimes(time_zone=timezones()),
            zoned_datetimes(time_zone=timezones()),
        ),
        time_zone=timezones(),
    )
    def test_astimezone(
        self, *, datetimes: tuple[dt.datetime, dt.datetime], time_zone: ZoneInfo
    ) -> None:
        start, end = sorted(datetimes)
        with assume_does_not_raise(OverflowError):
            adj_start, adj_end = (
                start.astimezone(time_zone),
                end.astimezone(time_zone),
            )
        period = Period(start, end)
        result = period.astimezone(time_zone)
        expected = Period(adj_start, adj_end)
        assert result == expected

    @given(dates=tuples(dates(), dates()))
    def test_dates(self, *, dates: tuple[dt.date, dt.date]) -> None:
        start, end = sorted(dates)
        _ = Period(start, end)

    @given(
        datetimes=tuples(
            zoned_datetimes(time_zone=timezones()),
            zoned_datetimes(time_zone=timezones()),
        )
    )
    def test_datetimes(self, *, datetimes: tuple[dt.datetime, dt.datetime]) -> None:
        start, end = sorted(datetimes)
        _ = Period(start, end)

    @given(dates=tuples(dates(), dates()))
    def test_duration(self, *, dates: tuple[dt.date, dt.date]) -> None:
        start, end = sorted(dates)
        period = Period(start, end)
        assert period.duration == (end - start)

    @given(dates=tuples(dates(), dates()))
    def test_hashable(self, *, dates: tuple[dt.date, dt.date]) -> None:
        start, end = sorted(dates)
        period = Period(start, end)
        _ = hash(period)

    @given(
        case=tuples(dates(), just("date")) | tuples(zoned_datetimes(), just("datetime"))
    )
    def test_kind(self, *, case: tuple[dt.date, _DateOrDatetime]) -> None:
        date_or_datetime, kind = case
        period = Period(date_or_datetime, date_or_datetime)
        assert period.kind == kind

    @given(dates=tuples(dates(), dates()), func=sampled_from([repr, str]))
    def test_repr_date(
        self, *, dates: tuple[dt.date, dt.date], func: Callable[..., str]
    ) -> None:
        start, end = sorted(dates)
        period = Period(start, end)
        result = func(period)
        assert search(r"^Period\(\d{4}-\d{2}-\d{2}, \d{4}-\d{2}-\d{2}\)", result)

    @given(data=data(), time_zone=timezones(), func=sampled_from([repr, str]))
    def test_repr_datetime_same_time_zone(
        self, *, data: DataObject, time_zone: ZoneInfo, func: Callable[..., str]
    ) -> None:
        datetimes = data.draw(
            tuples(
                zoned_datetimes(time_zone=time_zone),
                zoned_datetimes(time_zone=time_zone),
            )
        )
        start, end = sorted(datetimes)
        period = Period(start, end)
        result = func(period)
        assert search(
            r"^Period\(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?, \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?, .+\)",
            result,
        )

    @given(
        datetimes=tuples(
            zoned_datetimes(time_zone=timezones()),
            zoned_datetimes(time_zone=timezones()),
        ),
        time_zones=sets(timezones(), min_size=2, max_size=2),
        func=sampled_from([repr, str]),
    )
    def test_repr_datetime_different_time_zone(
        self,
        *,
        datetimes: tuple[dt.datetime, dt.datetime],
        time_zones: set[ZoneInfo],
        func: Callable[..., str],
    ) -> None:
        start, end = sorted(datetimes)
        time_zone1, time_zone2 = time_zones
        period = Period(start.astimezone(time_zone1), end.astimezone(time_zone2))
        result = func(period)
        assert search(
            r"^Period\(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?[\+-]\d{2}:\d{2}(:\d{2})?\[.+\], \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?[\+-]\d{2}:\d{2}(:\d{2})?\[.+\]\)",
            result,
        )

    @given(dates1=tuples(dates(), dates()), dates2=tuples(dates(), dates()))
    def test_sortable(
        self, *, dates1: tuple[dt.date, dt.date], dates2: tuple[dt.date, dt.date]
    ) -> None:
        start1, end1 = sorted(dates1)
        start2, end2 = sorted(dates2)
        period1 = Period(start1, end1)
        period2 = Period(start2, end2)
        _ = sorted([period1, period2])

    @given(dates=tuples(dates(), dates()), duration=timedeltas())
    def test_sub(
        self, *, dates: tuple[dt.date, dt.date], duration: dt.timedelta
    ) -> None:
        start, end = sorted(dates)
        with assume_does_not_raise(OverflowError):
            adj_start, adj_end = start - duration, end - duration
        period = Period(start, end)
        result = period - duration
        expected = Period(adj_start, adj_end)
        assert result == expected

    @given(data=data(), time_zone=timezones())
    def test_time_zone(self, *, data: DataObject, time_zone: ZoneInfo) -> None:
        datetimes = data.draw(
            tuples(
                zoned_datetimes(time_zone=time_zone),
                zoned_datetimes(time_zone=time_zone),
            )
        )
        start, end = sorted(datetimes)
        period = Period(start, end)
        assert period.time_zone is time_zone

    @given(dates=tuples(dates(), dates()))
    def test_to_dict(self, *, dates: tuple[dt.date, dt.date]) -> None:
        start, end = sorted(dates)
        period = Period(start, end)
        result = period.to_dict()
        expected = {"start": start, "end": end}
        assert result == expected

    @given(dates=tuples(dates(), dates()), time_zone=timezones())
    def test_error_as_time_zone_inapplicable(
        self, *, dates: tuple[dt.date, dt.date], time_zone: ZoneInfo
    ) -> None:
        start, end = sorted(dates)
        period = Period(start, end)
        with raises(
            _PeriodAsTimeZoneInapplicableError,
            match="Period of dates does not have a timezone attribute",
        ):
            _ = period.astimezone(time_zone)

    @given(
        data=data(),
        date=dates(),
        datetime=datetimes(timezones=sampled_from([HongKong, UTC, dt.UTC]) | none()),
    )
    def test_error_date_and_datetime_mix(
        self, *, data: DataObject, date: dt.date, datetime: dt.datetime
    ) -> None:
        start, end = data.draw(permutations([date, datetime]))
        with raises(
            _PeriodDateAndDatetimeMixedError,
            match=r"Invalid period; got date and datetime mix \(.*, .*\)",
        ):
            _ = Period(start, end)

    @given(
        start=datetimes() | zoned_datetimes(time_zone=timezones()),
        end=datetimes() | zoned_datetimes(time_zone=timezones()),
    )
    def test_error_naive_datetime(
        self, *, start: dt.datetime, end: dt.datetime
    ) -> None:
        _ = assume((start.tzinfo is None) or (end.tzinfo is None))
        with raises(
            _PeriodNaiveDatetimeError,
            match=r"Invalid period; got naive datetime\(s\) \(.*, .*\)",
        ):
            _ = Period(start, end)

    @given(dates=tuples(dates(), dates()))
    def test_error_invalid_dates(self, *, dates: tuple[dt.date, dt.date]) -> None:
        start, end = sorted(dates)
        _ = assume(start != end)
        with raises(_PeriodInvalidError, match="Invalid period; got .* > .*"):
            _ = Period(end, start)

    @given(
        datetimes=tuples(
            zoned_datetimes(time_zone=timezones()),
            zoned_datetimes(time_zone=timezones()),
        )
    )
    def test_error_invalid_datetimes(
        self, *, datetimes: tuple[dt.datetime, dt.datetime]
    ) -> None:
        start, end = sorted(datetimes)
        _ = assume(start != end)
        with raises(_PeriodInvalidError, match="Invalid period; got .* > .*"):
            _ = Period(end, start)

    @given(dates=tuples(dates(), dates()), duration=timedeltas(min_value=ZERO_TIME))
    def test_error_req_duration(
        self, *, dates: tuple[dt.date, dt.date], duration: dt.timedelta
    ) -> None:
        start, end = sorted(dates)
        _ = assume(end - start != duration)
        with raises(
            _PeriodReqDurationError, match="Period must have duration .*; got .*"
        ):
            _ = Period(start, end, req_duration=duration)

    @given(dates=tuples(dates(), dates()), min_duration=timedeltas(min_value=ZERO_TIME))
    def test_error_min_duration(
        self, *, dates: tuple[dt.date, dt.date], min_duration: dt.timedelta
    ) -> None:
        start, end = sorted(dates)
        _ = assume(end - start < min_duration)
        with raises(
            _PeriodMinDurationError, match="Period must have min duration .*; got .*"
        ):
            _ = Period(start, end, min_duration=min_duration)

    @given(dates=tuples(dates(), dates()), max_duration=timedeltas(max_value=ZERO_TIME))
    def test_error_max_duration(
        self, *, dates: tuple[dt.date, dt.date], max_duration: dt.timedelta
    ) -> None:
        start, end = sorted(dates)
        _ = assume(end - start > max_duration)
        with raises(
            _PeriodMaxDurationError,
            match="Period must have duration at most .*; got .*",
        ):
            _ = Period(start, end, max_duration=max_duration)

    @given(dates=tuples(dates(), dates()))
    def test_error_time_zone_inapplicable(
        self, *, dates: tuple[dt.date, dt.date]
    ) -> None:
        start, end = sorted(dates)
        period = Period(start, end)
        with raises(
            _PeriodTimeZoneInapplicableError,
            match="Period of dates does not have a timezone attribute",
        ):
            _ = period.time_zone

    @given(
        datetimes=tuples(
            zoned_datetimes(time_zone=timezones()),
            zoned_datetimes(time_zone=timezones()),
        ),
        time_zones=sets(timezones(), min_size=2, max_size=2),
    )
    def test_error_time_zone_non_unique(
        self, *, datetimes: tuple[dt.datetime, dt.datetime], time_zones: set[ZoneInfo]
    ) -> None:
        start, end = sorted(datetimes)
        time_zone1, time_zone2 = time_zones
        period = Period(start.astimezone(time_zone1), end.astimezone(time_zone2))
        with raises(
            _PeriodTimeZoneNonUniqueError,
            match="Period must contain exactly one time zone; got .* and .*",
        ):
            _ = period.time_zone
