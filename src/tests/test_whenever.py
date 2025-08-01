from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from logging import DEBUG
from re import search
from typing import TYPE_CHECKING, Any, ClassVar, Self, cast
from zoneinfo import ZoneInfo

from hypothesis import HealthCheck, given, settings
from hypothesis.strategies import (
    DataObject,
    data,
    integers,
    none,
    sampled_from,
    timezones,
)
from pytest import mark, param, raises
from whenever import (
    Date,
    DateDelta,
    DateTimeDelta,
    PlainDateTime,
    Time,
    TimeDelta,
    TimeZoneNotFoundError,
    Weekday,
    YearMonth,
    ZonedDateTime,
)

from utilities.dataclasses import replace_non_sentinel
from utilities.hypothesis import (
    assume_does_not_raise,
    date_deltas,
    dates,
    pairs,
    plain_datetimes,
    time_deltas,
    times,
    zoned_datetimes,
    zoned_datetimes_2000,
)
from utilities.sentinel import Sentinel, sentinel
from utilities.tzdata import HongKong, Tokyo, USCentral, USEastern
from utilities.tzlocal import LOCAL_TIME_ZONE_NAME
from utilities.whenever import (
    DATE_DELTA_MAX,
    DATE_DELTA_MIN,
    DATE_DELTA_PARSABLE_MAX,
    DATE_DELTA_PARSABLE_MIN,
    DATE_TIME_DELTA_MAX,
    DATE_TIME_DELTA_MIN,
    DATE_TIME_DELTA_PARSABLE_MAX,
    DATE_TIME_DELTA_PARSABLE_MIN,
    DAY,
    MICROSECOND,
    MINUTE,
    MONTH,
    NOW_LOCAL,
    NOW_UTC,
    SECOND,
    TIME_DELTA_MAX,
    TIME_DELTA_MIN,
    TODAY_LOCAL,
    TODAY_UTC,
    ZERO_DAYS,
    ZONED_DATE_TIME_MAX,
    ZONED_DATE_TIME_MIN,
    DatePeriod,
    DatePeriodError,
    MeanDateTimeError,
    MinMaxDateError,
    PeriodDict,
    TimePeriod,
    ToMonthsAndDaysError,
    ToNanosecondsError,
    ToPyTimeDeltaError,
    ToZonedDateTimeError,
    WheneverLogRecord,
    ZonedDateTimePeriod,
    _MinMaxDatePeriodError,
    _RoundDateOrDateTimeDateTimeIntraDayWithWeekdayError,
    _RoundDateOrDateTimeDateWithIntradayDeltaError,
    _RoundDateOrDateTimeDateWithWeekdayError,
    _RoundDateOrDateTimeIncrementError,
    _RoundDateOrDateTimeInvalidDurationError,
    _ToDaysMonthsError,
    _ToDaysNanosecondsError,
    _ToHoursMonthsError,
    _ToHoursNanosecondsError,
    _ToMicrosecondsMonthsError,
    _ToMicrosecondsNanosecondsError,
    _ToMillisecondsMonthsError,
    _ToMillisecondsNanosecondsError,
    _ToMinutesMonthsError,
    _ToMinutesNanosecondsError,
    _ToMonthsDaysError,
    _ToMonthsTimeError,
    _ToSecondsMonthsError,
    _ToSecondsNanosecondsError,
    _ToWeeksDaysError,
    _ToWeeksMonthsError,
    _ToWeeksNanosecondsError,
    _ToYearsDaysError,
    _ToYearsMonthsError,
    _ToYearsTimeError,
    _ZonedDateTimePeriodExactEqError,
    _ZonedDateTimePeriodInvalidError,
    _ZonedDateTimePeriodTimeZoneError,
    add_year_month,
    datetime_utc,
    diff_year_month,
    format_compact,
    from_timestamp,
    from_timestamp_millis,
    from_timestamp_nanos,
    get_now,
    get_now_local,
    get_today,
    get_today_local,
    mean_datetime,
    min_max_date,
    round_date_or_date_time,
    sub_year_month,
    to_date,
    to_date_time_delta,
    to_days,
    to_hours,
    to_local_plain,
    to_microseconds,
    to_milliseconds,
    to_minutes,
    to_months,
    to_months_and_days,
    to_nanoseconds,
    to_py_date_or_date_time,
    to_py_time_delta,
    to_seconds,
    to_time_delta,
    to_weeks,
    to_years,
    to_zoned_date_time,
    two_digit_year_month,
)
from utilities.zoneinfo import UTC, get_time_zone_name

if TYPE_CHECKING:
    from collections.abc import Callable

    from _pytest.mark import ParameterSet

    from utilities.sentinel import Sentinel
    from utilities.types import (
        DateOrDateTimeDelta,
        DateTimeRoundMode,
        Delta,
        MaybeCallableDateLike,
        MaybeCallableZonedDateTimeLike,
        TimeOrDateTimeDelta,
    )


class TestAddAndSubYearMonth:
    x: ClassVar[YearMonth] = YearMonth(2005, 7)
    cases: ClassVar[list[tuple[int, int, YearMonth, YearMonth]]] = [
        (1, 0, YearMonth(2006, 7), YearMonth(2004, 7)),
        (0, 11, YearMonth(2006, 6), YearMonth(2004, 8)),
        (0, 6, YearMonth(2006, 1), YearMonth(2005, 1)),
        (0, 2, YearMonth(2005, 9), YearMonth(2005, 5)),
        (0, 1, YearMonth(2005, 8), YearMonth(2005, 6)),
        (0, 0, YearMonth(2005, 7), YearMonth(2005, 7)),
        (0, -1, YearMonth(2005, 6), YearMonth(2005, 8)),
        (0, -2, YearMonth(2005, 5), YearMonth(2005, 9)),
        (0, -6, YearMonth(2005, 1), YearMonth(2006, 1)),
        (0, -11, YearMonth(2004, 8), YearMonth(2006, 6)),
        (-1, 0, YearMonth(2004, 7), YearMonth(2006, 7)),
    ]

    @mark.parametrize(
        ("years", "months", "expected"), [param(y, m, e) for y, m, e, _ in cases]
    )
    def test_add(self, *, years: int, months: int, expected: YearMonth) -> None:
        result = add_year_month(self.x, years=years, months=months)
        assert result == expected

    @mark.parametrize(
        ("years", "months", "expected"), [param(y, m, e) for y, m, _, e in cases]
    )
    def test_sub(self, *, years: int, months: int, expected: YearMonth) -> None:
        result = sub_year_month(self.x, years=years, months=months)
        assert result == expected


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

    @given(dates=pairs(dates(), sorted=True), time=times())
    def test_at_time(self, *, dates: tuple[Date, Date], time: Time) -> None:
        start, end = dates
        period = DatePeriod(start, end)
        result = period.at(time)
        expected = ZonedDateTimePeriod(
            start.at(time).assume_tz(UTC.key), end.at(time).assume_tz(UTC.key)
        )
        assert result.exact_eq(expected)

    @given(dates=pairs(dates(), sorted=True), times=pairs(times()))
    def test_at_times(
        self, *, dates: tuple[Date, Date], times: tuple[Time, Time]
    ) -> None:
        start_date, end_date = dates
        period = DatePeriod(start_date, end_date)
        start_time, end_time = times
        with assume_does_not_raise(_ZonedDateTimePeriodInvalidError):
            result = period.at((start_time, end_time))
        expected = ZonedDateTimePeriod(
            start_date.at(start_time).assume_tz(UTC.key),
            end_date.at(end_time).assume_tz(UTC.key),
        )
        assert result.exact_eq(expected)

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
    def test_replace(self, *, dates: tuple[Date, Date]) -> None:
        start, end = dates
        period = DatePeriod(start, end)
        new = period.replace(start=start - DAY, end=end + DAY)
        assert new.start == (start - DAY)
        assert new.end == (end + DAY)

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

    @given(data=data(), dates=pairs(dates(), sorted=True))
    def test_to_and_from_dict(
        self, *, data: DataObject, dates: tuple[Date, Date]
    ) -> None:
        start, end = dates
        period = DatePeriod(start, end)
        dict1 = period.to_dict()
        dstart, dend = dict1["start"], dict1["end"]
        dict2 = PeriodDict(
            start=data.draw(sampled_from([dstart, dstart.py_date()])),
            end=data.draw(sampled_from([dend, dend.py_date()])),
        )
        result = DatePeriod.from_dict(dict2)
        assert result == period

    @given(dates=pairs(dates(), unique=True, sorted=True))
    def test_error_period_invalid(self, *, dates: tuple[Date, Date]) -> None:
        start, end = dates
        with raises(DatePeriodError, match="Invalid period; got .* > .*"):
            _ = DatePeriod(end, start)


class TestDatetimeUTC:
    @given(datetime=zoned_datetimes())
    def test_main(self, *, datetime: ZonedDateTime) -> None:
        result = datetime_utc(
            datetime.year,
            datetime.month,
            datetime.day,
            hour=datetime.hour,
            minute=datetime.minute,
            second=datetime.second,
            nanosecond=datetime.nanosecond,
        )
        assert result == datetime


class TestDiffYearMonth:
    x: ClassVar[YearMonth] = YearMonth(2005, 7)
    cases: ClassVar[list[ParameterSet]] = [
        param(YearMonth(2004, 7), 1, 0),
        param(YearMonth(2004, 8), 0, 11),
        param(YearMonth(2005, 1), 0, 6),
        param(YearMonth(2005, 5), 0, 2),
        param(YearMonth(2005, 6), 0, 1),
        param(YearMonth(2005, 7), 0, 0),
        param(YearMonth(2005, 8), 0, -1),
        param(YearMonth(2005, 9), 0, -2),
        param(YearMonth(2006, 1), 0, -6),
        param(YearMonth(2006, 6), 0, -11),
        param(YearMonth(2006, 7), -1, 0),
    ]

    @mark.parametrize(("y", "year", "month"), cases)
    def test_main(self, *, y: YearMonth, year: int, month: int) -> None:
        result = diff_year_month(self.x, y)
        expected = 12 * year + month
        assert result == expected

    @mark.parametrize(("y", "year", "month"), cases)
    def test_year_and_month(self, *, y: YearMonth, year: int, month: int) -> None:
        result = diff_year_month(self.x, y, years=True)
        expected = (year, month)
        assert result == expected


class TestFormatCompact:
    @given(date=dates())
    def test_date(self, *, date: Date) -> None:
        result = format_compact(date)
        assert isinstance(result, str)
        parsed = Date.parse_common_iso(result)
        assert parsed == date

    @given(time=times())
    def test_time(self, *, time: Time) -> None:
        result = format_compact(time)
        assert isinstance(result, str)
        parsed = Time.parse_common_iso(result)
        assert parsed.nanosecond == 0
        expected = time.round()
        assert parsed == expected

    @given(datetime=plain_datetimes())
    def test_plain_datetime(self, *, datetime: PlainDateTime) -> None:
        result = format_compact(datetime)
        assert isinstance(result, str)
        parsed = PlainDateTime.parse_common_iso(result)
        assert parsed.nanosecond == 0
        expected = datetime.round()
        assert parsed == expected

    @given(datetime=zoned_datetimes())
    def test_zoned_datetime(self, *, datetime: ZonedDateTime) -> None:
        result = format_compact(datetime)
        assert isinstance(result, str)
        parsed = ZonedDateTime.parse_common_iso(result)
        assert parsed.nanosecond == 0
        expected = datetime.round()
        assert parsed == expected


class TestFromTimeStamp:
    @given(
        datetime=zoned_datetimes(time_zone=timezones()).map(lambda d: d.round("second"))
    )
    def test_main(self, *, datetime: ZonedDateTime) -> None:
        timestamp = datetime.timestamp()
        result = from_timestamp(timestamp, time_zone=ZoneInfo(datetime.tz))
        assert result == datetime

    @given(
        datetime=zoned_datetimes(time_zone=timezones()).map(
            lambda d: d.round("millisecond")
        )
    )
    def test_millis(self, *, datetime: ZonedDateTime) -> None:
        timestamp = datetime.timestamp_millis()
        result = from_timestamp_millis(timestamp, time_zone=ZoneInfo(datetime.tz))
        assert result == datetime

    @given(datetime=zoned_datetimes(time_zone=timezones()))
    def test_nanos(self, *, datetime: ZonedDateTime) -> None:
        timestamp = datetime.timestamp_nanos()
        result = from_timestamp_nanos(timestamp, time_zone=ZoneInfo(datetime.tz))
        assert result == datetime


class TestGetNow:
    @given(time_zone=timezones())
    def test_function(self, *, time_zone: ZoneInfo) -> None:
        with assume_does_not_raise(TimeZoneNotFoundError):
            now = get_now(time_zone)
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

    def test_constant(self) -> None:
        assert isinstance(NOW_LOCAL, ZonedDateTime)
        assert NOW_LOCAL.tz == LOCAL_TIME_ZONE_NAME


class TestGetToday:
    def test_function(self) -> None:
        today = get_today()
        assert isinstance(today, Date)

    def test_constant(self) -> None:
        assert isinstance(TODAY_UTC, Date)


class TestGetTodayLocal:
    def test_function(self) -> None:
        today = get_today_local()
        assert isinstance(today, Date)

    def test_constant(self) -> None:
        assert isinstance(TODAY_LOCAL, Date)


class TestMeanDateTime:
    threshold: ClassVar[TimeDelta] = 100 * MICROSECOND

    @given(datetime=zoned_datetimes())
    def test_one(self, *, datetime: ZonedDateTime) -> None:
        result = mean_datetime([datetime])
        assert result == datetime

    @given(datetime=zoned_datetimes())
    def test_many(self, *, datetime: ZonedDateTime) -> None:
        result = mean_datetime([datetime, datetime + MINUTE])
        expected = datetime + 30 * SECOND
        assert abs(result - expected) <= self.threshold

    @given(datetime=zoned_datetimes())
    def test_weights(self, *, datetime: ZonedDateTime) -> None:
        result = mean_datetime([datetime, datetime + MINUTE], weights=[1, 3])
        expected = datetime + 45 * SECOND
        assert abs(result - expected) <= self.threshold

    def test_error(self) -> None:
        with raises(MeanDateTimeError, match="Mean requires at least 1 datetime"):
            _ = mean_datetime([])


class TestMinMax:
    def test_date_delta_min(self) -> None:
        with raises(ValueError, match="Addition result out of bounds"):
            _ = DATE_DELTA_MIN - DateDelta(days=1)

    def test_date_delta_max(self) -> None:
        with raises(ValueError, match="Addition result out of bounds"):
            _ = DATE_DELTA_MAX + DateDelta(days=1)

    def test_date_delta_parsable_min(self) -> None:
        self._format_parse_date_delta(DATE_DELTA_PARSABLE_MIN)
        with raises(ValueError, match="Invalid format: '.*'"):
            self._format_parse_date_delta(DATE_DELTA_PARSABLE_MIN - DateDelta(days=1))

    def test_date_delta_parsable_max(self) -> None:
        self._format_parse_date_delta(DATE_DELTA_PARSABLE_MAX)
        with raises(ValueError, match="Invalid format: '.*'"):
            self._format_parse_date_delta(DATE_DELTA_PARSABLE_MAX + DateDelta(days=1))

    def test_date_time_delta_min(self) -> None:
        nanos = to_nanoseconds(DATE_TIME_DELTA_MIN)
        with raises(ValueError, match="Out of range"):
            _ = to_date_time_delta(nanos - 1)

    def test_date_time_delta_max(self) -> None:
        nanos = to_nanoseconds(DATE_TIME_DELTA_MAX)
        with raises(ValueError, match="Out of range"):
            _ = to_date_time_delta(nanos + 1)

    def test_date_time_delta_parsable_min(self) -> None:
        self._format_parse_date_time_delta(DATE_TIME_DELTA_PARSABLE_MIN)
        nanos = to_nanoseconds(DATE_TIME_DELTA_PARSABLE_MIN)
        with raises(ValueError, match="Invalid format or out of range: '.*'"):
            self._format_parse_date_time_delta(to_date_time_delta(nanos - 1))

    def test_date_time_delta_parsable_max(self) -> None:
        self._format_parse_date_time_delta(DATE_TIME_DELTA_PARSABLE_MAX)
        nanos = to_nanoseconds(DATE_TIME_DELTA_PARSABLE_MAX)
        with raises(ValueError, match="Invalid format or out of range: '.*'"):
            _ = self._format_parse_date_time_delta(to_date_time_delta(nanos + 1))

    def test_plain_date_time_min(self) -> None:
        with raises(ValueError, match=r"Result of subtract\(\) out of range"):
            _ = PlainDateTime.MIN.subtract(nanoseconds=1, ignore_dst=True)

    def test_plain_date_time_max(self) -> None:
        with raises(ValueError, match=r"Result of add\(\) out of range"):
            _ = PlainDateTime.MAX.add(microseconds=1, ignore_dst=True)

    def test_time_delta_min(self) -> None:
        nanos = TIME_DELTA_MIN.in_nanoseconds()
        with raises(ValueError, match="TimeDelta out of range"):
            _ = to_time_delta(nanos - 1)

    def test_time_delta_max(self) -> None:
        nanos = TIME_DELTA_MAX.in_nanoseconds()
        with raises(ValueError, match="TimeDelta out of range"):
            _ = to_time_delta(nanos + 1)

    def test_zoned_date_time_min(self) -> None:
        with raises(ValueError, match="Instant is out of range"):
            _ = ZONED_DATE_TIME_MIN.subtract(nanoseconds=1)

    def test_zoned_date_time_max(self) -> None:
        with raises(ValueError, match="Instant is out of range"):
            _ = ZONED_DATE_TIME_MAX.add(microseconds=1)

    def _format_parse_date_delta(self, delta: DateDelta, /) -> None:
        _ = DateDelta.parse_common_iso(delta.format_common_iso())

    def _format_parse_date_time_delta(self, delta: DateTimeDelta, /) -> None:
        _ = DateTimeDelta.parse_common_iso(delta.format_common_iso())


class TestMinMaxDate:
    @given(
        min_date=dates(max_value=TODAY_LOCAL) | none(),
        max_date=dates(max_value=TODAY_LOCAL) | none(),
        min_age=date_deltas(min_value=ZERO_DAYS) | none(),
        max_age=date_deltas(min_value=ZERO_DAYS) | none(),
    )
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_main(
        self,
        *,
        min_date: Date | None,
        max_date: Date | None,
        min_age: DateDelta | None,
        max_age: DateDelta | None,
    ) -> None:
        with (
            assume_does_not_raise(MinMaxDateError),
            assume_does_not_raise(ValueError, match="Resulting date out of range"),
        ):
            min_date_use, max_date_use = min_max_date(
                min_date=min_date, max_date=max_date, min_age=min_age, max_age=max_age
            )
        if (min_date is None) and (max_age is None):
            assert min_date_use is None
        else:
            assert min_date_use is not None
        if (max_date is None) and (min_age is None):
            assert max_date_use is None
        else:
            assert max_date_use is not None
        if min_date_use is not None:
            assert min_date_use <= get_today()
        if max_date_use is not None:
            assert max_date_use <= get_today()
        if (min_date_use is not None) and (max_date_use is not None):
            assert min_date_use <= max_date_use

    @given(dates=pairs(dates(max_value=TODAY_UTC), unique=True, sorted=True))
    def test_error_period(self, *, dates: tuple[Date, Date]) -> None:
        with raises(
            _MinMaxDatePeriodError,
            match="Min date must be at most max date; got .* > .*",
        ):
            _ = min_max_date(min_date=dates[1], max_date=dates[0])


class TestRoundDateOrDateTime:
    @mark.parametrize(
        ("date", "delta", "mode", "expected"),
        [
            param(Date(2000, 1, 1), DateDelta(days=1), "half_even", Date(2000, 1, 1)),
            param(Date(2000, 1, 1), DateDelta(days=2), "half_even", Date(2000, 1, 2)),
            param(Date(2000, 1, 1), DateDelta(days=2), "ceil", Date(2000, 1, 2)),
            param(Date(2000, 1, 1), DateDelta(days=2), "floor", Date(1999, 12, 31)),
            param(Date(2000, 1, 1), DateDelta(days=2), "half_ceil", Date(2000, 1, 2)),
            param(
                Date(2000, 1, 1), DateDelta(days=2), "half_floor", Date(1999, 12, 31)
            ),
            param(Date(2000, 1, 2), DateDelta(days=2), "half_even", Date(2000, 1, 2)),
            param(Date(2000, 1, 2), DateDelta(days=2), "ceil", Date(2000, 1, 2)),
            param(Date(2000, 1, 2), DateDelta(days=2), "floor", Date(2000, 1, 2)),
            param(Date(2000, 1, 2), DateDelta(days=2), "half_ceil", Date(2000, 1, 2)),
            param(Date(2000, 1, 2), DateDelta(days=2), "half_floor", Date(2000, 1, 2)),
            param(Date(2000, 1, 1), DateDelta(days=3), "half_even", Date(2000, 1, 1)),
            param(Date(2000, 1, 1), DateDelta(days=3), "ceil", Date(2000, 1, 1)),
            param(Date(2000, 1, 1), DateDelta(days=3), "floor", Date(2000, 1, 1)),
            param(Date(2000, 1, 1), DateDelta(days=3), "half_ceil", Date(2000, 1, 1)),
            param(Date(2000, 1, 1), DateDelta(days=3), "half_floor", Date(2000, 1, 1)),
            param(Date(2000, 1, 2), DateDelta(days=3), "half_even", Date(2000, 1, 4)),
            param(Date(2000, 1, 2), DateDelta(days=3), "ceil", Date(2000, 1, 4)),
            param(Date(2000, 1, 2), DateDelta(days=3), "floor", Date(2000, 1, 1)),
            param(Date(2000, 1, 2), DateDelta(days=3), "half_ceil", Date(2000, 1, 4)),
            param(Date(2000, 1, 2), DateDelta(days=3), "half_floor", Date(2000, 1, 1)),
            param(Date(2000, 1, 3), DateDelta(days=3), "half_even", Date(2000, 1, 4)),
            param(Date(2000, 1, 3), DateDelta(days=3), "ceil", Date(2000, 1, 4)),
            param(Date(2000, 1, 3), DateDelta(days=3), "floor", Date(2000, 1, 1)),
            param(Date(2000, 1, 3), DateDelta(days=3), "half_ceil", Date(2000, 1, 4)),
            param(Date(2000, 1, 3), DateDelta(days=3), "half_floor", Date(2000, 1, 4)),
        ],
    )
    def test_date_daily(
        self,
        *,
        date: Date,
        delta: Delta,
        mode: DateTimeRoundMode,
        expected: ZonedDateTime,
    ) -> None:
        result = round_date_or_date_time(date, delta, mode=mode)
        assert result == expected

    @mark.parametrize(
        ("date", "weekday", "expected"),
        [
            param(Date(2000, 1, 1), None, Date(1999, 12, 27)),
            param(Date(2000, 1, 2), None, Date(1999, 12, 27)),
            param(Date(2000, 1, 3), None, Date(2000, 1, 3)),
            param(Date(2000, 1, 4), None, Date(2000, 1, 3)),
            param(Date(2000, 1, 5), None, Date(2000, 1, 3)),
            param(Date(2000, 1, 6), None, Date(2000, 1, 3)),
            param(Date(2000, 1, 7), None, Date(2000, 1, 3)),
            param(Date(2000, 1, 8), None, Date(2000, 1, 3)),
            param(Date(2000, 1, 9), None, Date(2000, 1, 3)),
            param(Date(2000, 1, 10), None, Date(2000, 1, 10)),
            param(Date(2000, 1, 11), None, Date(2000, 1, 10)),
            param(Date(2000, 1, 1), Weekday.WEDNESDAY, Date(1999, 12, 29)),
            param(Date(2000, 1, 2), Weekday.WEDNESDAY, Date(1999, 12, 29)),
            param(Date(2000, 1, 3), Weekday.WEDNESDAY, Date(1999, 12, 29)),
            param(Date(2000, 1, 4), Weekday.WEDNESDAY, Date(1999, 12, 29)),
            param(Date(2000, 1, 5), Weekday.WEDNESDAY, Date(2000, 1, 5)),
            param(Date(2000, 1, 6), Weekday.WEDNESDAY, Date(2000, 1, 5)),
            param(Date(2000, 1, 7), Weekday.WEDNESDAY, Date(2000, 1, 5)),
            param(Date(2000, 1, 8), Weekday.WEDNESDAY, Date(2000, 1, 5)),
            param(Date(2000, 1, 9), Weekday.WEDNESDAY, Date(2000, 1, 5)),
            param(Date(2000, 1, 10), Weekday.WEDNESDAY, Date(2000, 1, 5)),
            param(Date(2000, 1, 11), Weekday.WEDNESDAY, Date(2000, 1, 5)),
            param(Date(2000, 1, 12), Weekday.WEDNESDAY, Date(2000, 1, 12)),
            param(Date(2000, 1, 13), Weekday.WEDNESDAY, Date(2000, 1, 12)),
        ],
    )
    def test_date_weekly(
        self, *, date: Date, weekday: Weekday | None, expected: ZonedDateTime
    ) -> None:
        result = round_date_or_date_time(
            date, DateDelta(weeks=1), mode="floor", weekday=weekday
        )
        assert result == expected
        if weekday is not None:
            assert result.day_of_week() is weekday

    @mark.parametrize(
        ("delta", "expected"),
        [
            param(TimeDelta(hours=2), ZonedDateTime(2000, 1, 2, 2, tz=UTC.key)),
            param(TimeDelta(minutes=2), ZonedDateTime(2000, 1, 2, 3, 4, tz=UTC.key)),
            param(TimeDelta(seconds=2), ZonedDateTime(2000, 1, 2, 3, 4, 4, tz=UTC.key)),
            param(
                TimeDelta(milliseconds=2),
                ZonedDateTime(2000, 1, 2, 3, 4, 5, nanosecond=122000000, tz=UTC.key),
            ),
            param(
                TimeDelta(microseconds=2),
                ZonedDateTime(2000, 1, 2, 3, 4, 5, nanosecond=123456000, tz=UTC.key),
            ),
            param(
                TimeDelta(nanoseconds=2),
                ZonedDateTime(2000, 1, 2, 3, 4, 5, nanosecond=123456788, tz=UTC.key),
            ),
        ],
    )
    def test_date_time_intraday(self, *, delta: Delta, expected: ZonedDateTime) -> None:
        now = ZonedDateTime(2000, 1, 2, 3, 4, 5, nanosecond=123456789, tz=UTC.key)
        result = round_date_or_date_time(now, delta, mode="floor")
        assert result.exact_eq(expected)

    @mark.parametrize(
        ("date_time", "expected"),
        [
            param(
                ZonedDateTime(2000, 1, 1, 2, 3, 4, nanosecond=123456789, tz=UTC.key),
                ZonedDateTime(1999, 12, 31, tz=UTC.key),
            ),
            param(
                ZonedDateTime(2000, 1, 1, tz=UTC.key),
                ZonedDateTime(1999, 12, 31, tz=UTC.key),
            ),
            param(
                ZonedDateTime(2000, 1, 2, tz=UTC.key),
                ZonedDateTime(2000, 1, 2, tz=UTC.key),
            ),
        ],
    )
    def test_date_time_daily(
        self, *, date_time: ZonedDateTime, expected: ZonedDateTime
    ) -> None:
        result = round_date_or_date_time(date_time, DateDelta(days=2), mode="floor")
        assert result.exact_eq(expected)

    @mark.parametrize(
        "delta",
        [
            param(TimeDelta(hours=5)),
            param(TimeDelta(minutes=7)),
            param(TimeDelta(seconds=7)),
            param(TimeDelta(milliseconds=3)),
            param(TimeDelta(microseconds=3)),
            param(TimeDelta(nanoseconds=3)),
        ],
    )
    def test_error_increment(self, *, delta: TimeDelta) -> None:
        with raises(
            _RoundDateOrDateTimeIncrementError,
            match=r"Duration PT.* increment must be a proper divisor of \d+; got \d+",
        ):
            _ = round_date_or_date_time(TODAY_UTC, delta)

    def test_error_invalid(self) -> None:
        with raises(
            _RoundDateOrDateTimeInvalidDurationError,
            match="Duration must be valid; got P1M",
        ):
            _ = round_date_or_date_time(TODAY_UTC, MONTH)

    def test_error_date_with_weekday(self) -> None:
        with raises(
            _RoundDateOrDateTimeDateWithWeekdayError,
            match=r"Daily rounding must not be given a weekday; got Weekday\.MONDAY",
        ):
            _ = round_date_or_date_time(TODAY_UTC, DAY, weekday=Weekday.MONDAY)

    def test_error_date_with_intraday_delta(self) -> None:
        with raises(
            _RoundDateOrDateTimeDateWithIntradayDeltaError,
            match="Dates must not be given intraday durations; got .* and PT1S",
        ):
            _ = round_date_or_date_time(TODAY_UTC, SECOND)

    def test_error_date_time_intra_day_with_weekday(self) -> None:
        with raises(
            _RoundDateOrDateTimeDateTimeIntraDayWithWeekdayError,
            match=r"Date-times and intraday rounding must not be given a weekday; got .*, PT1S and Weekday\.MONDAY",
        ):
            _ = round_date_or_date_time(NOW_UTC, SECOND, weekday=Weekday.MONDAY)


class TestTimePeriod:
    @given(times=pairs(times()), date=dates())
    def test_at_day(self, *, times: tuple[Time, Time], date: Date) -> None:
        start, end = times
        period = TimePeriod(start, end)
        with assume_does_not_raise(_ZonedDateTimePeriodInvalidError):
            result = period.at(date)
        expected = ZonedDateTimePeriod(
            date.at(start).assume_tz(UTC.key), date.at(end).assume_tz(UTC.key)
        )
        assert result.exact_eq(expected)

    @given(times=pairs(times()), dates=pairs(dates(), sorted=True))
    def test_at_days(
        self, *, times: tuple[Time, Time], dates: tuple[Date, Date]
    ) -> None:
        start_time, end_time = times
        period = TimePeriod(start_time, end_time)
        start_date, end_date = dates
        with assume_does_not_raise(_ZonedDateTimePeriodInvalidError):
            result = period.at((start_date, end_date))
        expected = ZonedDateTimePeriod(
            start_date.at(start_time).assume_tz(UTC.key),
            end_date.at(end_time).assume_tz(UTC.key),
        )
        assert result.exact_eq(expected)

    @given(dates=pairs(dates(), sorted=True), times=pairs(times()))
    def test_at_times(
        self, *, dates: tuple[Date, Date], times: tuple[Time, Time]
    ) -> None:
        start_date, end_date = dates
        period = DatePeriod(start_date, end_date)
        start_time, end_time = times
        with assume_does_not_raise(_ZonedDateTimePeriodInvalidError):
            result = period.at((start_time, end_time))
        expected = ZonedDateTimePeriod(
            start_date.at(start_time).assume_tz(UTC.key),
            end_date.at(end_time).assume_tz(UTC.key),
        )
        assert result.exact_eq(expected)

    @given(times=pairs(times()), new_times=pairs(times()))
    def test_replace(
        self, *, times: tuple[Time, Time], new_times: tuple[Time, Time]
    ) -> None:
        start, end = times
        period = TimePeriod(start, end)
        new_start, new_end = new_times
        new = period.replace(start=new_start, end=new_end)
        assert new.start == new_start
        assert new.end == new_end

    @given(times=pairs(times()))
    @mark.parametrize("func", [param(repr), param(str)])
    def test_repr(self, *, times: tuple[Time, Time], func: Callable[..., str]) -> None:
        start, end = times
        period = TimePeriod(start, end)
        result = func(period)
        assert search(
            r"^TimePeriod\(\d{2}:\d{2}:\d{2}(\.\d{1,6})?, \d{2}:\d{2}:\d{2}(\.\d{1,6})?\)$",
            result,
        )

    @given(data=data(), times=pairs(times()))
    def test_to_and_from_dict(
        self, *, data: DataObject, times: tuple[Time, Time]
    ) -> None:
        start, end = times
        period = TimePeriod(start, end)
        dict1 = period.to_dict()
        dstart, dend = dict1["start"], dict1["end"]
        dict2 = PeriodDict(
            start=data.draw(sampled_from([dstart, dstart.py_time()])),
            end=data.draw(sampled_from([dend, dend.py_time()])),
        )
        result = TimePeriod.from_dict(dict2)
        assert result == period


class TestToDate:
    def test_default(self) -> None:
        assert to_date() == get_today()

    @given(date=dates())
    def test_date(self, *, date: Date) -> None:
        assert to_date(date) == date

    @given(date=dates())
    def test_str(self, *, date: Date) -> None:
        assert to_date(date.format_common_iso()) == date

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
    def test_replace_non_sentinel(self, *, dates: tuple[Date, Date]) -> None:
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
            assume_does_not_raise(ValueError, match="Out of range"),
            assume_does_not_raise(ValueError, match="days out of range"),
            assume_does_not_raise(
                OverflowError, match="Python int too large to convert to C long"
            ),
        ):
            delta = cls(days=days)
        assert to_days(delta) == days

    @given(days=integers())
    def test_time_delta(self, *, days: int) -> None:
        with (
            assume_does_not_raise(ValueError, match="Out of range"),
            assume_does_not_raise(ValueError, match="hours out of range"),
            assume_does_not_raise(OverflowError, match="int too big to convert"),
            assume_does_not_raise(
                OverflowError, match="Python int too large to convert to C long"
            ),
        ):
            delta = TimeDelta(hours=24 * days)
        assert to_days(delta) == days

    @mark.parametrize(
        "delta", [param(DateDelta(months=1)), param(DateTimeDelta(months=1))]
    )
    def test_error_months(self, *, delta: DateOrDateTimeDelta) -> None:
        with raises(_ToDaysMonthsError, match="Delta must not contain months; got 1"):
            _ = to_days(delta)

    @mark.parametrize(
        "delta", [param(TimeDelta(nanoseconds=1)), param(DateTimeDelta(nanoseconds=1))]
    )
    def test_error_nanoseconds(self, *, delta: TimeOrDateTimeDelta) -> None:
        with raises(
            _ToDaysNanosecondsError,
            match="Delta must not contain extra nanoseconds; got .*",
        ):
            _ = to_days(delta)


class TestToHours:
    @given(days=integers())
    def test_date_delta(self, *, days: int) -> None:
        with (
            assume_does_not_raise(ValueError, match="Out of range"),
            assume_does_not_raise(ValueError, match="days out of range"),
            assume_does_not_raise(
                OverflowError, match="Python int too large to convert to C long"
            ),
        ):
            delta = DateDelta(days=days)
        assert to_hours(delta) == (24 * days)

    @given(cls=sampled_from([TimeDelta, DateTimeDelta]), hours=integers())
    def test_time_or_date_time_delta(
        self, *, cls: type[TimeOrDateTimeDelta], hours: int
    ) -> None:
        with (
            assume_does_not_raise(ValueError, match="Out of range"),
            assume_does_not_raise(ValueError, match="hours out of range"),
            assume_does_not_raise(OverflowError, match="int too big to convert"),
            assume_does_not_raise(
                OverflowError, match="Python int too large to convert to C long"
            ),
        ):
            delta = cls(hours=hours)
        assert to_hours(delta) == hours

    @mark.parametrize(
        "delta", [param(DateDelta(months=1)), param(DateTimeDelta(months=1))]
    )
    def test_error_months(self, *, delta: DateOrDateTimeDelta) -> None:
        with raises(_ToHoursMonthsError, match="Delta must not contain months; got 1"):
            _ = to_hours(delta)

    @mark.parametrize(
        "delta", [param(TimeDelta(nanoseconds=1)), param(DateTimeDelta(nanoseconds=1))]
    )
    def test_error_nanoseconds(self, *, delta: TimeOrDateTimeDelta) -> None:
        with raises(
            _ToHoursNanosecondsError,
            match="Delta must not contain extra nanoseconds; got .*",
        ):
            _ = to_hours(delta)


class TestToLocalPlain:
    @given(date_time=zoned_datetimes())
    def test_main(self, *, date_time: ZonedDateTime) -> None:
        result = to_local_plain(date_time)
        assert isinstance(result, PlainDateTime)


class TestToMicroseconds:
    @given(days=integers())
    def test_date_delta(self, *, days: int) -> None:
        with (
            assume_does_not_raise(ValueError, match="Out of range"),
            assume_does_not_raise(ValueError, match="days out of range"),
            assume_does_not_raise(
                OverflowError, match="Python int too large to convert to C long"
            ),
        ):
            delta = DateDelta(days=days)
        assert to_microseconds(delta) == (24 * 60 * 60 * int(1e6) * days)

    @given(cls=sampled_from([TimeDelta, DateTimeDelta]), microseconds=integers())
    def test_time_or_date_time_delta(
        self, *, cls: type[TimeOrDateTimeDelta], microseconds: int
    ) -> None:
        with (
            assume_does_not_raise(ValueError, match="Out of range"),
            assume_does_not_raise(ValueError, match="microseconds out of range"),
            assume_does_not_raise(OverflowError, match="int too big to convert"),
            assume_does_not_raise(
                OverflowError, match="Python int too large to convert to C long"
            ),
        ):
            delta = cls(microseconds=microseconds)
        assert to_microseconds(delta) == microseconds

    @mark.parametrize(
        "delta", [param(DateDelta(months=1)), param(DateTimeDelta(months=1))]
    )
    def test_error_months(self, *, delta: DateOrDateTimeDelta) -> None:
        with raises(
            _ToMicrosecondsMonthsError, match="Delta must not contain months; got 1"
        ):
            _ = to_microseconds(delta)

    @mark.parametrize(
        "delta", [param(TimeDelta(nanoseconds=1)), param(DateTimeDelta(nanoseconds=1))]
    )
    def test_error_nanoseconds(self, *, delta: TimeOrDateTimeDelta) -> None:
        with raises(
            _ToMicrosecondsNanosecondsError,
            match="Delta must not contain extra nanoseconds; got .*",
        ):
            _ = to_microseconds(delta)


class TestToMilliseconds:
    @given(days=integers())
    def test_date_delta(self, *, days: int) -> None:
        with (
            assume_does_not_raise(ValueError, match="Out of range"),
            assume_does_not_raise(ValueError, match="days out of range"),
            assume_does_not_raise(
                OverflowError, match="Python int too large to convert to C long"
            ),
        ):
            delta = DateDelta(days=days)
        assert to_milliseconds(delta) == (24 * 60 * 60 * int(1e3) * days)

    @given(cls=sampled_from([TimeDelta, DateTimeDelta]), milliseconds=integers())
    def test_time_or_date_time_delta(
        self, *, cls: type[TimeOrDateTimeDelta], milliseconds: int
    ) -> None:
        with (
            assume_does_not_raise(ValueError, match="Out of range"),
            assume_does_not_raise(ValueError, match="milliseconds out of range"),
            assume_does_not_raise(OverflowError, match="int too big to convert"),
            assume_does_not_raise(
                OverflowError, match="Python int too large to convert to C long"
            ),
        ):
            delta = cls(milliseconds=milliseconds)
        assert to_milliseconds(delta) == milliseconds

    @mark.parametrize(
        "delta", [param(DateDelta(months=1)), param(DateTimeDelta(months=1))]
    )
    def test_error_months(self, *, delta: DateOrDateTimeDelta) -> None:
        with raises(
            _ToMillisecondsMonthsError, match="Delta must not contain months; got 1"
        ):
            _ = to_milliseconds(delta)

    @mark.parametrize(
        "delta", [param(TimeDelta(nanoseconds=1)), param(DateTimeDelta(nanoseconds=1))]
    )
    def test_error_nanoseconds(self, *, delta: TimeOrDateTimeDelta) -> None:
        with raises(
            _ToMillisecondsNanosecondsError,
            match="Delta must not contain extra nanoseconds; got .*",
        ):
            _ = to_milliseconds(delta)


class TestToMinutes:
    @given(days=integers())
    def test_date_delta(self, *, days: int) -> None:
        with (
            assume_does_not_raise(ValueError, match="Out of range"),
            assume_does_not_raise(ValueError, match="days out of range"),
            assume_does_not_raise(
                OverflowError, match="Python int too large to convert to C long"
            ),
        ):
            delta = DateDelta(days=days)
        assert to_minutes(delta) == (24 * 60 * days)

    @given(cls=sampled_from([TimeDelta, DateTimeDelta]), minutes=integers())
    def test_time_or_date_time_delta(
        self, *, cls: type[TimeOrDateTimeDelta], minutes: int
    ) -> None:
        with (
            assume_does_not_raise(ValueError, match="Out of range"),
            assume_does_not_raise(ValueError, match="minutes out of range"),
            assume_does_not_raise(OverflowError, match="int too big to convert"),
            assume_does_not_raise(
                OverflowError, match="Python int too large to convert to C long"
            ),
        ):
            delta = cls(minutes=minutes)
        assert to_minutes(delta) == minutes

    @mark.parametrize(
        "delta", [param(DateDelta(months=1)), param(DateTimeDelta(months=1))]
    )
    def test_error_months(self, *, delta: DateOrDateTimeDelta) -> None:
        with raises(
            _ToMinutesMonthsError, match="Delta must not contain months; got 1"
        ):
            _ = to_minutes(delta)

    @mark.parametrize(
        "delta", [param(TimeDelta(nanoseconds=1)), param(DateTimeDelta(nanoseconds=1))]
    )
    def test_error_nanoseconds(self, *, delta: TimeOrDateTimeDelta) -> None:
        with raises(
            _ToMinutesNanosecondsError,
            match="Delta must not contain extra nanoseconds; got .*",
        ):
            _ = to_minutes(delta)


class TestToMonths:
    @given(cls=sampled_from([DateDelta, DateTimeDelta]), months=integers())
    def test_main(self, *, cls: type[DateOrDateTimeDelta], months: int) -> None:
        with (
            assume_does_not_raise(ValueError, match="Out of range"),
            assume_does_not_raise(ValueError, match="months out of range"),
            assume_does_not_raise(
                OverflowError, match="Python int too large to convert to C long"
            ),
        ):
            delta = cls(months=months)
        assert to_months(delta) == months

    @mark.parametrize("delta", [param(DateDelta(days=1)), param(DateTimeDelta(days=1))])
    def test_error_days(self, *, delta: DateOrDateTimeDelta) -> None:
        with raises(_ToMonthsDaysError, match="Delta must not contain days; got 1"):
            _ = to_months(delta)

    def test_error_date_time_delta_time(self) -> None:
        delta = DateTimeDelta(nanoseconds=1)
        with raises(
            _ToMonthsTimeError, match="Delta must not contain a time part; got .*"
        ):
            _ = to_months(delta)


class TestToMonthsAndDays:
    @given(
        cls=sampled_from([DateDelta, DateTimeDelta]), months=integers(), days=integers()
    )
    def test_main(
        self, *, cls: type[DateOrDateTimeDelta], months: int, days: int
    ) -> None:
        with (
            assume_does_not_raise(ValueError, match="Out of range"),
            assume_does_not_raise(ValueError, match="Mixed sign in Date(Time)?Delta"),
            assume_does_not_raise(ValueError, match="months out of range"),
            assume_does_not_raise(ValueError, match="days out of range"),
            assume_does_not_raise(
                OverflowError, match="Python int too large to convert to C long"
            ),
        ):
            delta = cls(months=months, days=days)
        assert to_months_and_days(delta) == (months, days)

    def test_error_date_time_delta_time(self) -> None:
        delta = DateTimeDelta(nanoseconds=1)
        with raises(
            ToMonthsAndDaysError, match="Delta must not contain a time part; got .*"
        ):
            _ = to_months_and_days(delta)


class TestToNanoseconds:
    @given(func=sampled_from([to_time_delta, to_date_time_delta]), nanos=integers())
    def test_main(
        self, *, func: Callable[[int], TimeOrDateTimeDelta], nanos: int
    ) -> None:
        with (
            assume_does_not_raise(ValueError, match="Out of range"),
            assume_does_not_raise(ValueError, match="TimeDelta out of range"),
            assume_does_not_raise(ValueError, match="total days out of range"),
            assume_does_not_raise(
                OverflowError, match="Python int too large to convert to C long"
            ),
            assume_does_not_raise(OverflowError, match="int too big to convert"),
        ):
            delta = func(nanos)
        assert to_nanoseconds(delta) == nanos

    @mark.parametrize(
        "delta", [param(DateDelta(months=1)), param(DateTimeDelta(months=1))]
    )
    def test_error(self, *, delta: DateOrDateTimeDelta) -> None:
        with raises(ToNanosecondsError, match="Delta must not contain months; got 1"):
            _ = to_nanoseconds(delta)


class TestToPyDateOrDateTime:
    @mark.parametrize(
        ("date_or_date_time", "expected"),
        [
            param(Date(2000, 1, 1), dt.date(2000, 1, 1)),
            param(
                ZonedDateTime(2000, 1, 1, tz=UTC.key),
                dt.datetime(2000, 1, 1, tzinfo=UTC),
            ),
            param(None, None),
        ],
    )
    def test_main(
        self,
        *,
        date_or_date_time: Date | ZonedDateTime | None,
        expected: dt.date | None,
    ) -> None:
        result = to_py_date_or_date_time(date_or_date_time)
        assert result == expected


class TestToPyTimeDelta:
    @mark.parametrize(
        ("delta", "expected"),
        [
            param(DateDelta(days=1), dt.timedelta(days=1)),
            param(TimeDelta(microseconds=1), dt.timedelta(microseconds=1)),
            param(
                DateTimeDelta(days=1, microseconds=1),
                dt.timedelta(days=1, microseconds=1),
            ),
            param(None, None),
        ],
    )
    def test_main(self, *, delta: Delta | None, expected: dt.timedelta | None) -> None:
        result = to_py_time_delta(delta)
        assert result == expected

    def test_error(self) -> None:
        delta = TimeDelta(nanoseconds=1)
        with raises(
            ToPyTimeDeltaError, match="Time delta must not contain nanoseconds; got 1"
        ):
            _ = to_py_time_delta(delta)


class TestToSeconds:
    @given(days=integers())
    def test_date_delta(self, *, days: int) -> None:
        with (
            assume_does_not_raise(ValueError, match="Out of range"),
            assume_does_not_raise(ValueError, match="days out of range"),
            assume_does_not_raise(
                OverflowError, match="Python int too large to convert to C long"
            ),
        ):
            delta = DateDelta(days=days)
        assert to_seconds(delta) == (24 * 60 * 60 * days)

    @given(cls=sampled_from([TimeDelta, DateTimeDelta]), seconds=integers())
    def test_time_or_date_time_delta(
        self, *, cls: type[TimeOrDateTimeDelta], seconds: int
    ) -> None:
        with (
            assume_does_not_raise(ValueError, match="Out of range"),
            assume_does_not_raise(ValueError, match="seconds out of range"),
            assume_does_not_raise(OverflowError, match="int too big to convert"),
            assume_does_not_raise(
                OverflowError, match="Python int too large to convert to C long"
            ),
        ):
            delta = cls(seconds=seconds)
        assert to_seconds(delta) == seconds

    @mark.parametrize(
        "delta", [param(DateDelta(months=1)), param(DateTimeDelta(months=1))]
    )
    def test_error_months(self, *, delta: DateOrDateTimeDelta) -> None:
        with raises(
            _ToSecondsMonthsError, match="Delta must not contain months; got 1"
        ):
            _ = to_seconds(delta)

    @mark.parametrize(
        "delta", [param(TimeDelta(nanoseconds=1)), param(DateTimeDelta(nanoseconds=1))]
    )
    def test_error_nanoseconds(self, *, delta: TimeOrDateTimeDelta) -> None:
        with raises(
            _ToSecondsNanosecondsError,
            match="Delta must not contain extra nanoseconds; got .*",
        ):
            _ = to_seconds(delta)


class TestToWeeks:
    @given(cls=sampled_from([DateDelta, DateTimeDelta]), weeks=integers())
    def test_date_or_date_time_delta(
        self, *, cls: type[DateOrDateTimeDelta], weeks: int
    ) -> None:
        with (
            assume_does_not_raise(ValueError, match="Out of range"),
            assume_does_not_raise(ValueError, match="days out of range"),
            assume_does_not_raise(ValueError, match="weeks out of range"),
            assume_does_not_raise(
                OverflowError, match="Python int too large to convert to C long"
            ),
        ):
            delta = cls(weeks=weeks)
        assert to_weeks(delta) == weeks

    @given(weeks=integers())
    def test_time_delta(self, *, weeks: int) -> None:
        with (
            assume_does_not_raise(ValueError, match="Out of range"),
            assume_does_not_raise(ValueError, match="hours out of range"),
            assume_does_not_raise(OverflowError, match="int too big to convert"),
            assume_does_not_raise(
                OverflowError, match="Python int too large to convert to C long"
            ),
        ):
            delta = TimeDelta(hours=7 * 24 * weeks)
        assert to_weeks(delta) == weeks

    @mark.parametrize(
        "delta", [param(DateDelta(months=1)), param(DateTimeDelta(months=1))]
    )
    def test_error_months(self, *, delta: DateOrDateTimeDelta) -> None:
        with raises(_ToWeeksMonthsError, match="Delta must not contain months; got 1"):
            _ = to_weeks(delta)

    @mark.parametrize("delta", [param(DateDelta(days=8)), param(DateTimeDelta(days=8))])
    def test_error_days(self, *, delta: DateOrDateTimeDelta) -> None:
        with raises(
            _ToWeeksDaysError, match="Delta must not contain extra days; got 1"
        ):
            _ = to_weeks(delta)

    @mark.parametrize(
        "delta", [param(TimeDelta(nanoseconds=1)), param(DateTimeDelta(nanoseconds=1))]
    )
    def test_error_nanoseconds(self, *, delta: TimeOrDateTimeDelta) -> None:
        with raises(
            _ToWeeksNanosecondsError,
            match="Delta must not contain extra nanoseconds; got .*",
        ):
            _ = to_weeks(delta)


class TestToYears:
    @given(cls=sampled_from([DateDelta, DateTimeDelta]), years=integers())
    def test_main(self, *, cls: type[DateOrDateTimeDelta], years: int) -> None:
        with (
            assume_does_not_raise(ValueError, match="Out of range"),
            assume_does_not_raise(ValueError, match="months out of range"),
            assume_does_not_raise(ValueError, match="years out of range"),
            assume_does_not_raise(
                OverflowError, match="Python int too large to convert to C long"
            ),
        ):
            delta = cls(years=years)
        assert to_years(delta) == years

    @mark.parametrize(
        "delta", [param(DateDelta(months=1)), param(DateTimeDelta(months=1))]
    )
    def test_error_date_delta_months(self, *, delta: DateOrDateTimeDelta) -> None:
        with raises(_ToYearsMonthsError, match="Delta must not contain months; got 1"):
            _ = to_years(delta)

    @mark.parametrize("delta", [param(DateDelta(days=1)), param(DateTimeDelta(days=1))])
    def test_error_date_delta_days(self, *, delta: DateOrDateTimeDelta) -> None:
        with raises(_ToYearsDaysError, match="Delta must not contain days; got 1"):
            _ = to_years(delta)

    def test_error_date_time_delta_time(self) -> None:
        delta = DateTimeDelta(nanoseconds=1)
        with raises(
            _ToYearsTimeError, match="Delta must not contain a time part; got .*"
        ):
            _ = to_years(delta)


class TestToZonedDateTime:
    def test_default(self) -> None:
        assert abs(to_zoned_date_time() - get_now()) <= SECOND

    @given(date_time=zoned_datetimes())
    def test_date_time(self, *, date_time: ZonedDateTime) -> None:
        assert to_zoned_date_time(date_time) == date_time

    @given(date_time=zoned_datetimes_2000)
    def test_py_date_time_zone_info(self, *, date_time: ZonedDateTime) -> None:
        assert to_zoned_date_time(date_time.py_datetime()) == date_time

    @given(date_time=zoned_datetimes_2000)
    def test_py_date_time_dt_utc(self, *, date_time: ZonedDateTime) -> None:
        result = to_zoned_date_time(date_time.py_datetime().astimezone(dt.UTC))
        assert result == date_time

    @given(date_time=zoned_datetimes())
    def test_callable(self, *, date_time: ZonedDateTime) -> None:
        assert to_zoned_date_time(lambda: date_time) == date_time

    def test_none(self) -> None:
        assert abs(to_zoned_date_time(None) - get_now()) <= SECOND

    def test_sentinel(self) -> None:
        assert to_zoned_date_time(sentinel) is sentinel

    @given(date_times=pairs(zoned_datetimes()))
    def test_replace_non_sentinel(
        self, *, date_times: tuple[ZonedDateTime, ZonedDateTime]
    ) -> None:
        date_time1, date_time2 = date_times

        @dataclass(kw_only=True, slots=True)
        class Example:
            date_time: ZonedDateTime = field(default_factory=get_now)

            def replace(
                self, *, date_time: MaybeCallableZonedDateTimeLike | Sentinel = sentinel
            ) -> Self:
                return replace_non_sentinel(
                    self, date_time=to_zoned_date_time(date_time)
                )

        obj = Example(date_time=date_time1)
        assert obj.date_time == date_time1
        assert obj.replace().date_time == date_time1
        assert obj.replace(date_time=date_time2).date_time == date_time2
        assert abs(obj.replace(date_time=get_now).date_time - get_now()) <= SECOND

    def test_error_py_date_time(self) -> None:
        with raises(
            ToZonedDateTimeError,
            match=r"Expected date-time to have a `ZoneInfo` or `dt\.UTC` as its timezone; got None",
        ):
            _ = to_zoned_date_time(NOW_UTC.py_datetime().replace(tzinfo=None))


class TestTwoDigitYearMonth:
    def test_parse_common_iso(self) -> None:
        result = two_digit_year_month(0, 1)
        expected = YearMonth(2000, 1)
        assert result == expected


class TestWheneverLogRecord:
    def test_init(self) -> None:
        _ = WheneverLogRecord("name", DEBUG, "pathname", 0, None, None, None)

    def test_get_length(self) -> None:
        assert isinstance(WheneverLogRecord._get_length(), int)

    def test_get_time_zone(self) -> None:
        assert isinstance(WheneverLogRecord._get_time_zone(), ZoneInfo)

    def test_get_time_zone_key(self) -> None:
        assert isinstance(WheneverLogRecord._get_time_zone_key(), str)


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

    @given(datetimes=pairs(zoned_datetimes(), sorted=True))
    def test_replace(self, *, datetimes: tuple[ZonedDateTime, ZonedDateTime]) -> None:
        start, end = datetimes
        period = ZonedDateTimePeriod(start, end)
        new = period.replace(start=start - SECOND, end=end + SECOND)
        assert new.start == (start - SECOND)
        assert new.end == (end + SECOND)

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

    @given(data=data(), datetimes=pairs(zoned_datetimes_2000, sorted=True))
    def test_to_and_from_dict(
        self, data: DataObject, *, datetimes: tuple[ZonedDateTime, ZonedDateTime]
    ) -> None:
        start, end = datetimes
        period = ZonedDateTimePeriod(start, end)
        dict1 = period.to_dict()
        dstart, dend = dict1["start"], dict1["end"]
        dict2 = PeriodDict(
            start=data.draw(sampled_from([dstart, dstart.py_datetime()])),
            end=data.draw(sampled_from([dend, dend.py_datetime()])),
        )
        result = ZonedDateTimePeriod.from_dict(dict2)
        assert result == period

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
        with raises(
            _ZonedDateTimePeriodInvalidError, match="Invalid period; got .* > .*"
        ):
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
            _ZonedDateTimePeriodTimeZoneError,
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
            _ZonedDateTimePeriodExactEqError, match=r"Invalid arguments; got \(.*\)"
        ):
            _ = period.exact_eq(cast("Any", start))
