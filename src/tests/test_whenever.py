from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from logging import DEBUG
from re import search
from typing import TYPE_CHECKING, Any, ClassVar, Self, cast
from zoneinfo import ZoneInfo

from hypothesis import HealthCheck, given, settings
from hypothesis.strategies import DataObject, data, integers, none, sampled_from
from pathvalidate import validate_filepath
from pytest import mark, param, raises
from whenever import (
    Date,
    DateDelta,
    DateTimeDelta,
    PlainDateTime,
    Time,
    TimeDelta,
    Weekday,
    YearMonth,
    ZonedDateTime,
)

from utilities.dataclasses import replace_non_sentinel
from utilities.hypothesis import (
    assume_does_not_raise,
    date_deltas,
    date_periods,
    dates,
    pairs,
    plain_date_times,
    time_deltas,
    time_periods,
    times,
    zone_infos,
    zoned_date_time_periods,
    zoned_date_times,
    zoned_date_times_2000,
)
from utilities.sentinel import Sentinel, sentinel
from utilities.types import TIME_ZONES, MaybeCallableTimeLike
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
    NOW_LOCAL_PLAIN,
    NOW_PLAIN,
    NOW_UTC,
    SECOND,
    TIME_DELTA_MAX,
    TIME_DELTA_MIN,
    TIME_LOCAL,
    TIME_UTC,
    TODAY_LOCAL,
    TODAY_UTC,
    ZERO_DAYS,
    ZONED_DATE_TIME_MAX,
    ZONED_DATE_TIME_MIN,
    DatePeriod,
    DatePeriodError,
    MeanDateTimeError,
    MinMaxDateError,
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
    get_now_local_plain,
    get_now_plain,
    get_time,
    get_time_local,
    get_today,
    get_today_local,
    is_weekend,
    mean_datetime,
    min_max_date,
    round_date_or_date_time,
    sub_year_month,
    to_date,
    to_date_time_delta,
    to_days,
    to_hours,
    to_microseconds,
    to_milliseconds,
    to_minutes,
    to_months,
    to_months_and_days,
    to_nanoseconds,
    to_py_date_or_date_time,
    to_py_time_delta,
    to_seconds,
    to_time,
    to_time_delta,
    to_weeks,
    to_years,
    to_zoned_date_time,
    two_digit_year_month,
)
from utilities.zoneinfo import UTC

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
        TimeZone,
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
    @given(period=date_periods(), delta=date_deltas())
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_add(self, *, period: DatePeriod, delta: DateDelta) -> None:
        with assume_does_not_raise(ValueError, match=r"Resulting date out of range"):
            result = period + delta
        expected = DatePeriod(period.start + delta, period.end + delta)
        assert result == expected

    @given(period=date_periods(), time=times())
    def test_at_time(self, *, period: DatePeriod, time: Time) -> None:
        result = period.at(time)
        expected = ZonedDateTimePeriod(
            period.start.at(time).assume_tz(UTC.key),
            period.end.at(time).assume_tz(UTC.key),
        )
        assert result.exact_eq(expected)

    @given(period=date_periods(), times=pairs(times()))
    def test_at_times(self, *, period: DatePeriod, times: tuple[Time, Time]) -> None:
        start_time, end_time = times
        with assume_does_not_raise(_ZonedDateTimePeriodInvalidError):
            result = period.at((start_time, end_time))
        expected = ZonedDateTimePeriod(
            period.start.at(start_time).assume_tz(UTC.key),
            period.end.at(end_time).assume_tz(UTC.key),
        )
        assert result.exact_eq(expected)

    @given(period=date_periods(), date=dates())
    def test_contains(self, *, period: DatePeriod, date: Date) -> None:
        result = date in period
        expected = period.start <= date <= period.end
        assert result is expected

    @given(period=date_periods())
    def test_delta(self, *, period: DatePeriod) -> None:
        assert period.delta == (period.end - period.start)

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

    @given(period=date_periods())
    def test_hashable(self, *, period: DatePeriod) -> None:
        _ = hash(period)

    @given(period=date_periods())
    def test_replace(self, *, period: DatePeriod) -> None:
        new = period.replace(start=period.start - DAY, end=period.end + DAY)
        assert new.start == (period.start - DAY)
        assert new.end == (period.end + DAY)

    @given(period=date_periods())
    @mark.parametrize("func", [param(repr), param(str)])
    def test_repr(self, *, period: DatePeriod, func: Callable[..., str]) -> None:
        result = func(period)
        assert search(r"^DatePeriod\(\d{4}-\d{2}-\d{2}, \d{4}-\d{2}-\d{2}\)$", result)

    @given(periods=pairs(date_periods()))
    def test_sortable(self, *, periods: tuple[DatePeriod, DatePeriod]) -> None:
        _ = sorted(periods)

    @given(period=date_periods(), delta=date_deltas())
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_sub(self, *, period: DatePeriod, delta: DateDelta) -> None:
        with assume_does_not_raise(ValueError, match=r"Resulting date out of range"):
            result = period - delta
        expected = DatePeriod(period.start - delta, period.end - delta)
        assert result == expected

    @given(data=data(), period=date_periods())
    def test_to_and_from_dict(self, *, data: DataObject, period: DatePeriod) -> None:
        dict_ = data.draw(sampled_from([period.to_dict(), period.to_py_dict()]))
        result = DatePeriod.from_dict(dict_)
        assert result == period

    @given(dates=pairs(dates(), unique=True, sorted=True))
    def test_error_period_invalid(self, *, dates: tuple[Date, Date]) -> None:
        start, end = dates
        with raises(DatePeriodError, match=r"Invalid period; got .* > .*"):
            _ = DatePeriod(end, start)


class TestDatetimeUTC:
    @given(datetime=zoned_date_times())
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
        parsed = Date.parse_iso(result)
        assert parsed == date

    @given(time=times())
    def test_time(self, *, time: Time) -> None:
        result = format_compact(time)
        assert isinstance(result, str)
        parsed = Time.parse_iso(result)
        assert parsed.nanosecond == 0
        expected = time.round()
        assert parsed == expected

    @given(date_time=plain_date_times())
    def test_plain_date_time(self, *, date_time: PlainDateTime) -> None:
        result = format_compact(date_time)
        assert isinstance(result, str)
        parsed = PlainDateTime.parse_iso(result)
        assert parsed.nanosecond == 0
        expected = date_time.round()
        assert parsed == expected

    @given(date_time=zoned_date_times(time_zone=zone_infos()))
    def test_zoned_date_time(self, *, date_time: ZonedDateTime) -> None:
        result = format_compact(date_time)
        assert isinstance(result, str)
        parsed = to_zoned_date_time(result)
        assert parsed.nanosecond == 0
        expected = date_time.round()
        assert parsed == expected

    @mark.parametrize(
        ("time_zone", "suffix"),
        [
            param("America/Argentina/Buenos_Aires", "America~Argentina~Buenos_Aires"),
            param("Asia/Hong_Kong", "Asia~Hong_Kong"),
            param("Etc/GMT", "Etc~GMT"),
            param("Etc/GMT+1", "Etc~GMT+1"),
            param("Etc/GMT-1", "Etc~GMT-1"),
        ],
    )
    def test_zoned_date_time_path(self, *, time_zone: TimeZone, suffix: str) -> None:
        assert time_zone in TIME_ZONES
        date_time = ZonedDateTime(
            2000, 1, 2, 12, 34, 56, nanosecond=123456789, tz=time_zone
        )
        ser = format_compact(date_time, path=True)
        validate_filepath(ser)
        expected = f"20000102T123456[{suffix}]"
        assert ser == expected
        result = to_zoned_date_time(ser)
        assert result.exact_eq(date_time.round())


class TestFromTimeStamp:
    @given(
        datetime=zoned_date_times(time_zone=zone_infos()).map(
            lambda d: d.round("second")
        )
    )
    def test_main(self, *, datetime: ZonedDateTime) -> None:
        timestamp = datetime.timestamp()
        result = from_timestamp(timestamp, time_zone=ZoneInfo(datetime.tz))
        assert result == datetime

    @given(
        datetime=zoned_date_times(time_zone=zone_infos()).map(
            lambda d: d.round("millisecond")
        )
    )
    def test_millis(self, *, datetime: ZonedDateTime) -> None:
        timestamp = datetime.timestamp_millis()
        result = from_timestamp_millis(timestamp, time_zone=ZoneInfo(datetime.tz))
        assert result == datetime

    @given(datetime=zoned_date_times(time_zone=zone_infos()))
    def test_nanos(self, *, datetime: ZonedDateTime) -> None:
        timestamp = datetime.timestamp_nanos()
        result = from_timestamp_nanos(timestamp, time_zone=ZoneInfo(datetime.tz))
        assert result == datetime


class TestGetNow:
    @given(time_zone=zone_infos())
    def test_function(self, *, time_zone: ZoneInfo) -> None:
        now = get_now(time_zone)
        assert isinstance(now, ZonedDateTime)
        assert now.tz == time_zone.key

    def test_constant(self) -> None:
        assert isinstance(NOW_UTC, ZonedDateTime)
        expected = UTC.key
        assert NOW_UTC.tz == expected


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


class TestGetNowLocalPlain:
    def test_function(self) -> None:
        now = get_now_local_plain()
        assert isinstance(now, PlainDateTime)

    def test_constant(self) -> None:
        assert isinstance(NOW_LOCAL_PLAIN, PlainDateTime)


class TestGetNowPlain:
    @given(time_zone=zone_infos())
    def test_function(self, *, time_zone: ZoneInfo) -> None:
        now = get_now_plain(time_zone)
        assert isinstance(now, PlainDateTime)

    def test_constant(self) -> None:
        assert isinstance(NOW_PLAIN, PlainDateTime)


class TestGetTime:
    @given(time_zone=zone_infos())
    def test_function(self, *, time_zone: ZoneInfo) -> None:
        now = get_time(time_zone)
        assert isinstance(now, Time)

    def test_constant(self) -> None:
        assert isinstance(TIME_UTC, Time)


class TestGetTimeLocal:
    def test_function(self) -> None:
        now = get_time_local()
        assert isinstance(now, Time)

    def test_constant(self) -> None:
        assert isinstance(TIME_LOCAL, Time)


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


class TestIsWeekend:
    @mark.parametrize(
        ("weekday", "time", "expected"),
        [
            param(Weekday.SATURDAY, Time(23, 59, 59), False),
            param(Weekday.SUNDAY, Time(7, 59, 59), False),
            param(Weekday.SUNDAY, Time(8), True),
            param(Weekday.SUNDAY, Time(8, 0, 1), True),
            param(Weekday.SUNDAY, Time(16, 59, 59), True),
            param(Weekday.SUNDAY, Time(17), True),
            param(Weekday.SUNDAY, Time(17, 0, 1), False),
        ],
    )
    def test_one_day(self, *, weekday: Weekday, time: Time, expected: bool) -> None:
        self._run_test(
            weekday,
            time,
            Weekday.SUNDAY,
            Time(8),
            Weekday.SUNDAY,
            Time(17),
            expected=expected,
        )

    @mark.parametrize(
        ("weekday", "time", "expected"),
        [
            param(Weekday.FRIDAY, Time(23, 59, 59), False),
            param(Weekday.SATURDAY, Time(), False),
            param(Weekday.SATURDAY, Time(0, 0, 1), False),
            param(Weekday.SATURDAY, Time(7, 59, 59), False),
            param(Weekday.SATURDAY, Time(8), True),
            param(Weekday.SATURDAY, Time(8, 0, 1), True),
            param(Weekday.SATURDAY, Time(23, 59, 59), True),
            param(Weekday.SUNDAY, Time(), True),
            param(Weekday.SUNDAY, Time(0, 0, 1), True),
            param(Weekday.SUNDAY, Time(16, 59, 59), True),
            param(Weekday.SUNDAY, Time(17), True),
            param(Weekday.SUNDAY, Time(17, 0, 1), False),
        ],
    )
    def test_two_days(self, *, weekday: Weekday, time: Time, expected: bool) -> None:
        self._run_test(
            weekday,
            time,
            Weekday.SATURDAY,
            Time(8),
            Weekday.SUNDAY,
            Time(17),
            expected=expected,
        )

    @mark.parametrize(
        ("weekday", "time", "expected"),
        [
            param(Weekday.THURSDAY, Time(23, 59, 59), False),
            param(Weekday.FRIDAY, Time(), False),
            param(Weekday.FRIDAY, Time(0, 0, 1), False),
            param(Weekday.FRIDAY, Time(7, 59, 59), False),
            param(Weekday.FRIDAY, Time(8), True),
            param(Weekday.FRIDAY, Time(8, 0, 1), True),
            param(Weekday.FRIDAY, Time(23, 59, 59), True),
            param(Weekday.SATURDAY, Time(), True),
            param(Weekday.SATURDAY, Time(0, 0, 1), True),
            param(Weekday.SATURDAY, Time(23, 59, 59), True),
            param(Weekday.SUNDAY, Time(), True),
            param(Weekday.SUNDAY, Time(0, 0, 1), True),
            param(Weekday.SUNDAY, Time(16, 59, 59), True),
            param(Weekday.SUNDAY, Time(17), True),
            param(Weekday.SUNDAY, Time(17, 0, 1), False),
        ],
    )
    def test_three_days(self, *, weekday: Weekday, time: Time, expected: bool) -> None:
        self._run_test(
            weekday,
            time,
            Weekday.FRIDAY,
            Time(8),
            Weekday.SUNDAY,
            Time(17),
            expected=expected,
        )

    @mark.parametrize(
        ("weekday", "time", "expected"),
        [
            param(Weekday.FRIDAY, Time(23, 59, 59), False),
            param(Weekday.SATURDAY, Time(), False),
            param(Weekday.SATURDAY, Time(0, 0, 1), False),
            param(Weekday.SATURDAY, Time(7, 59, 59), False),
            param(Weekday.SATURDAY, Time(8), True),
            param(Weekday.SATURDAY, Time(8, 0, 1), True),
            param(Weekday.SATURDAY, Time(23, 59, 59), True),
            param(Weekday.SUNDAY, Time(), True),
            param(Weekday.SUNDAY, Time(0, 0, 1), True),
            param(Weekday.SUNDAY, Time(23, 59, 59), True),
            param(Weekday.MONDAY, Time(), True),
            param(Weekday.MONDAY, Time(0, 0, 1), True),
            param(Weekday.MONDAY, Time(16, 59, 59), True),
            param(Weekday.MONDAY, Time(17), True),
            param(Weekday.MONDAY, Time(17, 0, 1), False),
        ],
    )
    def test_wrap(self, *, weekday: Weekday, time: Time, expected: bool) -> None:
        self._run_test(
            weekday,
            time,
            Weekday.SATURDAY,
            Time(8),
            Weekday.MONDAY,
            Time(17),
            expected=expected,
        )

    def _run_test(
        self,
        weekday: Weekday,
        time: Time,
        start_weekday: Weekday,
        start_time: Time,
        end_weekday: Weekday,
        end_time: Time,
        /,
        *,
        expected: bool,
    ) -> None:
        date_time = get_today().at(time).assume_tz(UTC.key)
        while date_time.date().day_of_week() is not weekday:
            date_time += DAY
        result = is_weekend(
            date_time, start=(start_weekday, start_time), end=(end_weekday, end_time)
        )
        assert result is expected


class TestMeanDateTime:
    threshold: ClassVar[TimeDelta] = 100 * MICROSECOND

    @given(datetime=zoned_date_times())
    def test_one(self, *, datetime: ZonedDateTime) -> None:
        result = mean_datetime([datetime])
        assert result == datetime

    @given(datetime=zoned_date_times())
    def test_many(self, *, datetime: ZonedDateTime) -> None:
        result = mean_datetime([datetime, datetime + MINUTE])
        expected = datetime + 30 * SECOND
        assert abs(result - expected) <= self.threshold

    @given(datetime=zoned_date_times())
    def test_weights(self, *, datetime: ZonedDateTime) -> None:
        result = mean_datetime([datetime, datetime + MINUTE], weights=[1, 3])
        expected = datetime + 45 * SECOND
        assert abs(result - expected) <= self.threshold

    def test_error(self) -> None:
        with raises(MeanDateTimeError, match=r"Mean requires at least 1 datetime"):
            _ = mean_datetime([])


class TestMinMax:
    def test_date_delta_min(self) -> None:
        with raises(ValueError, match=r"Addition result out of bounds"):
            _ = DATE_DELTA_MIN - DateDelta(days=1)

    def test_date_delta_max(self) -> None:
        with raises(ValueError, match=r"Addition result out of bounds"):
            _ = DATE_DELTA_MAX + DateDelta(days=1)

    def test_date_delta_parsable_min(self) -> None:
        self._format_parse_date_delta(DATE_DELTA_PARSABLE_MIN)
        with raises(ValueError, match=r"Invalid format: '.*'"):
            self._format_parse_date_delta(DATE_DELTA_PARSABLE_MIN - DateDelta(days=1))

    def test_date_delta_parsable_max(self) -> None:
        self._format_parse_date_delta(DATE_DELTA_PARSABLE_MAX)
        with raises(ValueError, match=r"Invalid format: '.*'"):
            self._format_parse_date_delta(DATE_DELTA_PARSABLE_MAX + DateDelta(days=1))

    def test_date_time_delta_min(self) -> None:
        nanos = to_nanoseconds(DATE_TIME_DELTA_MIN)
        with raises(ValueError, match=r"Out of range"):
            _ = to_date_time_delta(nanos - 1)

    def test_date_time_delta_max(self) -> None:
        nanos = to_nanoseconds(DATE_TIME_DELTA_MAX)
        with raises(ValueError, match=r"Out of range"):
            _ = to_date_time_delta(nanos + 1)

    def test_date_time_delta_parsable_min(self) -> None:
        self._format_parse_date_time_delta(DATE_TIME_DELTA_PARSABLE_MIN)
        nanos = to_nanoseconds(DATE_TIME_DELTA_PARSABLE_MIN)
        with raises(ValueError, match=r"Invalid format or out of range: '.*'"):
            self._format_parse_date_time_delta(to_date_time_delta(nanos - 1))

    def test_date_time_delta_parsable_max(self) -> None:
        self._format_parse_date_time_delta(DATE_TIME_DELTA_PARSABLE_MAX)
        nanos = to_nanoseconds(DATE_TIME_DELTA_PARSABLE_MAX)
        with raises(ValueError, match=r"Invalid format or out of range: '.*'"):
            _ = self._format_parse_date_time_delta(to_date_time_delta(nanos + 1))

    def test_plain_date_time_min(self) -> None:
        with raises(ValueError, match=r"Result of subtract\(\) out of range"):
            _ = PlainDateTime.MIN.subtract(nanoseconds=1, ignore_dst=True)

    def test_plain_date_time_max(self) -> None:
        with raises(ValueError, match=r"Result of add\(\) out of range"):
            _ = PlainDateTime.MAX.add(microseconds=1, ignore_dst=True)

    def test_time_delta_min(self) -> None:
        nanos = TIME_DELTA_MIN.in_nanoseconds()
        with raises(ValueError, match=r"TimeDelta out of range"):
            _ = to_time_delta(nanos - 1)

    def test_time_delta_max(self) -> None:
        nanos = TIME_DELTA_MAX.in_nanoseconds()
        with raises(ValueError, match=r"TimeDelta out of range"):
            _ = to_time_delta(nanos + 1)

    def test_zoned_date_time_min(self) -> None:
        with raises(ValueError, match=r"Instant is out of range"):
            _ = ZONED_DATE_TIME_MIN.subtract(nanoseconds=1)

    def test_zoned_date_time_max(self) -> None:
        with raises(ValueError, match=r"Instant is out of range"):
            _ = ZONED_DATE_TIME_MAX.add(microseconds=1)

    def _format_parse_date_delta(self, delta: DateDelta, /) -> None:
        _ = DateDelta.parse_iso(delta.format_iso())

    def _format_parse_date_time_delta(self, delta: DateTimeDelta, /) -> None:
        _ = DateTimeDelta.parse_iso(delta.format_iso())


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
            assume_does_not_raise(ValueError, match=r"Resulting date out of range"),
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
            match=r"Min date must be at most max date; got .* > .*",
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
            match=r"Duration must be valid; got P1M",
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
            match=r"Dates must not be given intraday durations; got .* and PT1S",
        ):
            _ = round_date_or_date_time(TODAY_UTC, SECOND)

    def test_error_date_time_intra_day_with_weekday(self) -> None:
        with raises(
            _RoundDateOrDateTimeDateTimeIntraDayWithWeekdayError,
            match=r"Date-times and intraday rounding must not be given a weekday; got .*, PT1S and Weekday\.MONDAY",
        ):
            _ = round_date_or_date_time(NOW_UTC, SECOND, weekday=Weekday.MONDAY)


class TestTimePeriod:
    @given(period=time_periods(), date=dates())
    def test_at_day(self, *, period: TimePeriod, date: Date) -> None:
        with assume_does_not_raise(_ZonedDateTimePeriodInvalidError):
            result = period.at(date)
        expected = ZonedDateTimePeriod(
            date.at(period.start).assume_tz(UTC.key),
            date.at(period.end).assume_tz(UTC.key),
        )
        assert result.exact_eq(expected)

    @given(period=time_periods(), dates=pairs(dates(), sorted=True))
    def test_at_days(self, *, period: TimePeriod, dates: tuple[Date, Date]) -> None:
        start_date, end_date = dates
        with assume_does_not_raise(_ZonedDateTimePeriodInvalidError):
            result = period.at((start_date, end_date))
        expected = ZonedDateTimePeriod(
            start_date.at(period.start).assume_tz(UTC.key),
            end_date.at(period.end).assume_tz(UTC.key),
        )
        assert result.exact_eq(expected)

    @given(period=time_periods(), new_times=pairs(times()))
    def test_replace(self, *, period: TimePeriod, new_times: tuple[Time, Time]) -> None:
        new_start, new_end = new_times
        new = period.replace(start=new_start, end=new_end)
        assert new.start == new_start
        assert new.end == new_end

    @given(period=time_periods())
    @mark.parametrize("func", [param(repr), param(str)])
    def test_repr(self, *, period: TimePeriod, func: Callable[..., str]) -> None:
        result = func(period)
        assert search(
            r"^TimePeriod\(\d{2}:\d{2}:\d{2}(\.\d{1,6})?, \d{2}:\d{2}:\d{2}(\.\d{1,6})?\)$",
            result,
        )

    @given(data=data(), period=time_periods())
    def test_to_dict_and_from_dict(
        self, *, data: DataObject, period: TimePeriod
    ) -> None:
        dict_ = data.draw(sampled_from([period.to_dict(), period.to_py_dict()]))
        result = TimePeriod.from_dict(dict_)
        assert result == period


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


class TestToHours:
    @given(days=integers())
    def test_date_delta(self, *, days: int) -> None:
        with (
            assume_does_not_raise(ValueError, match=r"Out of range"),
            assume_does_not_raise(ValueError, match=r"days out of range"),
            assume_does_not_raise(
                OverflowError, match=r"Python int too large to convert to C long"
            ),
        ):
            delta = DateDelta(days=days)
        assert to_hours(delta) == (24 * days)

    @given(cls=sampled_from([TimeDelta, DateTimeDelta]), hours=integers())
    def test_time_or_date_time_delta(
        self, *, cls: type[TimeOrDateTimeDelta], hours: int
    ) -> None:
        with (
            assume_does_not_raise(ValueError, match=r"Out of range"),
            assume_does_not_raise(ValueError, match=r"hours out of range"),
            assume_does_not_raise(OverflowError, match=r"int too big to convert"),
            assume_does_not_raise(
                OverflowError, match=r"Python int too large to convert to C long"
            ),
        ):
            delta = cls(hours=hours)
        assert to_hours(delta) == hours

    @mark.parametrize(
        "delta", [param(DateDelta(months=1)), param(DateTimeDelta(months=1))]
    )
    def test_error_months(self, *, delta: DateOrDateTimeDelta) -> None:
        with raises(_ToHoursMonthsError, match=r"Delta must not contain months; got 1"):
            _ = to_hours(delta)

    @mark.parametrize(
        "delta", [param(TimeDelta(nanoseconds=1)), param(DateTimeDelta(nanoseconds=1))]
    )
    def test_error_nanoseconds(self, *, delta: TimeOrDateTimeDelta) -> None:
        with raises(
            _ToHoursNanosecondsError,
            match=r"Delta must not contain extra nanoseconds; got .*",
        ):
            _ = to_hours(delta)


class TestToMicroseconds:
    @given(days=integers())
    def test_date_delta(self, *, days: int) -> None:
        with (
            assume_does_not_raise(ValueError, match=r"Out of range"),
            assume_does_not_raise(ValueError, match=r"days out of range"),
            assume_does_not_raise(
                OverflowError, match=r"Python int too large to convert to C long"
            ),
        ):
            delta = DateDelta(days=days)
        assert to_microseconds(delta) == (24 * 60 * 60 * int(1e6) * days)

    @given(cls=sampled_from([TimeDelta, DateTimeDelta]), microseconds=integers())
    def test_time_or_date_time_delta(
        self, *, cls: type[TimeOrDateTimeDelta], microseconds: int
    ) -> None:
        with (
            assume_does_not_raise(ValueError, match=r"Out of range"),
            assume_does_not_raise(ValueError, match=r"microseconds out of range"),
            assume_does_not_raise(OverflowError, match=r"int too big to convert"),
            assume_does_not_raise(
                OverflowError, match=r"Python int too large to convert to C long"
            ),
        ):
            delta = cls(microseconds=microseconds)
        assert to_microseconds(delta) == microseconds

    @mark.parametrize(
        "delta", [param(DateDelta(months=1)), param(DateTimeDelta(months=1))]
    )
    def test_error_months(self, *, delta: DateOrDateTimeDelta) -> None:
        with raises(
            _ToMicrosecondsMonthsError, match=r"Delta must not contain months; got 1"
        ):
            _ = to_microseconds(delta)

    @mark.parametrize(
        "delta", [param(TimeDelta(nanoseconds=1)), param(DateTimeDelta(nanoseconds=1))]
    )
    def test_error_nanoseconds(self, *, delta: TimeOrDateTimeDelta) -> None:
        with raises(
            _ToMicrosecondsNanosecondsError,
            match=r"Delta must not contain extra nanoseconds; got .*",
        ):
            _ = to_microseconds(delta)


class TestToMilliseconds:
    @given(days=integers())
    def test_date_delta(self, *, days: int) -> None:
        with (
            assume_does_not_raise(ValueError, match=r"Out of range"),
            assume_does_not_raise(ValueError, match=r"days out of range"),
            assume_does_not_raise(
                OverflowError, match=r"Python int too large to convert to C long"
            ),
        ):
            delta = DateDelta(days=days)
        assert to_milliseconds(delta) == (24 * 60 * 60 * int(1e3) * days)

    @given(cls=sampled_from([TimeDelta, DateTimeDelta]), milliseconds=integers())
    def test_time_or_date_time_delta(
        self, *, cls: type[TimeOrDateTimeDelta], milliseconds: int
    ) -> None:
        with (
            assume_does_not_raise(ValueError, match=r"Out of range"),
            assume_does_not_raise(ValueError, match=r"milliseconds out of range"),
            assume_does_not_raise(OverflowError, match=r"int too big to convert"),
            assume_does_not_raise(
                OverflowError, match=r"Python int too large to convert to C long"
            ),
        ):
            delta = cls(milliseconds=milliseconds)
        assert to_milliseconds(delta) == milliseconds

    @mark.parametrize(
        "delta", [param(DateDelta(months=1)), param(DateTimeDelta(months=1))]
    )
    def test_error_months(self, *, delta: DateOrDateTimeDelta) -> None:
        with raises(
            _ToMillisecondsMonthsError, match=r"Delta must not contain months; got 1"
        ):
            _ = to_milliseconds(delta)

    @mark.parametrize(
        "delta", [param(TimeDelta(nanoseconds=1)), param(DateTimeDelta(nanoseconds=1))]
    )
    def test_error_nanoseconds(self, *, delta: TimeOrDateTimeDelta) -> None:
        with raises(
            _ToMillisecondsNanosecondsError,
            match=r"Delta must not contain extra nanoseconds; got .*",
        ):
            _ = to_milliseconds(delta)


class TestToMinutes:
    @given(days=integers())
    def test_date_delta(self, *, days: int) -> None:
        with (
            assume_does_not_raise(ValueError, match=r"Out of range"),
            assume_does_not_raise(ValueError, match=r"days out of range"),
            assume_does_not_raise(
                OverflowError, match=r"Python int too large to convert to C long"
            ),
        ):
            delta = DateDelta(days=days)
        assert to_minutes(delta) == (24 * 60 * days)

    @given(cls=sampled_from([TimeDelta, DateTimeDelta]), minutes=integers())
    def test_time_or_date_time_delta(
        self, *, cls: type[TimeOrDateTimeDelta], minutes: int
    ) -> None:
        with (
            assume_does_not_raise(ValueError, match=r"Out of range"),
            assume_does_not_raise(ValueError, match=r"minutes out of range"),
            assume_does_not_raise(OverflowError, match=r"int too big to convert"),
            assume_does_not_raise(
                OverflowError, match=r"Python int too large to convert to C long"
            ),
        ):
            delta = cls(minutes=minutes)
        assert to_minutes(delta) == minutes

    @mark.parametrize(
        "delta", [param(DateDelta(months=1)), param(DateTimeDelta(months=1))]
    )
    def test_error_months(self, *, delta: DateOrDateTimeDelta) -> None:
        with raises(
            _ToMinutesMonthsError, match=r"Delta must not contain months; got 1"
        ):
            _ = to_minutes(delta)

    @mark.parametrize(
        "delta", [param(TimeDelta(nanoseconds=1)), param(DateTimeDelta(nanoseconds=1))]
    )
    def test_error_nanoseconds(self, *, delta: TimeOrDateTimeDelta) -> None:
        with raises(
            _ToMinutesNanosecondsError,
            match=r"Delta must not contain extra nanoseconds; got .*",
        ):
            _ = to_minutes(delta)


class TestToMonths:
    @given(cls=sampled_from([DateDelta, DateTimeDelta]), months=integers())
    def test_main(self, *, cls: type[DateOrDateTimeDelta], months: int) -> None:
        with (
            assume_does_not_raise(ValueError, match=r"Out of range"),
            assume_does_not_raise(ValueError, match=r"months out of range"),
            assume_does_not_raise(
                OverflowError, match=r"Python int too large to convert to C long"
            ),
        ):
            delta = cls(months=months)
        assert to_months(delta) == months

    @mark.parametrize("delta", [param(DateDelta(days=1)), param(DateTimeDelta(days=1))])
    def test_error_days(self, *, delta: DateOrDateTimeDelta) -> None:
        with raises(_ToMonthsDaysError, match=r"Delta must not contain days; got 1"):
            _ = to_months(delta)

    def test_error_date_time_delta_time(self) -> None:
        delta = DateTimeDelta(nanoseconds=1)
        with raises(
            _ToMonthsTimeError, match=r"Delta must not contain a time part; got .*"
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
            assume_does_not_raise(ValueError, match=r"Out of range"),
            assume_does_not_raise(ValueError, match=r"Mixed sign in Date(Time)?Delta"),
            assume_does_not_raise(ValueError, match=r"months out of range"),
            assume_does_not_raise(ValueError, match=r"days out of range"),
            assume_does_not_raise(
                OverflowError, match=r"Python int too large to convert to C long"
            ),
        ):
            delta = cls(months=months, days=days)
        assert to_months_and_days(delta) == (months, days)

    def test_error_date_time_delta_time(self) -> None:
        delta = DateTimeDelta(nanoseconds=1)
        with raises(
            ToMonthsAndDaysError, match=r"Delta must not contain a time part; got .*"
        ):
            _ = to_months_and_days(delta)


class TestToNanoseconds:
    @given(func=sampled_from([to_time_delta, to_date_time_delta]), nanos=integers())
    def test_main(
        self, *, func: Callable[[int], TimeOrDateTimeDelta], nanos: int
    ) -> None:
        with (
            assume_does_not_raise(ValueError, match=r"Out of range"),
            assume_does_not_raise(ValueError, match=r"TimeDelta out of range"),
            assume_does_not_raise(ValueError, match=r"total days out of range"),
            assume_does_not_raise(
                OverflowError, match=r"Python int too large to convert to C long"
            ),
            assume_does_not_raise(OverflowError, match=r"int too big to convert"),
        ):
            delta = func(nanos)
        assert to_nanoseconds(delta) == nanos

    @mark.parametrize(
        "delta", [param(DateDelta(months=1)), param(DateTimeDelta(months=1))]
    )
    def test_error(self, *, delta: DateOrDateTimeDelta) -> None:
        with raises(ToNanosecondsError, match=r"Delta must not contain months; got 1"):
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
            ToPyTimeDeltaError, match=r"Time delta must not contain nanoseconds; got 1"
        ):
            _ = to_py_time_delta(delta)


class TestToSeconds:
    @given(days=integers())
    def test_date_delta(self, *, days: int) -> None:
        with (
            assume_does_not_raise(ValueError, match=r"Out of range"),
            assume_does_not_raise(ValueError, match=r"days out of range"),
            assume_does_not_raise(
                OverflowError, match=r"Python int too large to convert to C long"
            ),
        ):
            delta = DateDelta(days=days)
        assert to_seconds(delta) == (24 * 60 * 60 * days)

    @given(cls=sampled_from([TimeDelta, DateTimeDelta]), seconds=integers())
    def test_time_or_date_time_delta(
        self, *, cls: type[TimeOrDateTimeDelta], seconds: int
    ) -> None:
        with (
            assume_does_not_raise(ValueError, match=r"Out of range"),
            assume_does_not_raise(ValueError, match=r"seconds out of range"),
            assume_does_not_raise(OverflowError, match=r"int too big to convert"),
            assume_does_not_raise(
                OverflowError, match=r"Python int too large to convert to C long"
            ),
        ):
            delta = cls(seconds=seconds)
        assert to_seconds(delta) == seconds

    @mark.parametrize(
        "delta", [param(DateDelta(months=1)), param(DateTimeDelta(months=1))]
    )
    def test_error_months(self, *, delta: DateOrDateTimeDelta) -> None:
        with raises(
            _ToSecondsMonthsError, match=r"Delta must not contain months; got 1"
        ):
            _ = to_seconds(delta)

    @mark.parametrize(
        "delta", [param(TimeDelta(nanoseconds=1)), param(DateTimeDelta(nanoseconds=1))]
    )
    def test_error_nanoseconds(self, *, delta: TimeOrDateTimeDelta) -> None:
        with raises(
            _ToSecondsNanosecondsError,
            match=r"Delta must not contain extra nanoseconds; got .*",
        ):
            _ = to_seconds(delta)


class TestToTime:
    def test_default(self) -> None:
        first = get_today().at(to_time()).assume_tz(UTC.key)
        second = get_today().at(to_time()).assume_tz(UTC.key)
        assert abs(first - second) <= SECOND

    @given(time=times())
    def test_time(self, *, time: Time) -> None:
        assert to_time(time) == time

    @given(time=times())
    def test_str(self, *, time: Time) -> None:
        assert to_time(time.format_iso()) == time

    @given(time=times())
    def test_py_time(self, *, time: Time) -> None:
        assert to_time(time.py_time()) == time

    @given(time=times())
    def test_callable(self, *, time: Time) -> None:
        assert to_time(lambda: time) == time

    def test_none(self) -> None:
        first = get_today().at(to_time(None)).assume_tz(UTC.key)
        second = get_today().at(get_time()).assume_tz(UTC.key)
        assert abs(first - second) <= SECOND

    def test_sentinel(self) -> None:
        assert to_time(sentinel) is sentinel

    @given(times=pairs(times()))
    def test_replace_non_sentinel(self, *, times: tuple[Time, Time]) -> None:
        time1, time2 = times

        @dataclass(kw_only=True, slots=True)
        class Example:
            time: Time = field(default_factory=get_time)

            def replace(
                self, *, time: MaybeCallableTimeLike | Sentinel = sentinel
            ) -> Self:
                return replace_non_sentinel(self, time=to_time(time))

        obj = Example(time=time1)
        assert obj.time == time1
        assert obj.replace().time == time1
        assert obj.replace(time=time2).time == time2


class TestToWeeks:
    @given(cls=sampled_from([DateDelta, DateTimeDelta]), weeks=integers())
    def test_date_or_date_time_delta(
        self, *, cls: type[DateOrDateTimeDelta], weeks: int
    ) -> None:
        with (
            assume_does_not_raise(ValueError, match=r"Out of range"),
            assume_does_not_raise(ValueError, match=r"days out of range"),
            assume_does_not_raise(ValueError, match=r"weeks out of range"),
            assume_does_not_raise(
                OverflowError, match=r"Python int too large to convert to C long"
            ),
        ):
            delta = cls(weeks=weeks)
        assert to_weeks(delta) == weeks

    @given(weeks=integers())
    def test_time_delta(self, *, weeks: int) -> None:
        with (
            assume_does_not_raise(ValueError, match=r"Out of range"),
            assume_does_not_raise(ValueError, match=r"hours out of range"),
            assume_does_not_raise(OverflowError, match=r"int too big to convert"),
            assume_does_not_raise(
                OverflowError, match=r"Python int too large to convert to C long"
            ),
        ):
            delta = TimeDelta(hours=7 * 24 * weeks)
        assert to_weeks(delta) == weeks

    @mark.parametrize(
        "delta", [param(DateDelta(months=1)), param(DateTimeDelta(months=1))]
    )
    def test_error_months(self, *, delta: DateOrDateTimeDelta) -> None:
        with raises(_ToWeeksMonthsError, match=r"Delta must not contain months; got 1"):
            _ = to_weeks(delta)

    @mark.parametrize("delta", [param(DateDelta(days=8)), param(DateTimeDelta(days=8))])
    def test_error_days(self, *, delta: DateOrDateTimeDelta) -> None:
        with raises(
            _ToWeeksDaysError, match=r"Delta must not contain extra days; got 1"
        ):
            _ = to_weeks(delta)

    @mark.parametrize(
        "delta", [param(TimeDelta(nanoseconds=1)), param(DateTimeDelta(nanoseconds=1))]
    )
    def test_error_nanoseconds(self, *, delta: TimeOrDateTimeDelta) -> None:
        with raises(
            _ToWeeksNanosecondsError,
            match=r"Delta must not contain extra nanoseconds; got .*",
        ):
            _ = to_weeks(delta)


class TestToYears:
    @given(cls=sampled_from([DateDelta, DateTimeDelta]), years=integers())
    def test_main(self, *, cls: type[DateOrDateTimeDelta], years: int) -> None:
        with (
            assume_does_not_raise(ValueError, match=r"Out of range"),
            assume_does_not_raise(ValueError, match=r"months out of range"),
            assume_does_not_raise(ValueError, match=r"years out of range"),
            assume_does_not_raise(
                OverflowError, match=r"Python int too large to convert to C long"
            ),
        ):
            delta = cls(years=years)
        assert to_years(delta) == years

    @mark.parametrize(
        "delta", [param(DateDelta(months=1)), param(DateTimeDelta(months=1))]
    )
    def test_error_date_delta_months(self, *, delta: DateOrDateTimeDelta) -> None:
        with raises(_ToYearsMonthsError, match=r"Delta must not contain months; got 1"):
            _ = to_years(delta)

    @mark.parametrize("delta", [param(DateDelta(days=1)), param(DateTimeDelta(days=1))])
    def test_error_date_delta_days(self, *, delta: DateOrDateTimeDelta) -> None:
        with raises(_ToYearsDaysError, match=r"Delta must not contain days; got 1"):
            _ = to_years(delta)

    def test_error_date_time_delta_time(self) -> None:
        delta = DateTimeDelta(nanoseconds=1)
        with raises(
            _ToYearsTimeError, match=r"Delta must not contain a time part; got .*"
        ):
            _ = to_years(delta)


class TestToZonedDateTime:
    def test_default(self) -> None:
        assert abs(to_zoned_date_time() - get_now()) <= SECOND

    @given(date_time=zoned_date_times(time_zone=zone_infos()))
    def test_date_time_without_time_zone(self, *, date_time: ZonedDateTime) -> None:
        result = to_zoned_date_time(date_time)
        assert result.exact_eq(date_time)

    @given(date_time=zoned_date_times(), time_zone=zone_infos())
    def test_date_time_with_time_zone(
        self, *, date_time: ZonedDateTime, time_zone: ZoneInfo
    ) -> None:
        result = to_zoned_date_time(date_time, time_zone=time_zone)
        expected = date_time.to_tz(time_zone.key)
        assert result.exact_eq(expected)

    @given(data=data(), date_time=zoned_date_times(), time_zone=zone_infos())
    def test_str(
        self, *, data: DataObject, date_time: ZonedDateTime, time_zone: ZoneInfo
    ) -> None:
        text = date_time.format_iso()
        text_use = data.draw(sampled_from([text, text.replace("/", "_")]))
        result = to_zoned_date_time(text_use, time_zone=time_zone)
        expected = date_time.to_tz(time_zone.key)
        assert result.exact_eq(expected)

    @given(date_time=zoned_date_times_2000, time_zone=zone_infos())
    def test_py_date_time_zone_info(
        self, *, date_time: ZonedDateTime, time_zone: ZoneInfo
    ) -> None:
        result = to_zoned_date_time(date_time.py_datetime(), time_zone=time_zone)
        expected = date_time.to_tz(time_zone.key)
        assert result.exact_eq(expected)

    @given(date_time=zoned_date_times_2000, time_zone=zone_infos())
    def test_py_date_time_dt_utc(
        self, *, date_time: ZonedDateTime, time_zone: ZoneInfo
    ) -> None:
        result = to_zoned_date_time(
            date_time.py_datetime().astimezone(dt.UTC), time_zone=time_zone
        )
        expected = date_time.to_tz(time_zone.key)
        assert result.exact_eq(expected)

    @given(date_time=zoned_date_times(), time_zone=zone_infos())
    def test_callable(self, *, date_time: ZonedDateTime, time_zone: ZoneInfo) -> None:
        result = to_zoned_date_time(lambda: date_time, time_zone=time_zone)
        expected = date_time.to_tz(time_zone.key)
        assert result.exact_eq(expected)

    @given(time_zone=zone_infos())
    def test_none(self, *, time_zone: ZoneInfo) -> None:
        result = to_zoned_date_time(None, time_zone=time_zone)
        assert abs(result - get_now(time_zone)) <= SECOND

    @given(time_zone=zone_infos())
    def test_sentinel(self, *, time_zone: ZoneInfo) -> None:
        assert to_zoned_date_time(sentinel, time_zone=time_zone) is sentinel

    @given(date_times=pairs(zoned_date_times()))
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
    def test_parse_iso(self) -> None:
        result = two_digit_year_month(0, 1)
        expected = YearMonth(2000, 1)
        assert result == expected


class TestWheneverLogRecord:
    def test_init(self) -> None:
        _ = WheneverLogRecord("name", DEBUG, "pathname", 0, None, None, None)

    def test_get_length(self) -> None:
        assert isinstance(WheneverLogRecord._get_length(), int)


class TestZonedDateTimePeriod:
    @given(period=zoned_date_time_periods(), delta=time_deltas())
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_add(self, *, period: ZonedDateTimePeriod, delta: TimeDelta) -> None:
        with assume_does_not_raise(ValueError, match=r"Instant is out of range"):
            result = period + delta
        expected = ZonedDateTimePeriod(period.start + delta, period.end + delta)
        assert result == expected

    @given(datetime=zoned_date_times(), period=zoned_date_time_periods())
    def test_contains(
        self, *, datetime: ZonedDateTime, period: ZonedDateTimePeriod
    ) -> None:
        result = datetime in period
        expected = period.start <= datetime <= period.end
        assert result is expected

    @given(period=zoned_date_time_periods())
    def test_delta(self, *, period: ZonedDateTimePeriod) -> None:
        assert period.delta == (period.end - period.start)

    @given(period=zoned_date_time_periods())
    def test_exact_eq(self, *, period: ZonedDateTimePeriod) -> None:
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

    @given(period=zoned_date_time_periods())
    def test_hashable(self, *, period: ZonedDateTimePeriod) -> None:
        _ = hash(period)

    @given(period=zoned_date_time_periods())
    def test_replace(self, *, period: ZonedDateTimePeriod) -> None:
        new = period.replace(start=period.start - SECOND, end=period.end + SECOND)
        assert new.start == (period.start - SECOND)
        assert new.end == (period.end + SECOND)

    @given(period=zoned_date_time_periods())
    @mark.parametrize("func", [param(repr), param(str)])
    def test_repr(
        self, *, period: ZonedDateTimePeriod, func: Callable[..., str]
    ) -> None:
        result = func(period)
        assert search(
            r"^ZonedDateTimePeriod\(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?, \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?\[.+\]\)$",
            result,
        )

    @given(periods=pairs(zoned_date_time_periods()))
    def test_sortable(
        self, *, periods: tuple[ZonedDateTimePeriod, ZonedDateTimePeriod]
    ) -> None:
        _ = sorted(periods)

    @given(period=zoned_date_time_periods(), delta=time_deltas())
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_sub(self, *, period: ZonedDateTimePeriod, delta: TimeDelta) -> None:
        with assume_does_not_raise(ValueError, match=r"Instant is out of range"):
            result = period - delta
        expected = ZonedDateTimePeriod(period.start - delta, period.end - delta)
        assert result == expected

    @given(data=data(), datetimes=pairs(zoned_date_times_2000, sorted=True))
    def test_to_and_from_dict(
        self, data: DataObject, *, datetimes: tuple[ZonedDateTime, ZonedDateTime]
    ) -> None:
        start, end = datetimes
        period = ZonedDateTimePeriod(start, end)
        dict_ = data.draw(sampled_from([period.to_dict(), period.to_py_dict()]))
        result = ZonedDateTimePeriod.from_dict(dict_)
        assert result == period

    @given(period=zoned_date_time_periods())
    def test_to_tz(self, *, period: ZonedDateTimePeriod) -> None:
        with assume_does_not_raise(OverflowError, match=r"date value out of range"):
            result = period.to_tz(UTC)
        assert result.time_zone == UTC
        name = UTC.key
        expected = ZonedDateTimePeriod(period.start.to_tz(name), period.end.to_tz(name))
        assert result == expected

    @given(datetimes=pairs(zoned_date_times(), unique=True, sorted=True))
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_error_period_invalid(
        self, *, datetimes: tuple[ZonedDateTime, ZonedDateTime]
    ) -> None:
        start, end = datetimes
        with raises(
            _ZonedDateTimePeriodInvalidError, match=r"Invalid period; got .* > .*"
        ):
            _ = ZonedDateTimePeriod(end, start)

    @given(datetimes=pairs(plain_date_times(), sorted=True))
    def test_error_period_time_zone(
        self, *, datetimes: tuple[PlainDateTime, PlainDateTime]
    ) -> None:
        plain_start, plain_end = datetimes
        with assume_does_not_raise(OverflowError, match=r"date value out of range"):
            start = (plain_start - DAY).assume_tz(USCentral.key)
            end = (plain_end + DAY).assume_tz(USEastern.key)
        with raises(
            _ZonedDateTimePeriodTimeZoneError,
            match=r"Period must contain exactly one time zone; got .* and .*",
        ):
            _ = ZonedDateTimePeriod(start, end)

    @given(period=zoned_date_time_periods())
    def test_error_exact_eq(self, *, period: ZonedDateTimePeriod) -> None:
        with raises(
            _ZonedDateTimePeriodExactEqError, match=r"Invalid arguments; got \(.*\)"
        ):
            _ = period.exact_eq(cast("Any", None))
