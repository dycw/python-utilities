from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from math import isclose
from operator import eq, gt, lt
from re import search
from typing import TYPE_CHECKING, Any, Self

from hypothesis import HealthCheck, assume, given, settings
from hypothesis.strategies import (
    DataObject,
    booleans,
    data,
    dates,
    datetimes,
    floats,
    integers,
    none,
    permutations,
    sampled_from,
    timedeltas,
    timezones,
)
from pytest import mark, param, raises

from utilities.dataclasses import replace_non_sentinel
from utilities.datetime import (
    _MICROSECONDS_PER_MILLISECOND,
    DAY,
    EPOCH_DATE,
    EPOCH_NAIVE,
    EPOCH_UTC,
    HALF_YEAR,
    HOUR,
    MICROSECOND,
    MILLISECOND,
    MINUTE,
    MONTH,
    NOW_UTC,
    QUARTER,
    SECOND,
    TODAY_UTC,
    WEEK,
    YEAR,
    ZERO_TIME,
    AddDurationError,
    AddWeekdaysError,
    AreEqualDatesOrDateTimesError,
    AreEqualDateTimesError,
    CheckDateNotDateTimeError,
    EnsureMonthError,
    GetMinMaxDateError,
    MeanDateTimeError,
    MeanTimeDeltaError,
    MillisecondsSinceEpochError,
    Month,
    MonthError,
    ParseDateCompactError,
    ParseDateTimeCompactError,
    ParseMonthError,
    SerializeCompactError,
    SubDurationError,
    TimedeltaToMillisecondsError,
    YieldDaysError,
    YieldWeekdaysError,
    _DateDurationToIntFloatError,
    _DateDurationToIntTimeDeltaError,
    _DateDurationToTimeDeltaFloatError,
    _DateDurationToTimeDeltaTimeDeltaError,
    _GetMinMaxDateMaxAgeError,
    _GetMinMaxDateMaxDateError,
    _GetMinMaxDateMinAgeError,
    _GetMinMaxDateMinDateError,
    _GetMinMaxDatePeriodError,
    _ParseTwoDigitYearInvalidIntegerError,
    _ParseTwoDigitYearInvalidStringError,
    add_duration,
    add_weekdays,
    are_equal_date_durations,
    are_equal_dates_or_datetimes,
    are_equal_datetime_durations,
    are_equal_datetimes,
    are_equal_months,
    check_date_not_datetime,
    date_duration_to_int,
    date_duration_to_timedelta,
    date_to_datetime,
    date_to_month,
    datetime_duration_to_float,
    datetime_duration_to_timedelta,
    datetime_utc,
    days_since_epoch,
    days_since_epoch_to_date,
    ensure_month,
    format_datetime_local_and_utc,
    get_date,
    get_datetime,
    get_half_years,
    get_min_max_date,
    get_months,
    get_now,
    get_quarters,
    get_today,
    get_years,
    is_integral_timedelta,
    is_local_datetime,
    is_weekday,
    is_zero_time,
    is_zoned_datetime,
    maybe_sub_pct_y,
    mean_datetime,
    mean_timedelta,
    microseconds_since_epoch,
    microseconds_since_epoch_to_datetime,
    microseconds_to_timedelta,
    milliseconds_since_epoch,
    milliseconds_since_epoch_to_datetime,
    milliseconds_to_timedelta,
    parse_date_compact,
    parse_datetime_compact,
    parse_month,
    parse_two_digit_year,
    round_datetime,
    round_to_next_weekday,
    round_to_prev_weekday,
    serialize_compact,
    serialize_month,
    sub_duration,
    timedelta_since_epoch,
    timedelta_to_microseconds,
    timedelta_to_milliseconds,
    yield_days,
    yield_weekdays,
)
from utilities.functions import not_func
from utilities.hypothesis import (
    assume_does_not_raise,
    date_durations,
    int32s,
    months,
    pairs,
    sentinels,
    text_clean,
    zoned_datetimes,
)
from utilities.math import MAX_INT32, MIN_INT32, is_integral, round_to_float
from utilities.sentinel import Sentinel, sentinel
from utilities.tzdata import HongKong
from utilities.zoneinfo import UTC

if TYPE_CHECKING:
    from collections.abc import Callable
    from zoneinfo import ZoneInfo

    from utilities.sentinel import Sentinel
    from utilities.types import (
        DateOrDateTime,
        Duration,
        MaybeCallableDate,
        MaybeCallableDateTime,
        Number,
    )


class TestAddDuration:
    @given(date=dates(), n=integers())
    def test_date_with_int(self, *, date: dt.date, n: int) -> None:
        with assume_does_not_raise(OverflowError):
            result = add_duration(date, duration=n)
        expected = date + dt.timedelta(days=n)
        assert result == expected

    @given(date=dates(), n=integers())
    def test_date_with_timedelta(self, *, date: dt.date, n: int) -> None:
        with assume_does_not_raise(OverflowError):
            timedelta = dt.timedelta(days=n)
        with assume_does_not_raise(OverflowError):
            result = add_duration(date, duration=timedelta)
        expected = date + timedelta
        assert result == expected

    @given(datetime=zoned_datetimes(), n=integers())
    def test_datetime_with_int(self, *, datetime: dt.datetime, n: int) -> None:
        with assume_does_not_raise(OverflowError):
            result = add_duration(datetime, duration=n)
        expected = datetime + dt.timedelta(seconds=n)
        assert result == expected

    @given(datetime=zoned_datetimes(), n=integers())
    def test_datetime_with_timedelta(self, *, datetime: dt.datetime, n: int) -> None:
        with assume_does_not_raise(OverflowError):
            timedelta = dt.timedelta(seconds=n)
        with assume_does_not_raise(OverflowError):
            result = add_duration(datetime, duration=timedelta)
        expected = datetime + timedelta
        assert result == expected

    @given(date=dates() | zoned_datetimes())
    def test_none(self, *, date: DateOrDateTime) -> None:
        result = add_duration(date)
        assert result == date

    @given(
        date=dates(),
        n=integers(),
        frac=timedeltas(
            min_value=-(DAY - MICROSECOND), max_value=DAY - MICROSECOND
        ).filter(not_func(is_zero_time)),
    )
    def test_error(self, *, date: dt.date, n: int, frac: dt.timedelta) -> None:
        with assume_does_not_raise(OverflowError):
            timedelta = dt.timedelta(days=n) + frac
        with raises(
            AddDurationError,
            match="Date .* must be paired with an integral duration; got .*",
        ):
            _ = add_duration(date, duration=timedelta)


class TestAddWeekdays:
    @given(date=dates(), n=integers(-10, 10))
    @mark.parametrize("predicate", [param(gt), param(lt)])
    def test_add(
        self, *, date: dt.date, n: int, predicate: Callable[[Any, Any], bool]
    ) -> None:
        _ = assume(predicate(n, 0))
        with assume_does_not_raise(OverflowError):
            result = add_weekdays(date, n=n)
        assert is_weekday(result)
        assert predicate(result, date)

    @given(date=dates())
    def test_zero(self, *, date: dt.date) -> None:
        _ = assume(is_weekday(date))
        result = add_weekdays(date, n=0)
        assert result == date

    @given(date=dates())
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_error(self, *, date: dt.date) -> None:
        _ = assume(not is_weekday(date))
        with raises(AddWeekdaysError):
            _ = add_weekdays(date, n=0)

    @given(date=dates(), ns=pairs(integers(-10, 10)))
    def test_two(self, *, date: dt.date, ns: tuple[int, int]) -> None:
        with assume_does_not_raise(AddWeekdaysError, OverflowError):
            weekday1, weekday2 = (add_weekdays(date, n=n) for n in ns)
        result = weekday1 <= weekday2
        n1, n2 = ns
        expected = n1 <= n2
        assert result is expected


class TestAreEqualDateDurations:
    @given(x=integers(), y=integers())
    def test_ints(self, *, x: int, y: int) -> None:
        with assume_does_not_raise(OverflowError):
            result = are_equal_date_durations(x, y)
        expected = x == y
        assert result is expected

    @given(x=integers(), y=integers())
    def test_timedeltas(self, *, x: int, y: int) -> None:
        with assume_does_not_raise(OverflowError):
            x_timedelta, y_timedelta = dt.timedelta(days=x), dt.timedelta(days=y)
        result = are_equal_date_durations(x_timedelta, y_timedelta)
        expected = x == y
        assert result is expected

    @given(data=data(), x=integers(), y=integers())
    def test_int_vs_timedelta(self, *, data: DataObject, x: int, y: int) -> None:
        with assume_does_not_raise(OverflowError):
            y_timedelta = dt.timedelta(days=y)
        left, right = data.draw(permutations([x, y_timedelta]))
        with assume_does_not_raise(OverflowError):
            result = are_equal_date_durations(left, right)
        expected = x == y
        assert result is expected


class TestAreEqualDateOrDateTimes:
    @given(x=dates(), y=dates())
    def test_dates(self, *, x: dt.date, y: dt.date) -> None:
        result = are_equal_dates_or_datetimes(x, y)
        expected = x == y
        assert result is expected

    @given(x=datetimes(), y=datetimes())
    def test_datetimes(self, *, x: dt.datetime, y: dt.datetime) -> None:
        result = are_equal_dates_or_datetimes(x, y)
        expected = x == y
        assert result is expected

    @given(data=data(), x=dates(), y=datetimes())
    def test_date_vs_datetime(
        self, *, data: DataObject, x: dt.date, y: dt.datetime
    ) -> None:
        left, right = data.draw(permutations([x, y]))
        with raises(
            AreEqualDatesOrDateTimesError,
            match=r"Cannot compare date and datetime \(.*, .*\)",
        ):
            _ = are_equal_dates_or_datetimes(left, right)


class TestAreEqualDateTimeDurations:
    @given(x=integers(), y=integers())
    def test_ints(self, *, x: int, y: int) -> None:
        with assume_does_not_raise(OverflowError):
            result = are_equal_datetime_durations(x, y)
        expected = x == y
        assert result is expected

    @given(x=timedeltas(), y=timedeltas())
    def test_timedeltas(self, *, x: dt.timedelta, y: dt.timedelta) -> None:
        result = are_equal_datetime_durations(x, y)
        expected = x == y
        assert result is expected

    @given(data=data(), x=integers(), y=timedeltas())
    def test_int_vs_timedelta(
        self, *, data: DataObject, x: int, y: dt.timedelta
    ) -> None:
        left, right = data.draw(permutations([x, y]))
        with assume_does_not_raise(OverflowError):
            result = are_equal_datetime_durations(left, right)
        expected = x == datetime_duration_to_float(y)
        assert result is expected


class TestAreEqualDateTimes:
    @given(x=datetimes(), y=datetimes())
    def test_local(self, *, x: dt.datetime, y: dt.datetime) -> None:
        result = are_equal_datetimes(x, y)
        expected = x == y
        assert result is expected

    @given(
        x=zoned_datetimes(time_zone=timezones()),
        y=zoned_datetimes(time_zone=timezones()),
    )
    def test_zoned_non_strict(self, *, x: dt.datetime, y: dt.datetime) -> None:
        result = are_equal_datetimes(x, y)
        expected = x == y
        assert result is expected

    @given(
        x=zoned_datetimes(time_zone=UTC),
        y=zoned_datetimes(time_zone=UTC),
        time_zone1=timezones(),
        time_zone2=timezones(),
    )
    def test_zoned_strict(
        self,
        *,
        x: dt.datetime,
        y: dt.datetime,
        time_zone1: ZoneInfo,
        time_zone2: ZoneInfo,
    ) -> None:
        with assume_does_not_raise(OverflowError, match="date value out of range"):
            x1 = x.astimezone(time_zone1)
            y2 = y.astimezone(time_zone2)
        result = are_equal_datetimes(x1, y2, strict=True)
        expected = (x == y) and (time_zone1 is time_zone2)
        assert result is expected

    @given(data=data(), x=datetimes(), y=zoned_datetimes(time_zone=timezones()))
    def test_local_vs_zoned(
        self, *, data: DataObject, x: dt.datetime, y: dt.datetime
    ) -> None:
        left, right = data.draw(permutations([x, y]))
        with raises(
            AreEqualDateTimesError,
            match=r"Cannot compare local and zoned datetimes \(.*, .*\)",
        ):
            _ = are_equal_datetimes(left, right)


class TestAreEqualMonths:
    @given(x=dates(), y=dates())
    def test_dates(self, *, x: dt.date, y: dt.date) -> None:
        result = are_equal_months(x, y)
        expected = (x.year == y.year) and (x.month == y.month)
        assert result is expected

    @given(x=months(), y=months())
    def test_months(self, *, x: Month, y: Month) -> None:
        result = are_equal_months(x, y)
        expected = x == y
        assert result is expected

    @given(data=data(), x=dates(), y=months())
    def test_date_vs_month(self, *, data: DataObject, x: dt.date, y: Month) -> None:
        left, right = data.draw(permutations([x, y]))
        result = are_equal_months(left, right)
        expected = (x.year == y.year) and (x.month == y.month)
        assert result is expected


class TestCheckDateNotDateTime:
    @given(date=dates())
    def test_main(self, *, date: dt.date) -> None:
        check_date_not_datetime(date)

    @given(datetime=datetimes())
    def test_error(self, *, datetime: dt.datetime) -> None:
        with raises(
            CheckDateNotDateTimeError, match="Date must not be a datetime; got .*"
        ):
            check_date_not_datetime(datetime)


class TestDateDurationToInt:
    @given(n=integers())
    def test_int(self, *, n: int) -> None:
        result = date_duration_to_int(n)
        assert result == n

    @given(n=integers().map(float))
    def test_float_integral(self, *, n: float) -> None:
        result = date_duration_to_int(n)
        assert result == round(n)

    @given(n=integers())
    def test_timedelta(self, *, n: int) -> None:
        with assume_does_not_raise(OverflowError):
            timedelta = dt.timedelta(days=n)
        result = date_duration_to_int(timedelta)
        assert result == n

    @given(n=floats(allow_nan=False, allow_infinity=False))
    def test_error_float(self, *, n: float) -> None:
        _ = assume(not is_integral(n))
        with raises(
            _DateDurationToIntFloatError,
            match="Float duration must be integral; got .*",
        ):
            _ = date_duration_to_int(n)

    @given(
        n=integers(),
        frac=timedeltas(
            min_value=-(DAY - MICROSECOND), max_value=DAY - MICROSECOND
        ).filter(not_func(is_zero_time)),
    )
    def test_error_timedelta(self, *, n: int, frac: dt.timedelta) -> None:
        with assume_does_not_raise(OverflowError):
            timedelta = dt.timedelta(days=n) + frac
        with raises(
            _DateDurationToIntTimeDeltaError,
            match="Timedelta duration must be integral; got .*",
        ):
            _ = date_duration_to_int(timedelta)


class TestDateDurationToTimeDelta:
    @given(n=integers())
    def test_int(self, *, n: int) -> None:
        with assume_does_not_raise(OverflowError):
            result = date_duration_to_timedelta(n)
        expected = dt.timedelta(days=n)
        assert result == expected

    @given(n=integers().map(float))
    def test_float_integral(self, *, n: float) -> None:
        with assume_does_not_raise(OverflowError):
            result = date_duration_to_timedelta(n)
        expected = dt.timedelta(days=round(n))
        assert result == expected

    @given(n=integers())
    def test_timedelta(self, *, n: int) -> None:
        with assume_does_not_raise(OverflowError):
            timedelta = dt.timedelta(days=n)
        result = date_duration_to_timedelta(timedelta)
        assert result == timedelta

    @given(n=floats(allow_nan=False, allow_infinity=False))
    def test_error_float(self, *, n: float) -> None:
        _ = assume(not is_integral(n))
        with raises(
            _DateDurationToTimeDeltaFloatError,
            match="Float duration must be integral; got .*",
        ):
            _ = date_duration_to_timedelta(n)

    @given(
        n=integers(),
        frac=timedeltas(
            min_value=-(DAY - MICROSECOND), max_value=DAY - MICROSECOND
        ).filter(not_func(is_zero_time)),
    )
    def test_error_timedelta(self, *, n: int, frac: dt.timedelta) -> None:
        with assume_does_not_raise(OverflowError):
            timedelta = dt.timedelta(days=n) + frac
        with raises(
            _DateDurationToTimeDeltaTimeDeltaError,
            match="Timedelta duration must be integral; got .*",
        ):
            _ = date_duration_to_timedelta(timedelta)


class TestDateTimeDurationToFloat:
    @given(n=int32s())
    def test_int(self, *, n: int) -> None:
        result = datetime_duration_to_float(n)
        assert result == n

    @given(n=floats(allow_nan=False, allow_infinity=False))
    def test_float(self, *, n: Number) -> None:
        result = datetime_duration_to_float(n)
        assert result == n

    @given(timedelta=timedeltas())
    def test_timedelta(self, *, timedelta: dt.timedelta) -> None:
        result = datetime_duration_to_float(timedelta)
        assert result == timedelta.total_seconds()


class TestDateTimeDurationToTimeDelta:
    @given(n=int32s())
    def test_int(self, *, n: int) -> None:
        result = datetime_duration_to_timedelta(n)
        assert result.total_seconds() == n

    @given(n=floats(min_value=MIN_INT32, max_value=MAX_INT32))
    def test_float(self, *, n: float) -> None:
        n = round_to_float(n, 1e-6)
        with assume_does_not_raise(OverflowError):
            result = datetime_duration_to_timedelta(n)
        assert isclose(result.total_seconds(), n)

    @given(duration=timedeltas())
    def test_timedelta(self, *, duration: dt.timedelta) -> None:
        result = datetime_duration_to_timedelta(duration)
        assert result == duration


class TestDateToDateTime:
    @given(date=dates())
    def test_main(self, *, date: dt.date) -> None:
        result = date_to_datetime(date).date()
        assert result == date


class TestDateToMonth:
    @given(date=dates())
    def test_main(self, *, date: dt.date) -> None:
        result = date_to_month(date).to_date(day=date.day)
        assert result == date


class TestDatetimeUTC:
    @given(datetime=zoned_datetimes())
    def test_main(self, *, datetime: dt.datetime) -> None:
        result = datetime_utc(
            datetime.year,
            datetime.month,
            datetime.day,
            hour=datetime.hour,
            minute=datetime.minute,
            second=datetime.second,
            microsecond=datetime.microsecond,
        )
        assert result == datetime


class TestDaysSinceEpoch:
    @given(date=dates())
    def test_main(self, *, date: dt.date) -> None:
        days = days_since_epoch(date)
        result = days_since_epoch_to_date(days)
        assert result == date


class TestEpoch:
    def test_date(self) -> None:
        assert isinstance(EPOCH_DATE, dt.date)
        assert not isinstance(EPOCH_DATE, dt.datetime)

    @mark.parametrize(
        ("epoch", "time_zone"), [param(EPOCH_NAIVE, None), param(EPOCH_UTC, UTC)]
    )
    def test_datetime(self, *, epoch: dt.datetime, time_zone: ZoneInfo | None) -> None:
        assert isinstance(EPOCH_UTC, dt.datetime)
        assert epoch.tzinfo is time_zone


class TestFormatDateTimeLocalAndUTC:
    @mark.parametrize(
        ("datetime", "expected"),
        [
            param(
                dt.datetime(2000, 1, 1, 2, 3, 4, tzinfo=UTC),
                "2000-01-01 02:03:04 (Sat, UTC)",
            ),
            param(
                dt.datetime(2000, 1, 1, 2, 3, 4, tzinfo=HongKong),
                "2000-01-01 02:03:04 (Sat, Asia/Hong_Kong, 1999-12-31 18:03:04 UTC)",
            ),
            param(
                dt.datetime(2000, 2, 1, 2, 3, 4, tzinfo=HongKong),
                "2000-02-01 02:03:04 (Tue, Asia/Hong_Kong, 01-31 18:03:04 UTC)",
            ),
            param(
                dt.datetime(2000, 2, 2, 2, 3, 4, tzinfo=HongKong),
                "2000-02-02 02:03:04 (Wed, Asia/Hong_Kong, 02-01 18:03:04 UTC)",
            ),
            param(
                dt.datetime(2000, 2, 2, 14, 3, 4, tzinfo=HongKong),
                "2000-02-02 14:03:04 (Wed, Asia/Hong_Kong, 06:03:04 UTC)",
            ),
        ],
    )
    def test_main(self, *, datetime: dt.datetime, expected: str) -> None:
        result = format_datetime_local_and_utc(datetime)
        assert result == expected


class TestGetDate:
    @given(date=dates())
    def test_date(self, *, date: dt.date) -> None:
        assert get_date(date=date) == date

    @given(date=none() | sentinels())
    def test_none_or_sentinel(self, *, date: None | Sentinel) -> None:
        assert get_date(date=date) is date

    @given(date1=dates(), date2=dates())
    def test_replace_non_sentinel(self, *, date1: dt.date, date2: dt.date) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            date: dt.date = field(default_factory=get_today)

            def replace(self, *, date: MaybeCallableDate | Sentinel = sentinel) -> Self:
                return replace_non_sentinel(self, date=get_date(date=date))

        obj = Example(date=date1)
        assert obj.date == date1
        assert obj.replace().date == date1
        assert obj.replace(date=date2).date == date2
        assert obj.replace(date=get_today).date == get_today()

    @given(date=dates())
    def test_callable(self, *, date: dt.date) -> None:
        assert get_date(date=lambda: date) == date


class TestGetDateTime:
    @given(datetime=zoned_datetimes())
    def test_datetime(self, *, datetime: dt.datetime) -> None:
        assert get_datetime(datetime=datetime) == datetime

    @given(datetime=none() | sentinels())
    def test_none_or_sentinel(self, *, datetime: None | Sentinel) -> None:
        assert get_datetime(datetime=datetime) is datetime

    @given(datetime1=datetimes(), datetime2=datetimes())
    def test_replace_non_sentinel(
        self, *, datetime1: dt.datetime, datetime2: dt.datetime
    ) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            datetime: dt.datetime = field(default_factory=get_now)

            def replace(
                self, *, datetime: MaybeCallableDateTime | Sentinel = sentinel
            ) -> Self:
                return replace_non_sentinel(
                    self, datetime=get_datetime(datetime=datetime)
                )

        obj = Example(datetime=datetime1)
        assert obj.datetime == datetime1
        assert obj.replace().datetime == datetime1
        assert obj.replace(datetime=datetime2).datetime == datetime2
        assert abs(obj.replace(datetime=get_now).datetime - get_now()) <= SECOND

    @given(datetime=zoned_datetimes())
    def test_callable(self, *, datetime: dt.datetime) -> None:
        assert get_datetime(datetime=lambda: datetime) == datetime


class TestGetMinMaxDate:
    @given(
        min_date=dates(max_value=TODAY_UTC) | none(),
        max_date=dates(max_value=TODAY_UTC) | none(),
        min_age=date_durations(min_int=0, min_timedelta=ZERO_TIME) | none(),
        max_age=date_durations(min_int=0, min_timedelta=ZERO_TIME) | none(),
    )
    def test_main(
        self,
        *,
        min_date: dt.date | None,
        max_date: dt.date | None,
        min_age: Duration | None,
        max_age: Duration | None,
    ) -> None:
        with assume_does_not_raise(GetMinMaxDateError, OverflowError):
            min_date_use, max_date_use = get_min_max_date(
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

    @given(date=dates(min_value=TODAY_UTC + DAY))
    def test_error_min_date(self, *, date: dt.date) -> None:
        with raises(
            _GetMinMaxDateMinDateError,
            match="Min date must be at most today; got .* > .*",
        ):
            _ = get_min_max_date(min_date=date)

    @given(duration=date_durations(max_int=-1, max_timedelta=-DAY))
    def test_error_min_age(self, *, duration: Duration) -> None:
        with raises(
            _GetMinMaxDateMinAgeError, match="Min age must be non-negative; got .*"
        ):
            _ = get_min_max_date(min_age=duration)

    @given(date=dates(min_value=TODAY_UTC + DAY))
    def test_error_max_date(self, *, date: dt.date) -> None:
        with raises(
            _GetMinMaxDateMaxDateError,
            match="Max date must be at most today; got .* > .*",
        ):
            _ = get_min_max_date(max_date=date)

    @given(duration=date_durations(max_int=-1, max_timedelta=-DAY))
    def test_error_max_age(self, *, duration: Duration) -> None:
        with raises(
            _GetMinMaxDateMaxAgeError, match="Max age must be non-negative; got .*"
        ):
            _ = get_min_max_date(max_age=duration)

    @given(dates=pairs(dates(max_value=TODAY_UTC), unique=True, sorted=True))
    def test_error_period(self, *, dates: tuple[dt.date, dt.date]) -> None:
        with raises(
            _GetMinMaxDatePeriodError,
            match="Min date must be at most max date; got .* > .*",
        ):
            _ = get_min_max_date(min_date=dates[1], max_date=dates[0])


class TestGetNow:
    @given(time_zone=timezones())
    def test_function(self, *, time_zone: ZoneInfo) -> None:
        now = get_now(time_zone=time_zone)
        assert isinstance(now, dt.datetime)
        assert now.tzinfo is time_zone

    def test_constant(self) -> None:
        assert isinstance(NOW_UTC, dt.datetime)
        assert NOW_UTC.tzinfo is UTC


class TestGetTimedelta:
    @given(n=integers(-10, 10))
    @mark.parametrize(
        "get_timedelta",
        [
            param(get_months),
            param(get_quarters),
            param(get_half_years),
            param(get_years),
        ],
    )
    def test_getters(
        self, *, get_timedelta: Callable[..., dt.timedelta], n: int
    ) -> None:
        assert isinstance(get_timedelta(n=n), dt.timedelta)

    @given(timedelta=sampled_from([MONTH, QUARTER, HALF_YEAR, YEAR]))
    def test_constants(self, *, timedelta: dt.timedelta) -> None:
        assert isinstance(timedelta, dt.timedelta)


class TestGetToday:
    @given(time_zone=timezones())
    def test_function(self, *, time_zone: ZoneInfo) -> None:
        today = get_today(time_zone=time_zone)
        assert isinstance(today, dt.date)

    def test_constant(self) -> None:
        assert isinstance(TODAY_UTC, dt.date)


class TestIsIntegralTimeDelta:
    @given(n=integers())
    def test_integral(self, *, n: int) -> None:
        with assume_does_not_raise(OverflowError):
            timedelta = dt.timedelta(days=n)
        assert is_integral_timedelta(timedelta)

    @given(
        n=integers(),
        frac=timedeltas(
            min_value=-(DAY - MICROSECOND), max_value=DAY - MICROSECOND
        ).filter(not_func(is_zero_time)),
    )
    def test_non_integral(self, *, n: int, frac: dt.timedelta) -> None:
        with assume_does_not_raise(OverflowError):
            timedelta = dt.timedelta(days=n) + frac
        assert not is_integral_timedelta(timedelta)


class TestIsLocalDateTime:
    @mark.parametrize(
        ("obj", "expected"),
        [
            param(None, False),
            param(dt.datetime(2000, 1, 1, tzinfo=UTC).replace(tzinfo=None), True),
            param(dt.datetime(2000, 1, 1, tzinfo=UTC), False),
        ],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        result = is_local_datetime(obj)
        assert result is expected


class TestIsWeekday:
    @given(date=dates())
    def test_main(self, *, date: dt.date) -> None:
        result = is_weekday(date)
        name = date.strftime("%A")
        expected = name in {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday"}
        assert result is expected


class TestIsZeroTime:
    @given(case=sampled_from([(ZERO_TIME, True), (SECOND, False)]))
    def test_main(self, *, case: tuple[dt.timedelta, bool]) -> None:
        timedelta, expected = case
        result = is_zero_time(timedelta)
        assert result is expected


class TestIsZonedDateTime:
    @mark.parametrize(
        ("obj", "expected"),
        [
            param(None, False),
            param(dt.datetime(2000, 1, 1, tzinfo=UTC).replace(tzinfo=None), False),
            param(dt.datetime(2000, 1, 1, tzinfo=UTC), True),
        ],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        result = is_zoned_datetime(obj)
        assert result is expected


class TestMaybeSubPctY:
    @given(text=text_clean())
    def test_main(self, *, text: str) -> None:
        result = maybe_sub_pct_y(text)
        _ = assume(not search("%Y", result))
        assert not search("%Y", result)


class TestMeanDateTime:
    @given(datetime=zoned_datetimes())
    def test_one(self, *, datetime: dt.datetime) -> None:
        assert mean_datetime([datetime]) == datetime

    def test_many(self) -> None:
        assert mean_datetime([NOW_UTC, NOW_UTC + MINUTE]) == (NOW_UTC + 30 * SECOND)

    def test_weights(self) -> None:
        assert mean_datetime([NOW_UTC, NOW_UTC + MINUTE], weights=[1, 3]) == (
            NOW_UTC + 45 * SECOND
        )

    def test_error(self) -> None:
        with raises(MeanDateTimeError, match="Mean requires at least 1 datetime"):
            _ = mean_datetime([])


class TestMeanTimeDelta:
    @given(timedelta=timedeltas())
    def test_one(self, *, timedelta: dt.timedelta) -> None:
        assert mean_timedelta([timedelta]) == timedelta

    def test_many(self) -> None:
        assert mean_timedelta([MINUTE, 2 * MINUTE]) == 1.5 * MINUTE

    def test_weights(self) -> None:
        assert mean_timedelta([MINUTE, 2 * MINUTE], weights=[1, 3]) == 1.75 * MINUTE

    def test_error(self) -> None:
        with raises(MeanTimeDeltaError, match="Mean requires at least 1 timedelta"):
            _ = mean_timedelta([])


class TestMicrosecondsOrMillisecondsSinceEpoch:
    @given(datetime=datetimes() | zoned_datetimes())
    def test_datetime_to_microseconds(self, *, datetime: dt.datetime) -> None:
        microseconds = microseconds_since_epoch(datetime)
        result = microseconds_since_epoch_to_datetime(
            microseconds, time_zone=datetime.tzinfo
        )
        assert result == datetime

    @given(microseconds=integers())
    def test_microseconds_to_datetime(self, *, microseconds: int) -> None:
        with assume_does_not_raise(OverflowError):
            datetime = microseconds_since_epoch_to_datetime(microseconds)
        result = microseconds_since_epoch(datetime)
        assert result == microseconds

    @given(datetime=datetimes() | zoned_datetimes())
    @mark.parametrize("strict", [param(True), param(False)])  # use mark.parametrize
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_datetime_to_milliseconds_exact(
        self, *, datetime: dt.datetime, strict: bool
    ) -> None:
        _ = assume(datetime.microsecond == 0)
        milliseconds = milliseconds_since_epoch(datetime, strict=strict)
        if strict:
            assert isinstance(milliseconds, int)
        else:
            assert milliseconds == round(milliseconds)
        result = milliseconds_since_epoch_to_datetime(
            round(milliseconds), time_zone=datetime.tzinfo
        )
        assert result == datetime

    @given(datetime=datetimes() | zoned_datetimes())
    def test_datetime_to_milliseconds_error(self, *, datetime: dt.datetime) -> None:
        _, microseconds = divmod(datetime.microsecond, _MICROSECONDS_PER_MILLISECOND)
        _ = assume(microseconds != 0)
        with raises(
            MillisecondsSinceEpochError,
            match=r"Unable to convert .* to milliseconds since epoch; got .* microsecond\(s\)",
        ):
            _ = milliseconds_since_epoch(datetime, strict=True)

    @given(milliseconds=integers())
    def test_milliseconds_to_datetime(self, *, milliseconds: int) -> None:
        with assume_does_not_raise(OverflowError):
            datetime = milliseconds_since_epoch_to_datetime(milliseconds)
        result = milliseconds_since_epoch(datetime)
        assert result == milliseconds


class TestMonth:
    @mark.parametrize(
        ("month", "n", "expected"),
        [
            param(Month(2000, 1), -2, Month(1999, 11)),
            param(Month(2000, 1), -1, Month(1999, 12)),
            param(Month(2000, 1), 0, Month(2000, 1)),
            param(Month(2000, 1), 1, Month(2000, 2)),
            param(Month(2000, 1), 2, Month(2000, 3)),
            param(Month(2000, 1), 11, Month(2000, 12)),
            param(Month(2000, 1), 12, Month(2001, 1)),
        ],
    )
    def test_add(self, *, month: Month, n: int, expected: Month) -> None:
        result = month + n
        assert result == expected

    @mark.parametrize(
        ("x", "y", "expected"),
        [
            param(Month(2000, 1), Month(1999, 11), 2),
            param(Month(2000, 1), Month(1999, 12), 1),
            param(Month(2000, 1), Month(2000, 1), 0),
            param(Month(2000, 1), Month(2000, 2), -1),
            param(Month(2000, 1), Month(2000, 3), -2),
            param(Month(2000, 1), Month(2000, 12), -11),
            param(Month(2000, 1), Month(2001, 1), -12),
        ],
    )
    def test_diff(self, *, x: Month, y: Month, expected: int) -> None:
        result = x - y
        assert result == expected

    @given(month=months())
    def test_hashable(self, *, month: Month) -> None:
        _ = hash(month)

    @mark.parametrize("func", [param(repr), param(str)])
    def test_repr(self, *, func: Callable[..., str]) -> None:
        result = func(Month(2000, 12))
        expected = "2000-12"
        assert result == expected

    @mark.parametrize(
        ("month", "n", "expected"),
        [
            param(Month(2000, 1), -2, Month(2000, 3)),
            param(Month(2000, 1), -1, Month(2000, 2)),
            param(Month(2000, 1), 0, Month(2000, 1)),
            param(Month(2000, 1), 1, Month(1999, 12)),
            param(Month(2000, 1), 2, Month(1999, 11)),
            param(Month(2000, 1), 12, Month(1999, 1)),
            param(Month(2000, 1), 13, Month(1998, 12)),
        ],
    )
    def test_subtract(self, *, month: Month, n: int, expected: Month) -> None:
        result = month - n
        assert result == expected

    @given(date=dates())
    def test_to_and_from_date(self, *, date: dt.date) -> None:
        month = Month.from_date(date)
        result = month.to_date(day=date.day)
        assert result == date

    def test_error(self) -> None:
        with raises(MonthError, match=r"Invalid year and month: \d+, \d+"):
            _ = Month(2000, 13)


class TestSerializeAndParseMonth:
    @given(month=months())
    def test_main(self, *, month: Month) -> None:
        serialized = serialize_month(month)
        result = parse_month(serialized)
        assert result == month

    def test_error_parse(self) -> None:
        with raises(ParseMonthError, match="Unable to parse month; got 'invalid'"):
            _ = parse_month("invalid")

    @given(data=data(), month=months())
    def test_ensure(self, *, data: DataObject, month: Month) -> None:
        str_or_value = data.draw(sampled_from([month, serialize_month(month)]))
        result = ensure_month(str_or_value)
        assert result == month

    def test_error_ensure(self) -> None:
        with raises(EnsureMonthError, match="Unable to ensure month; got 'invalid'"):
            _ = ensure_month("invalid")


class TestParseTwoDigitYear:
    @given(data=data(), year=integers(0, 99))
    def test_main(self, *, data: DataObject, year: int) -> None:
        input_ = data.draw(sampled_from([year, str(year)]))
        result = parse_two_digit_year(input_)
        expected = (
            dt.datetime.strptime(format(year, "02d"), "%y").replace(tzinfo=UTC).year
        )
        assert result == expected

    @given(year=integers(max_value=-1) | integers(min_value=100))
    def test_error_int(self, *, year: int) -> None:
        with raises(
            _ParseTwoDigitYearInvalidIntegerError, match="Unable to parse year; got .*"
        ):
            _ = parse_two_digit_year(year)

    @given(year=(integers(max_value=-1) | integers(min_value=100)).map(str))
    def test_error_str(self, *, year: str) -> None:
        with raises(
            _ParseTwoDigitYearInvalidStringError, match="Unable to parse year; got .*"
        ):
            _ = parse_two_digit_year(year)


class TestRoundDateTime:
    @given(datetime=datetimes() | zoned_datetimes(time_zone=timezones()))
    def test_minute(self, *, datetime: dt.datetime) -> None:
        floor = round_datetime(datetime, MINUTE, mode="floor")
        ceil = round_datetime(datetime, MINUTE, mode="ceil")
        assert floor.second == floor.microsecond == 0
        assert ceil.second == ceil.microsecond == 0
        assert floor.tzinfo == ceil.tzinfo == datetime.tzinfo
        assert floor <= datetime <= ceil

    @given(datetime=datetimes() | zoned_datetimes(time_zone=timezones()))
    def test_second(self, *, datetime: dt.datetime) -> None:
        floor = round_datetime(datetime, SECOND, mode="floor")
        ceil = round_datetime(datetime, SECOND, mode="ceil")
        assert floor.microsecond == 0
        assert ceil.microsecond == 0
        assert floor.tzinfo == ceil.tzinfo == datetime.tzinfo
        assert floor <= datetime <= ceil


class TestRoundToWeekday:
    @given(date=dates())
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    @mark.parametrize(
        ("func", "predicate", "operator"),
        [
            param(round_to_next_weekday, True, eq),
            param(round_to_next_weekday, False, gt),
            param(round_to_prev_weekday, True, eq),
            param(round_to_prev_weekday, False, lt),
        ],
    )
    def test_main(
        self,
        *,
        date: dt.date,
        func: Callable[[dt.date], dt.date],
        predicate: bool,
        operator: Callable[[dt.date, dt.date], bool],
    ) -> None:
        _ = assume(is_weekday(date) is predicate)
        with assume_does_not_raise(OverflowError):
            result = func(date)
        assert operator(result, date)


class TestSerializeAndParseCompact:
    @given(date=dates())
    def test_dates(self, *, date: dt.date) -> None:
        result = parse_date_compact(serialize_compact(date))
        assert result == date

    @given(datetime=zoned_datetimes(round_="standard", timedelta=SECOND))
    def test_datetimes(self, *, datetime: dt.datetime) -> None:
        result = parse_datetime_compact(serialize_compact(datetime))
        assert result == datetime

    @given(datetime=datetimes())
    def test_error_serialize(self, *, datetime: dt.datetime) -> None:
        with raises(
            SerializeCompactError, match="Unable to serialize local datetime .*"
        ):
            _ = serialize_compact(datetime)

    def test_error_parse_date(self) -> None:
        with raises(ParseDateCompactError, match="Unable to parse '.*' into a date"):
            _ = parse_date_compact("invalid")

    def test_error_parse_datetime(self) -> None:
        with raises(
            ParseDateTimeCompactError, match="Unable to parse '.*' into a datetime"
        ):
            _ = parse_datetime_compact("invalid")


class TestSubDuration:
    @given(date=dates(), n=integers())
    def test_date(self, *, date: dt.date, n: int) -> None:
        with assume_does_not_raise(OverflowError):
            result = sub_duration(date, duration=n)
        expected = date - dt.timedelta(days=n)
        assert result == expected

    @given(datetime=zoned_datetimes(), n=integers())
    def test_datetime(self, *, datetime: dt.datetime, n: int) -> None:
        with assume_does_not_raise(OverflowError):
            result = sub_duration(datetime, duration=n)
        expected = datetime - dt.timedelta(seconds=n)
        assert result == expected

    @given(date=dates() | zoned_datetimes())
    def test_none(self, *, date: DateOrDateTime) -> None:
        result = sub_duration(date)
        assert result == date

    @given(
        date=dates(),
        n=integers(),
        frac=timedeltas(
            min_value=-(DAY - MICROSECOND), max_value=DAY - MICROSECOND
        ).filter(not_func(is_zero_time)),
    )
    def test_error(self, *, date: dt.date, n: int, frac: dt.timedelta) -> None:
        with assume_does_not_raise(OverflowError):
            timedelta = dt.timedelta(days=n) + frac
        with raises(
            SubDurationError,
            match="Date .* must be paired with an integral duration; got .*",
        ):
            _ = sub_duration(date, duration=timedelta)


class TestTimedeltaSinceEpoch:
    @given(
        date_or_datetime=dates() | datetimes() | zoned_datetimes(time_zone=timezones())
    )
    def test_main(self, *, date_or_datetime: DateOrDateTime) -> None:
        result = timedelta_since_epoch(date_or_datetime)
        assert isinstance(result, dt.timedelta)

    @given(datetime=zoned_datetimes(), time_zone1=timezones(), time_zone2=timezones())
    def test_time_zone(
        self, *, datetime: dt.datetime, time_zone1: ZoneInfo, time_zone2: ZoneInfo
    ) -> None:
        with assume_does_not_raise(OverflowError, match="date value out of range"):
            datetime1 = datetime.astimezone(time_zone1)
            datetime2 = datetime.astimezone(time_zone2)
        result1, result2 = [timedelta_since_epoch(dt) for dt in [datetime1, datetime2]]
        assert result1 == result2


class TestTimedeltaToMicrosecondsOrMilliseconds:
    @given(timedelta=timedeltas())
    def test_timedelta_to_microseconds(self, *, timedelta: dt.timedelta) -> None:
        microseconds = timedelta_to_microseconds(timedelta)
        result = microseconds_to_timedelta(microseconds)
        assert result == timedelta

    @given(microseconds=integers())
    def test_microseconds_to_timedelta(self, *, microseconds: int) -> None:
        with assume_does_not_raise(OverflowError):
            timedelta = microseconds_to_timedelta(microseconds)
        result = timedelta_to_microseconds(timedelta)
        assert result == microseconds

    @given(timedelta=timedeltas(), strict=booleans())
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_timedelta_to_milliseconds_exact(
        self, *, timedelta: dt.timedelta, strict: bool
    ) -> None:
        _, remainder = divmod(timedelta.microseconds, _MICROSECONDS_PER_MILLISECOND)
        _ = assume(remainder == 0)
        milliseconds = timedelta_to_milliseconds(timedelta, strict=strict)
        assert isinstance(milliseconds, int)
        result = milliseconds_to_timedelta(milliseconds)
        assert result == timedelta

    @given(timedelta=timedeltas())
    def test_timedelta_to_milliseconds_inexact(
        self, *, timedelta: dt.timedelta
    ) -> None:
        _, remainder = divmod(timedelta.microseconds, _MICROSECONDS_PER_MILLISECOND)
        _ = assume(remainder != 0)
        milliseconds = timedelta_to_milliseconds(timedelta)
        result = milliseconds_to_timedelta(round(milliseconds))
        assert abs(result - timedelta) <= SECOND

    @given(timedelta=timedeltas())
    def test_timedelta_to_milliseconds_error(self, *, timedelta: dt.timedelta) -> None:
        _, microseconds = divmod(timedelta.microseconds, _MICROSECONDS_PER_MILLISECOND)
        _ = assume(microseconds != 0)
        with raises(
            TimedeltaToMillisecondsError,
            match=r"Unable to convert .* to milliseconds; got .* microsecond\(s\)",
        ):
            _ = timedelta_to_milliseconds(timedelta, strict=True)

    @given(milliseconds=int32s())
    def test_milliseconds_to_timedelta(self, *, milliseconds: int) -> None:
        with assume_does_not_raise(OverflowError):
            timedelta = milliseconds_to_timedelta(milliseconds)
        result = timedelta_to_milliseconds(timedelta)
        assert result == milliseconds


class TestTimedeltas:
    @mark.parametrize(
        "timedelta",
        [
            param(MICROSECOND),
            param(MILLISECOND),
            param(SECOND),
            param(MINUTE),
            param(HOUR),
            param(DAY),
            param(WEEK),
        ],
    )
    def test_main(self, *, timedelta: dt.timedelta) -> None:
        assert isinstance(timedelta, dt.timedelta)


class TestTimeZones:
    def test_main(self) -> None:
        assert isinstance(UTC, dt.tzinfo)


class TestYieldDays:
    @given(start=dates(), days=integers(0, 365))
    def test_start_and_end(self, *, start: dt.date, days: int) -> None:
        with assume_does_not_raise(OverflowError, match="date value out of range"):
            end = start + dt.timedelta(days=days)
            dates = list(yield_days(start=start, end=end))
        assert all(start <= d <= end for d in dates)

    @given(start=dates(), days=integers(0, 10))
    def test_start_and_days(self, *, start: dt.date, days: int) -> None:
        with assume_does_not_raise(OverflowError, match="date value out of range"):
            dates = list(yield_days(start=start, days=days))
        assert len(dates) == days
        assert all(d >= start for d in dates)

    @given(end=dates(), days=integers(0, 10))
    def test_end_and_days(self, *, end: dt.date, days: int) -> None:
        with assume_does_not_raise(OverflowError, match="date value out of range"):
            dates = list(yield_days(end=end, days=days))
        assert len(dates) == days
        assert all(d <= end for d in dates)

    def test_error(self) -> None:
        with raises(
            YieldDaysError, match="Invalid arguments: start=None, end=None, days=None"
        ):
            _ = list(yield_days())


class TestYieldWeekdays:
    @given(start=dates(), days=integers(0, 365))
    def test_start_and_end(self, *, start: dt.date, days: int) -> None:
        with assume_does_not_raise(OverflowError, match="date value out of range"):
            end = start + dt.timedelta(days=days)
            dates = list(yield_weekdays(start=start, end=end))
        assert all(start <= d <= end for d in dates)
        assert all(map(is_weekday, dates))
        if is_weekday(start):
            assert start in dates
        if is_weekday(end):
            assert end in dates

    @given(start=dates(), days=integers(0, 10))
    def test_start_and_days(self, *, start: dt.date, days: int) -> None:
        with assume_does_not_raise(OverflowError, match="date value out of range"):
            dates = list(yield_weekdays(start=start, days=days))
        assert len(dates) == days
        assert all(d >= start for d in dates)
        assert all(map(is_weekday, dates))

    @given(end=dates(), days=integers(0, 10))
    def test_end_and_days(self, *, end: dt.date, days: int) -> None:
        with assume_does_not_raise(OverflowError, match="date value out of range"):
            dates = list(yield_weekdays(end=end, days=days))
        assert len(dates) == days
        assert all(d <= end for d in dates)
        assert all(map(is_weekday, dates))

    def test_error(self) -> None:
        with raises(
            YieldWeekdaysError,
            match="Invalid arguments: start=None, end=None, days=None",
        ):
            _ = list(yield_weekdays())
