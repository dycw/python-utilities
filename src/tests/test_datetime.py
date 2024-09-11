from __future__ import annotations

import datetime as dt
from math import isclose
from operator import eq, gt, lt
from re import search
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

from hypothesis import HealthCheck, assume, given, settings
from hypothesis.strategies import (
    DataObject,
    data,
    dates,
    datetimes,
    floats,
    integers,
    just,
    none,
    sampled_from,
    timedeltas,
    timezones,
)
from pytest import mark, param, raises

from utilities.datetime import (
    _MICROSECONDS_PER_MILLISECOND,
    DAY,
    EPOCH_NAIVE,
    EPOCH_UTC,
    HALF_YEAR,
    HOUR,
    MICROSECOND,
    MILLISECOND,
    MINUTE,
    MONTH,
    NOW_HK,
    NOW_TOKYO,
    NOW_UTC,
    QUARTER,
    SECOND,
    TODAY_HK,
    TODAY_TOKYO,
    TODAY_UTC,
    WEEK,
    YEAR,
    AddWeekdaysError,
    CheckDateNotDatetimeError,
    CheckZonedDatetimeError,
    EnsureMonthError,
    MillisecondsSinceEpochError,
    Month,
    MonthError,
    ParseMonthError,
    Period,
    PeriodError,
    TimedeltaToMillisecondsError,
    YieldDaysError,
    YieldWeekdaysError,
    add_weekdays,
    check_date_not_datetime,
    check_zoned_datetime,
    date_to_datetime,
    date_to_month,
    drop_microseconds,
    drop_milli_and_microseconds,
    duration_to_float,
    duration_to_timedelta,
    ensure_month,
    format_datetime_local_and_utc,
    get_half_years,
    get_months,
    get_now,
    get_now_hk,
    get_now_tokyo,
    get_quarters,
    get_today,
    get_today_hk,
    get_today_tokyo,
    get_years,
    is_equal_mod_tz,
    is_instance_date_not_datetime,
    is_local_datetime,
    is_subclass_date_not_datetime,
    is_weekday,
    is_zoned_datetime,
    maybe_sub_pct_y,
    microseconds_since_epoch,
    microseconds_since_epoch_to_datetime,
    microseconds_to_timedelta,
    milliseconds_since_epoch,
    milliseconds_since_epoch_to_datetime,
    milliseconds_to_timedelta,
    parse_month,
    round_to_next_weekday,
    round_to_prev_weekday,
    serialize_month,
    timedelta_since_epoch,
    timedelta_to_microseconds,
    timedelta_to_milliseconds,
    yield_days,
    yield_weekdays,
)
from utilities.hypothesis import assume_does_not_raise, int32s, months, text_clean
from utilities.zoneinfo import UTC, HongKong, Tokyo, USCentral, USEastern

if TYPE_CHECKING:
    from collections.abc import Callable

    from utilities.types import Number


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

    @given(date=dates(), n1=integers(-10, 10), n2=integers(-10, 10))
    def test_two(self, *, date: dt.date, n1: int, n2: int) -> None:
        with assume_does_not_raise(AddWeekdaysError, OverflowError):
            weekday1, weekday2 = (add_weekdays(date, n=n) for n in [n1, n2])
        result = weekday1 <= weekday2
        expected = n1 <= n2
        assert result is expected


class TestCheckDateNotDatetime:
    @given(date=dates())
    def test_main(self, *, date: dt.date) -> None:
        check_date_not_datetime(date)

    @given(datetime=datetimes())
    def test_error(self, *, datetime: dt.datetime) -> None:
        with raises(
            CheckDateNotDatetimeError, match="Date must not be a datetime; got .*"
        ):
            check_date_not_datetime(datetime)


class TestCheckZonedDatetime:
    @given(datetime=datetimes(timezones=sampled_from([HongKong, UTC, dt.UTC])))
    def test_date(self, *, datetime: dt.datetime) -> None:
        check_zoned_datetime(datetime)

    @given(datetime=datetimes())
    def test_datetime(self, *, datetime: dt.datetime) -> None:
        with raises(CheckZonedDatetimeError, match="Datetime must be zoned; got .*"):
            check_zoned_datetime(datetime)


class TestDateToDatetime:
    @given(date=dates())
    def test_main(self, *, date: dt.date) -> None:
        result = date_to_datetime(date).date()
        assert result == date


class TestDateToMonth:
    @given(date=dates())
    def test_main(self, *, date: dt.date) -> None:
        result = date_to_month(date).to_date(day=date.day)
        assert result == date


class TestDropMicroseconds:
    @given(datetime=datetimes())
    def test_main(self, *, datetime: dt.datetime) -> None:
        result = drop_microseconds(datetime)
        _, remainder = divmod(result.microsecond, _MICROSECONDS_PER_MILLISECOND)
        assert remainder == 0


class TestDropMilliAndMicroseconds:
    @given(datetime=datetimes())
    def test_main(self, *, datetime: dt.datetime) -> None:
        result = drop_milli_and_microseconds(datetime)
        assert result.microsecond == 0


class TestDurationToFloat:
    @given(duration=integers(0, 10) | floats(0.0, 10.0))
    def test_number(self, *, duration: Number) -> None:
        result = duration_to_float(duration)
        assert result == duration

    @given(duration=timedeltas())
    def test_timedelta(self, *, duration: dt.timedelta) -> None:
        result = duration_to_float(duration)
        assert result == duration.total_seconds()


class TestDurationToTimedelta:
    @given(duration=integers(0, 10))
    def test_int(self, *, duration: int) -> None:
        result = duration_to_timedelta(duration)
        assert result.total_seconds() == duration

    @given(duration=floats(0.0, 10.0))
    def test_float(self, *, duration: float) -> None:
        duration = round(10 * duration) / 10
        result = duration_to_timedelta(duration)
        assert isclose(result.total_seconds(), duration)

    @given(duration=timedeltas())
    def test_timedelta(self, *, duration: dt.timedelta) -> None:
        result = duration_to_timedelta(duration)
        assert result == duration


class TestEpoch:
    @mark.parametrize(
        ("epoch", "time_zone"), [param(EPOCH_NAIVE, None), param(EPOCH_UTC, UTC)]
    )
    def test_main(self, *, epoch: dt.datetime, time_zone: ZoneInfo | None) -> None:
        assert isinstance(EPOCH_UTC, dt.datetime)
        assert epoch.tzinfo is time_zone


class TestFormatDatetimeLocalAndUTC:
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


class TestGetNow:
    @given(time_zone=timezones())
    def test_main(self, *, time_zone: ZoneInfo) -> None:
        now = get_now(time_zone=time_zone)
        assert isinstance(now, dt.datetime)
        assert now.tzinfo is time_zone

    def test_local(self) -> None:
        now = get_now(time_zone="local")
        assert isinstance(now, dt.datetime)
        ETC = ZoneInfo("Etc/UTC")  # noqa: N806
        assert now.tzinfo in {ETC, HongKong, Tokyo, UTC}

    @mark.parametrize(
        "get_now", [param(get_now), param(get_now_hk), param(get_now_tokyo)]
    )
    def test_getters(self, *, get_now: Callable[[], dt.datetime]) -> None:
        assert isinstance(get_now(), dt.date)

    @mark.parametrize("now", [param(NOW_UTC), param(NOW_HK), param(NOW_TOKYO)])
    def test_constants(self, *, now: dt.datetime) -> None:
        assert isinstance(now, dt.date)


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

    @mark.parametrize(
        "timedelta", [param(MONTH), param(QUARTER), param(HALF_YEAR), param(YEAR)]
    )
    def test_constants(self, *, timedelta: dt.timedelta) -> None:
        assert isinstance(timedelta, dt.timedelta)


class TestGetToday:
    @given(time_zone=timezones())
    def test_main(self, *, time_zone: ZoneInfo) -> None:
        today = get_today(time_zone=time_zone)
        assert isinstance(today, dt.date)

    @mark.parametrize(
        "get_today", [param(get_today), param(get_today_hk), param(get_today_tokyo)]
    )
    def test_getters(self, *, get_today: Callable[[], dt.datetime]) -> None:
        assert isinstance(get_today(), dt.date)

    @mark.parametrize("today", [param(TODAY_UTC), param(TODAY_HK), param(TODAY_TOKYO)])
    def test_constants(self, *, today: dt.date) -> None:
        assert isinstance(today, dt.date)


class TestIsInstanceDateNotDatetime:
    @given(date=dates())
    def test_date(self, *, date: dt.date) -> None:
        assert is_instance_date_not_datetime(date)

    @given(datetime=datetimes())
    def test_datetime(self, *, datetime: dt.datetime) -> None:
        assert not is_instance_date_not_datetime(datetime)


class TestIsEqualModTz:
    @given(x=datetimes(), y=datetimes())
    def test_naive(self, *, x: dt.datetime, y: dt.datetime) -> None:
        assert is_equal_mod_tz(x, y) == (x == y)

    @given(x=datetimes(timezones=just(UTC)), y=datetimes(timezones=just(UTC)))
    def test_utc(self, *, x: dt.datetime, y: dt.datetime) -> None:
        assert is_equal_mod_tz(x, y) == (x == y)

    @given(x=datetimes(), y=datetimes())
    def test_naive_vs_utc(self, *, x: dt.datetime, y: dt.datetime) -> None:
        expected = x == y
        naive = x
        aware = y.replace(tzinfo=UTC)
        assert is_equal_mod_tz(naive, aware) == expected
        assert is_equal_mod_tz(aware, naive) == expected


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


class TestIsSubClassDateNotDatetime:
    @given(date=dates())
    def test_date(self, *, date: dt.date) -> None:
        assert is_subclass_date_not_datetime(type(date))

    @given(datetime=datetimes())
    def test_datetime(self, *, datetime: dt.datetime) -> None:
        assert not is_subclass_date_not_datetime(type(datetime))


class TestIsWeekday:
    @given(date=dates())
    def test_main(self, *, date: dt.date) -> None:
        result = is_weekday(date)
        name = date.strftime("%A")
        expected = name in {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday"}
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


class TestMicrosecondsOrMillisecondsSinceEpoch:
    @given(datetime=datetimes(timezones=just(UTC)))
    def test_datetime_to_microseconds(self, *, datetime: dt.datetime) -> None:
        microseconds = microseconds_since_epoch(datetime)
        result = microseconds_since_epoch_to_datetime(microseconds)
        assert result == datetime

    @given(microseconds=integers())
    def test_microseconds_to_datetime(self, *, microseconds: int) -> None:
        with assume_does_not_raise(OverflowError):
            datetime = microseconds_since_epoch_to_datetime(microseconds)
        result = microseconds_since_epoch(datetime)
        assert result == microseconds

    @given(datetime=datetimes(timezones=just(UTC)))
    @mark.parametrize("strict", [param(True), param(False)])
    def test_datetime_to_milliseconds_exact(
        self, *, datetime: dt.datetime, strict: bool
    ) -> None:
        _ = assume(datetime.microsecond == 0)
        milliseconds = milliseconds_since_epoch(datetime, strict=strict)
        if strict:
            assert isinstance(milliseconds, int)
        else:
            assert milliseconds == round(milliseconds)
        result = milliseconds_since_epoch_to_datetime(round(milliseconds))
        assert result == datetime

    @given(datetime=datetimes(timezones=just(UTC)))
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


class TestParseAndSerializeMonth:
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


class TestPeriod:
    @given(start=dates(), end=dates())
    def test_dates(self, *, start: dt.date, end: dt.date) -> None:
        _ = assume(start <= end)
        period = Period(start, end)
        assert period.start == start
        assert period.end == end

    @given(
        start=datetimes(timezones=sampled_from([HongKong, UTC, dt.UTC])),
        end=datetimes(timezones=sampled_from([HongKong, UTC, dt.UTC])),
    )
    def test_datetimes(self, *, start: dt.datetime, end: dt.datetime) -> None:
        _ = assume(start <= end)
        period = Period(start, end)
        assert period.start == start
        assert period.end == end

    @given(start=dates(), days=integers(min_value=0))
    def test_duration(self, *, start: dt.date, days: int) -> None:
        with assume_does_not_raise(OverflowError):
            duration = dt.timedelta(days=days)
        with assume_does_not_raise(OverflowError):
            end = start + duration
        period = Period(start, end)
        assert period.duration == duration

    @given(
        start=dates(),
        end=datetimes(timezones=sampled_from([HongKong, UTC, dt.UTC]) | none()),
    )
    def test_error_date_and_datetime_mix(
        self, *, start: dt.date, end: dt.datetime
    ) -> None:
        with raises(
            PeriodError, match=r"Invalid period; got date and datetime mix \(.*, .*\)"
        ):
            _ = Period(start, end)

    @given(
        start=datetimes(timezones=sampled_from([HongKong, UTC, dt.UTC]) | none()),
        end=datetimes(timezones=sampled_from([HongKong, UTC, dt.UTC]) | none()),
    )
    def test_error_naive_datetime(
        self, *, start: dt.datetime, end: dt.datetime
    ) -> None:
        _ = assume((start.tzinfo is None) or (end.tzinfo is None))
        with raises(
            PeriodError, match=r"Invalid period; got naive datetime\(s\) \(.*, .*\)"
        ):
            _ = Period(start, end)

    @given(start=dates(), end=dates())
    def test_error_invalid_dates(self, *, start: dt.date, end: dt.date) -> None:
        _ = assume(start > end)
        with raises(PeriodError, match="Invalid period; got .* > .*"):
            _ = Period(start, end)

    @given(
        start=datetimes(timezones=sampled_from([HongKong, UTC, dt.UTC])),
        end=datetimes(timezones=sampled_from([HongKong, UTC, dt.UTC])),
    )
    def test_error_invalid_datetimes(
        self, *, start: dt.datetime, end: dt.datetime
    ) -> None:
        _ = assume(start > end)
        with raises(PeriodError, match="Invalid period; got .* > .*"):
            _ = Period(start, end)


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


class TestTimedeltaSinceEpoch:
    @given(datetime=datetimes(timezones=sampled_from([HongKong, UTC, dt.UTC])))
    def test_main(self, *, datetime: dt.datetime) -> None:
        result = timedelta_since_epoch(datetime)
        assert isinstance(result, dt.timedelta)

    @given(
        datetime=datetimes(timezones=just(UTC)),
        time_zone1=sampled_from([HongKong, Tokyo, USCentral, USEastern, UTC]),
        time_zone2=sampled_from([HongKong, Tokyo, USCentral, USEastern, UTC]),
    )
    def test_time_zone(
        self, *, datetime: dt.datetime, time_zone1: ZoneInfo, time_zone2: ZoneInfo
    ) -> None:
        result1 = timedelta_since_epoch(datetime.astimezone(time_zone1))
        result2 = timedelta_since_epoch(datetime.astimezone(time_zone2))
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

    @given(timedelta=timedeltas())
    @mark.parametrize("strict", [param(True), param(False)])
    def test_timedelta_to_milliseconds_exact(
        self, *, timedelta: dt.timedelta, strict: bool
    ) -> None:
        _ = assume(timedelta.microseconds == 0)
        milliseconds = timedelta_to_milliseconds(timedelta, strict=strict)
        if strict:
            assert isinstance(milliseconds, int)
        else:
            assert milliseconds == round(milliseconds)
        result = milliseconds_to_timedelta(round(milliseconds))
        assert result == timedelta

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
        with assume_does_not_raise(OverflowError):
            end = start + dt.timedelta(days=days)
            dates = list(yield_days(start=start, end=end))
        assert all(start <= d <= end for d in dates)

    @given(start=dates(), days=integers(0, 10))
    def test_start_and_days(self, *, start: dt.date, days: int) -> None:
        dates = list(yield_days(start=start, days=days))
        assert len(dates) == days
        assert all(d >= start for d in dates)

    @given(end=dates(), days=integers(0, 10))
    def test_end_and_days(self, *, end: dt.date, days: int) -> None:
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
