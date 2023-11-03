from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from datetime import tzinfo
from math import isclose
from operator import eq, gt, lt
from re import search
from typing import Any

from hypothesis import HealthCheck, assume, given, settings
from hypothesis.strategies import (
    DataObject,
    SearchStrategy,
    data,
    dates,
    datetimes,
    floats,
    integers,
    just,
    sampled_from,
    timedeltas,
    times,
    timezones,
)
from pytest import mark, param, raises

from utilities.datetime import (
    EPOCH_UTC,
    NOW_HKG,
    NOW_TKY,
    NOW_UTC,
    TODAY_HKG,
    TODAY_TKY,
    TODAY_UTC,
    UTC,
    IsWeekendError,
    ParseDateError,
    ParseDateTimeError,
    ParseTimedeltaError,
    ParseTimeError,
    YieldWeekdaysError,
    add_weekdays,
    date_to_datetime,
    duration_to_float,
    duration_to_timedelta,
    ensure_date,
    ensure_datetime,
    ensure_time,
    ensure_timedelta,
    get_now,
    get_today,
    is_weekday,
    local_timezone,
    maybe_sub_pct_y,
    parse_date,
    parse_datetime,
    parse_time,
    parse_timedelta,
    round_to_next_weekday,
    round_to_prev_weekday,
    serialize_date,
    serialize_datetime,
    serialize_time,
    serialize_timedelta,
    yield_weekdays,
)
from utilities.hypothesis import assume_does_not_raise, text_clean
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
        with raises(IsWeekendError):
            _ = add_weekdays(date, n=0)

    @given(date=dates(), n1=integers(-10, 10), n2=integers(-10, 10))
    def test_two(self, *, date: dt.date, n1: int, n2: int) -> None:
        with assume_does_not_raise(IsWeekendError, OverflowError):
            weekday1, weekday2 = (add_weekdays(date, n=n) for n in [n1, n2])
        result = weekday1 <= weekday2
        expected = n1 <= n2
        assert result is expected


class TestDateToDatetime:
    @given(date=dates())
    def test_main(self, *, date: dt.date) -> None:
        result = date_to_datetime(date).date()
        assert result == date


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


class TestEnsure:
    @given(data=data())
    @mark.parametrize(
        ("strategy", "func"),
        [
            param(dates(), ensure_date),
            param(datetimes(timezones=just(UTC)), ensure_datetime),
            param(times(), ensure_time),
            param(timedeltas(), ensure_timedelta),
        ],
    )
    def test_main(
        self,
        *,
        data: DataObject,
        strategy: SearchStrategy[Any],
        func: Callable[[Any], Any],
    ) -> None:
        value = data.draw(strategy)
        maybe_value = data.draw(sampled_from([value, str(value)]))
        result = func(maybe_value)
        assert result == value


class TestIsWeekday:
    @given(date=dates())
    def test_is_weekday(self, *, date: dt.date) -> None:
        result = is_weekday(date)
        name = date.strftime("%A")
        expected = name in {
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
        }
        assert result is expected


class TestLocalTimeZone:
    def test_main(self) -> None:
        tz = local_timezone()
        now = get_now(tz=UTC)
        result = tz.tzname(now)
        expected = {"Coordinated Universal Time", "HKT", "JST", "UTC"}
        assert result in expected


class TestMaybeSubPctY:
    @given(text=text_clean())
    def test_main(self, *, text: str) -> None:
        result = maybe_sub_pct_y(text)
        assert not search("%Y", result)


class TestGetNow:
    @given(tz=timezones())
    def test_function(self, *, tz: tzinfo) -> None:
        now = get_now(tz=tz)
        assert isinstance(now, dt.datetime)
        assert now.tzinfo is tz

    @mark.parametrize("now", [param(NOW_UTC), param(NOW_HKG), param(NOW_TKY)])
    def test_constants(self, *, now: dt.datetime) -> None:
        assert isinstance(now, dt.date)


class TestGetToday:
    @given(tz=timezones())
    def test_function(self, *, tz: tzinfo) -> None:
        today = get_today(tz=tz)
        assert isinstance(today, dt.date)

    @mark.parametrize("today", [param(TODAY_UTC), param(TODAY_HKG), param(TODAY_TKY)])
    def test_main(self, *, today: dt.date) -> None:
        assert isinstance(today, dt.date)


class TestParseDate:
    @given(date=dates())
    def test_str(self, *, date: dt.date) -> None:
        result = parse_date(str(date))
        assert result == date

    @given(date=dates())
    def test_isoformat(self, *, date: dt.date) -> None:
        result = parse_date(date.isoformat())
        assert result == date

    @given(
        date=dates(),
        fmt=sampled_from(
            [
                "%Y%m%d",
                "%Y %m %d",
                "%d%b%Y",
                "%d %b %Y",
            ]
        ).map(maybe_sub_pct_y),
    )
    def test_various_formats(self, *, date: dt.date, fmt: str) -> None:
        result = parse_date(date.strftime(fmt))
        assert result == date

    def test_error(self) -> None:
        with raises(ParseDateError):
            _ = parse_date("error")


class TestParseDateTime:
    @given(datetime=datetimes(timezones=just(UTC)))
    def test_str(self, *, datetime: dt.datetime) -> None:
        result = parse_datetime(str(datetime))
        assert result == datetime

    @given(datetime=datetimes(timezones=just(UTC)))
    def test_isoformat(self, *, datetime: dt.datetime) -> None:
        result = parse_datetime(datetime.isoformat())
        assert result == datetime

    @given(
        datetime=datetimes(timezones=just(UTC)),
        fmt=sampled_from(["%Y%m%dT%H%M%S.%f%z", "%Y-%m-%d %H:%M:%S.%f%z"]).map(
            maybe_sub_pct_y
        ),
    )
    def test_yyyymmdd_hhmmss_fff_zzzz(self, *, datetime: dt.datetime, fmt: str) -> None:
        result = parse_datetime(datetime.strftime(fmt))
        assert result == datetime

    @given(
        datetime=datetimes(timezones=just(UTC)),
        fmt=sampled_from(["%Y%m%dT%H%M%S.%f", "%Y-%m-%d %H:%M:%S.%f"]).map(
            maybe_sub_pct_y
        ),
    )
    def test_yyyymmdd_hhmmss_fff(self, *, datetime: dt.datetime, fmt: str) -> None:
        result = parse_datetime(datetime.strftime(fmt))
        assert result == datetime

    @given(
        datetime=datetimes(timezones=just(UTC)),
        fmt=sampled_from(
            ["%Y%m%dT%H%M%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"]
        ).map(maybe_sub_pct_y),
    )
    def test_yyyymmdd_hhmmss(self, *, datetime: dt.datetime, fmt: str) -> None:
        datetime = datetime.replace(microsecond=0)
        result = parse_datetime(datetime.strftime(fmt))
        assert result == datetime

    @given(
        datetime=datetimes(timezones=just(UTC)),
        fmt=sampled_from(["%Y%m%dT%H%M", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M"]).map(
            maybe_sub_pct_y
        ),
    )
    def test_yyyymmdd_hhmm(self, *, datetime: dt.datetime, fmt: str) -> None:
        datetime = datetime.replace(second=0, microsecond=0)
        result = parse_datetime(datetime.strftime(fmt))
        assert result == datetime

    @given(
        datetime=datetimes(timezones=just(UTC)),
        fmt=sampled_from(["%Y%m%dT%H", "%Y-%m-%d %H", "%Y-%m-%dT%H"]).map(
            maybe_sub_pct_y
        ),
    )
    def test_yyyymmdd_hh(self, *, datetime: dt.datetime, fmt: str) -> None:
        datetime = datetime.replace(minute=0, second=0, microsecond=0)
        result = parse_datetime(datetime.strftime(fmt))
        assert result == datetime

    @given(
        datetime=datetimes(timezones=just(UTC)),
        fmt=sampled_from(["%Y%m%d", "%Y-%m-%d"]).map(maybe_sub_pct_y),
    )
    def test_yyyymmdd(self, *, datetime: dt.datetime, fmt: str) -> None:
        datetime = datetime.replace(hour=0, minute=0, second=0, microsecond=0)
        result = parse_datetime(datetime.strftime(fmt))
        assert result == datetime

    def test_error(self) -> None:
        with raises(ParseDateTimeError):
            _ = parse_datetime("error")


class TestParseTime:
    @given(time=times())
    def test_str(self, *, time: dt.time) -> None:
        result = parse_time(str(time))
        assert result == time

    @given(time=times())
    def test_isoformat(self, *, time: dt.time) -> None:
        result = parse_time(time.isoformat())
        assert result == time

    @given(time=times(), fmt=sampled_from(["%H%M%S.%f", "%H:%M:%S.%f"]))
    def test_hhmmss_fff(self, *, time: dt.time, fmt: str) -> None:
        result = parse_time(time.strftime(fmt))
        assert result == time

    @given(time=times(), fmt=sampled_from(["%H%M%S", "%H:%M:%S"]))
    def test_hhmmss(self, *, time: dt.time, fmt: str) -> None:
        time = time.replace(microsecond=0)
        result = parse_time(time.strftime(fmt))
        assert result == time

    @given(time=times(), fmt=sampled_from(["%H%M", "%H:%M"]))
    def test_hhmm(self, *, time: dt.time, fmt: str) -> None:
        time = time.replace(second=0, microsecond=0)
        result = parse_time(time.strftime(fmt))
        assert result == time

    @given(time=times(), fmt=sampled_from(["%H", "%H"]))
    def test_hh(self, *, time: dt.time, fmt: str) -> None:
        time = time.replace(minute=0, second=0, microsecond=0)
        result = parse_time(time.strftime(fmt))
        assert result == time

    def test_error(self) -> None:
        with raises(ParseTimeError):
            _ = parse_time("error")


class TestParseTimedelta:
    @given(timedelta=timedeltas())
    def test_main(self, *, timedelta: dt.timedelta) -> None:
        result = parse_timedelta(str(timedelta))
        assert result == timedelta

    def test_error(self) -> None:
        with raises(ParseTimedeltaError):
            _ = parse_timedelta("error")


class TestSerialize:
    @given(data=data())
    @mark.parametrize(
        ("strategy", "serialize", "parse"),
        [
            param(dates(), serialize_date, parse_date),
            param(
                datetimes(timezones=just(UTC)),
                serialize_datetime,
                parse_datetime,
            ),
            param(times(), serialize_time, parse_time),
            param(timedeltas(), str, parse_timedelta),
            param(timedeltas(), serialize_timedelta, parse_timedelta),
        ],
    )
    def test_main(
        self,
        *,
        data: DataObject,
        strategy: SearchStrategy[Any],
        serialize: Callable[[Any], Any],
        parse: Callable[[Any], Any],
    ) -> None:
        value = data.draw(strategy)
        result = parse(serialize(value))
        assert result == value

    @given(datetime=datetimes())
    def test_serialize_date(self, *, datetime: dt.datetime) -> None:
        result = parse_date(serialize_date(datetime))
        assert result == datetime.date()


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


class TestTimes:
    def test_main(self) -> None:
        assert isinstance(EPOCH_UTC, dt.datetime)
        assert EPOCH_UTC.tzinfo is UTC


class TestTimeZones:
    def test_main(self) -> None:
        assert isinstance(UTC, dt.tzinfo)


class TestYieldWeekdays:
    @given(start=dates(), days=integers(0, 365))
    def test_start_and_end(self, *, start: dt.date, days: int) -> None:
        with assume_does_not_raise(OverflowError):
            end = start + dt.timedelta(days=days)
        dates = list(yield_weekdays(start=start, end=end))
        assert all(start <= d <= end for d in dates)
        assert all(map(is_weekday, dates))

    @given(start=dates(), days=integers(0, 10))
    def test_start_and_days(self, *, start: dt.date, days: int) -> None:
        dates = list(yield_weekdays(start=start, days=days))
        assert len(dates) == days
        assert all(d >= start for d in dates)
        assert all(map(is_weekday, dates))

    @given(end=dates(), days=integers(0, 10))
    def test_end_and_days(self, *, end: dt.date, days: int) -> None:
        dates = list(yield_weekdays(end=end, days=days))
        assert len(dates) == days
        assert all(d <= end for d in dates)
        assert all(map(is_weekday, dates))

    def test_error(self) -> None:
        with raises(YieldWeekdaysError):
            _ = list(yield_weekdays())
