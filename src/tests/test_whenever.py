from __future__ import annotations

import datetime as dt
from re import escape
from typing import ClassVar

from hypothesis import given
from hypothesis.strategies import (
    DataObject,
    data,
    dates,
    datetimes,
    integers,
    sampled_from,
    timedeltas,
    times,
)
from pytest import mark, param, raises
from whenever import DateTimeDelta

from utilities.datetime import get_years
from utilities.hypothesis import assume_does_not_raise
from utilities.whenever import (
    ParseDateError,
    ParseLocalDateTimeError,
    ParseTimedeltaError,
    ParseTimeError,
    ParseZonedDateTimeError,
    SerializeLocalDateTimeError,
    SerializeTimeDeltaError,
    SerializeZonedDateTimeError,
    _to_datetime_delta,
    _ToDateTimeDeltaError,
    ensure_date,
    ensure_local_datetime,
    ensure_time,
    ensure_timedelta,
    ensure_zoned_datetime,
    parse_date,
    parse_local_datetime,
    parse_time,
    parse_timedelta,
    parse_zoned_datetime,
    serialize_date,
    serialize_local_datetime,
    serialize_time,
    serialize_timedelta,
    serialize_zoned_datetime,
)
from utilities.zoneinfo import HONG_KONG, UTC

_TIMEDELTA_MICROSECONDS = dt.timedelta(microseconds=1e18)
_TIMEDELTA_OVERFLOW = dt.timedelta(days=106751991, seconds=14454, microseconds=775808)


class TestParseAndSerializeDate:
    @given(date=dates())
    def test_main(self, *, date: dt.date) -> None:
        serialized = serialize_date(date)
        result = parse_date(serialized)
        assert result == date

    def test_error_parse(self) -> None:
        with raises(ParseDateError, match="Unable to parse date; got 'invalid'"):
            _ = parse_date("invalid")

    @given(data=data(), date=dates())
    def test_ensure(self, *, data: DataObject, date: dt.date) -> None:
        str_or_value = data.draw(sampled_from([date, serialize_date(date)]))
        result = ensure_date(str_or_value)
        assert result == date


class TestParseAndSerializeLocalDateTime:
    @given(datetime=datetimes())
    def test_main(self, *, datetime: dt.datetime) -> None:
        serialized = serialize_local_datetime(datetime)
        result = parse_local_datetime(serialized)
        assert result == datetime

    def test_error_parse(self) -> None:
        with raises(
            ParseLocalDateTimeError,
            match="Unable to parse local datetime; got 'invalid'",
        ):
            _ = parse_local_datetime("invalid")

    def test_error_serialize(self) -> None:
        datetime = dt.datetime(2000, 1, 1, tzinfo=UTC)
        with raises(
            SerializeLocalDateTimeError,
            match=escape(
                "Unable to serialize local datetime; got 2000-01-01 00:00:00+00:00"
            ),
        ):
            _ = serialize_local_datetime(datetime)

    @given(data=data(), datetime=datetimes())
    def test_ensure(self, *, data: DataObject, datetime: dt.datetime) -> None:
        str_or_value = data.draw(
            sampled_from([datetime, serialize_local_datetime(datetime)])
        )
        result = ensure_local_datetime(str_or_value)
        assert result == datetime


class TestParseAndSerializeTime:
    @given(time=times())
    def test_main(self, *, time: dt.time) -> None:
        serialized = serialize_time(time)
        result = parse_time(serialized)
        assert result == time

    def test_error_parse(self) -> None:
        with raises(ParseTimeError, match="Unable to parse time; got 'invalid'"):
            _ = parse_time("invalid")

    @given(data=data(), time=times())
    def test_ensure(self, *, data: DataObject, time: dt.time) -> None:
        str_or_value = data.draw(sampled_from([time, serialize_time(time)]))
        result = ensure_time(str_or_value)
        assert result == time


class TestParseAndSerializeTimedelta:
    @given(timedelta=timedeltas())
    def test_main(self, *, timedelta: dt.timedelta) -> None:
        serialized = serialize_timedelta(timedelta)
        result = parse_timedelta(serialized)
        assert result == timedelta

    @mark.only
    def test_example(self) -> None:
        timedelta = dt.timedelta(days=104250, microseconds=1)
        serialized = serialize_timedelta(timedelta)
        result = parse_timedelta(serialized)
        breakpoint()

        assert result == timedelta

    def test_error_parse(self) -> None:
        with raises(
            ParseTimedeltaError, match="Unable to parse timedelta; got 'invalid'"
        ):
            _ = parse_timedelta("invalid")

    def test_error_parse_nano_seconds(self) -> None:
        with raises(
            ParseTimedeltaError, match="Unable to parse timedelta; got 333 nanoseconds"
        ):
            _ = parse_timedelta("PT0.111222333S")

    @mark.parametrize(
        "timedelta", [param(_TIMEDELTA_MICROSECONDS), param(_TIMEDELTA_OVERFLOW)]
    )
    def test_error_serialize(self, *, timedelta: dt.timedelta) -> None:
        with raises(
            SerializeTimeDeltaError, match="Unable to serialize timedelta; got .*"
        ):
            _ = serialize_timedelta(timedelta)

    @given(data=data(), timedelta=timedeltas())
    def test_ensure(self, *, data: DataObject, timedelta: dt.timedelta) -> None:
        with assume_does_not_raise(SerializeTimeDeltaError):
            str_value = serialize_timedelta(timedelta)
        str_or_value = data.draw(sampled_from([timedelta, str_value]))
        result = ensure_timedelta(str_or_value)
        assert result == timedelta


class TestParseAndSerializeZonedDateTime:
    @given(datetime=datetimes(timezones=sampled_from([HONG_KONG, UTC, dt.UTC])))
    def test_main(self, *, datetime: dt.datetime) -> None:
        serialized = serialize_zoned_datetime(datetime)
        result = parse_zoned_datetime(serialized)
        assert result == datetime

    def test_error_parse(self) -> None:
        with raises(
            ParseZonedDateTimeError,
            match="Unable to parse zoned datetime; got 'invalid'",
        ):
            _ = parse_zoned_datetime("invalid")

    def test_error_serialize(self) -> None:
        datetime = dt.datetime(2000, 1, 1).astimezone(None)
        with raises(
            SerializeZonedDateTimeError,
            match="Unable to serialize zoned datetime; got 2000-01-01 00:00:00",
        ):
            _ = serialize_zoned_datetime(datetime)

    @given(data=data(), datetime=datetimes(timezones=sampled_from([HONG_KONG, UTC])))
    def test_ensure(self, *, data: DataObject, datetime: dt.datetime) -> None:
        str_or_value = data.draw(
            sampled_from([datetime, serialize_zoned_datetime(datetime)])
        )
        result = ensure_zoned_datetime(str_or_value)
        assert result == datetime


class TestToDatetimeDelta:
    def test_mixed_sign(self) -> None:
        timedelta = dt.timedelta(days=-1, seconds=1)
        result = _to_datetime_delta(timedelta)
        expected = DateTimeDelta(seconds=timedelta.total_seconds())
        assert result == expected

    def test_close_to_overflow(self) -> None:
        timedelta = dt.timedelta(days=104250, microseconds=1)
        result = _to_datetime_delta(timedelta)
        expected = DateTimeDelta(days=104250, microseconds=1)
        assert result == expected

    @mark.only
    @given(days=integers())
    def test_days_only(self, *, days: int) -> None:
        with assume_does_not_raise(OverflowError):
            timedelta = dt.timedelta(days=days)
        with assume_does_not_raise(_ToDateTimeDeltaError):
            result = _to_datetime_delta(timedelta)
        expected = DateTimeDelta(days=days)
        assert result == expected

    @given(microseconds=integers())
    def test_microseconds_only(self, *, microseconds: int) -> None:
        with assume_does_not_raise(OverflowError):
            timedelta = dt.timedelta(microseconds=microseconds)
        with assume_does_not_raise(_ToDateTimeDeltaError):
            result = _to_datetime_delta(timedelta)
        expected = DateTimeDelta(microseconds=microseconds)
        assert result == expected

    @mark.parametrize(
        "timedelta", [param(_TIMEDELTA_MICROSECONDS), param(_TIMEDELTA_OVERFLOW)]
    )
    def test_error(self, *, timedelta: dt.timedelta) -> None:
        with raises(
            _ToDateTimeDeltaError, match="Unable to serialize timedelta; got .*"
        ):
            _ = _to_datetime_delta(timedelta)
