from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING, ClassVar

from hypothesis import given, reproduce_failure
from hypothesis.strategies import (
    DataObject,
    data,
    dates,
    datetimes,
    integers,
    sampled_from,
    timedeltas,
)
from pytest import mark, param, raises
from whenever import DateTimeDelta

from utilities.click import DateTime
from utilities.datetime import _DAYS_PER_YEAR, get_years
from utilities.types import ensure_date
from utilities.whenever import (
    ParseDateError,
    ParseLocalDateTimeError,
    ParseTimedeltaError,
    _to_datetime_delta,
    ensure_local_datetime,
    ensure_timedelta,
    parse_date,
    parse_local_datetime,
    parse_timedelta,
    serialize_date,
    serialize_local_datetime,
    serialize_timedelta,
)


class TestParseAndSerializeDate:
    @given(date=dates())
    def test_main(self, *, date: dt.date) -> None:
        serialized = serialize_date(date)
        result = parse_date(serialized)
        assert result == date

    def test_error(self) -> None:
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

    def test_error(self) -> None:
        with raises(
            ParseLocalDateTimeError,
            match="Unable to parse local datetime; got 'invalid'",
        ):
            _ = parse_local_datetime("invalid")

    @given(data=data(), datetime=datetimes())
    def test_ensure(self, *, data: DataObject, datetime: dt.datetime) -> None:
        str_or_value = data.draw(
            sampled_from([datetime, serialize_local_datetime(datetime)])
        )
        result = ensure_local_datetime(str_or_value)
        assert result == datetime


class TestParseAndSerializeTimedelta:
    max_timedelta: ClassVar[dt.timedelta] = get_years(n=10)

    @given(timedelta=timedeltas(min_value=-max_timedelta, max_value=max_timedelta))
    def test_main(self, *, timedelta: dt.timedelta) -> None:
        serialized = serialize_timedelta(timedelta)
        result = parse_timedelta(serialized)
        assert result == timedelta

    def test_error_parse(self) -> None:
        with raises(
            ParseTimedeltaError, match="Unable to parse timedelta; got 'invalid'"
        ):
            _ = parse_timedelta("invalid")

    def test_error_nano_seconds(self) -> None:
        with raises(
            ParseTimedeltaError, match="Unable to parse timedelta; got 333 nanoseconds"
        ):
            _ = parse_timedelta("PT0.111222333S")

    @given(
        data=data(),
        timedelta=timedeltas(min_value=-max_timedelta, max_value=max_timedelta),
    )
    def test_ensure(self, *, data: DataObject, timedelta: dt.timedelta) -> None:
        str_or_value = data.draw(
            sampled_from([timedelta, serialize_timedelta(timedelta)])
        )
        result = ensure_timedelta(str_or_value)
        assert result == timedelta


class TestToDatetimeDelta:
    max_microseconds: ClassVar[int] = int(1e9)

    def test_mixed_sign(self) -> None:
        timedelta = dt.timedelta(days=-1, seconds=1)
        result = _to_datetime_delta(timedelta)
        expected = DateTimeDelta(seconds=timedelta.total_seconds())
        assert result == expected

    @given(microseconds=integers(-int(max_microseconds), int(max_microseconds)))
    def test_next(self, *, microseconds: int) -> None:
        timedelta = dt.timedelta(microseconds=microseconds)
        result = _to_datetime_delta(timedelta)
        expected = DateTimeDelta(microseconds=microseconds)
        assert result == expected
