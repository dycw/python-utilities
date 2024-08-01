from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING, ClassVar

from hypothesis import given, reproduce_failure
from hypothesis.strategies import DataObject, data, integers, sampled_from, timedeltas
from pytest import mark, param, raises
from whenever import DateTimeDelta

from utilities.click import DateTime
from utilities.datetime import _DAYS_PER_YEAR, get_years
from utilities.whenever import (
    ParseTimedeltaError,
    _to_datetime_delta,
    ensure_timedelta,
    parse_timedelta,
    serialize_timedelta,
)


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
