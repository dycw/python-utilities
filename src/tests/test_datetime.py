from __future__ import annotations

import datetime as dt
from re import search

from hypothesis import given
from hypothesis.strategies import DataObject, data, dates, integers, sampled_from
from pytest import raises

from utilities.datetime import (
    EnsureMonthError,
    Month,
    ParseMonthError,
    _ParseTwoDigitYearInvalidIntegerError,
    _ParseTwoDigitYearInvalidStringError,
    date_to_month,
    ensure_month,
    parse_month,
    parse_two_digit_year,
    serialize_month,
)
from utilities.hypothesis import months
from utilities.zoneinfo import UTC


class TestDateToMonth:
    @given(date=dates())
    def test_main(self, *, date: dt.date) -> None:
        result = date_to_month(date).to_date(day=date.day)
        assert result == date


class TestMaybeSubPctY:
    @given(text=text_clean())
    def test_main(self, *, text: str) -> None:
        result = maybe_sub_pct_y(text)
        _ = assume(not search("%Y", result))
        assert not search("%Y", result)


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
