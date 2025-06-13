from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING

from hypothesis import given
from hypothesis.strategies import DataObject, data, dates, integers, sampled_from
from pytest import mark, param, raises

from utilities.datetime import (
    EnsureMonthError,
    Month,
    MonthError,
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

if TYPE_CHECKING:
    from collections.abc import Callable


class TestDateToMonth:
    @given(date=dates())
    def test_main(self, *, date: dt.date) -> None:
        result = date_to_month(date).to_date(day=date.day)
        assert result == date


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
