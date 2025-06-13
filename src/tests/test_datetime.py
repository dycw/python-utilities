from __future__ import annotations

import datetime as dt

from hypothesis import given
from hypothesis.strategies import DataObject, data, integers, sampled_from
from pytest import raises

from utilities.datetime import (
    _ParseTwoDigitYearInvalidIntegerError,
    _ParseTwoDigitYearInvalidStringError,
    parse_two_digit_year,
)
from utilities.zoneinfo import UTC


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
