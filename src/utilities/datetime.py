from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from re import search
from typing import assert_never, override

from utilities.iterables import OneEmptyError, one

_TWO_DIGIT_YEAR_MIN = 1969
_TWO_DIGIT_YEAR_MAX = _TWO_DIGIT_YEAR_MIN + 99
MIN_DATE_TWO_DIGIT_YEAR = dt.date(
    _TWO_DIGIT_YEAR_MIN, dt.date.min.month, dt.date.min.day
)
MAX_DATE_TWO_DIGIT_YEAR = dt.date(
    _TWO_DIGIT_YEAR_MAX, dt.date.max.month, dt.date.max.day
)


def parse_two_digit_year(year: int | str, /) -> int:
    """Parse a 2-digit year into a year."""
    match year:
        case int():
            years = range(_TWO_DIGIT_YEAR_MIN, _TWO_DIGIT_YEAR_MAX + 1)
            try:
                return one(y for y in years if y % 100 == year)
            except OneEmptyError:
                raise _ParseTwoDigitYearInvalidIntegerError(year=year) from None
        case str():
            if search(r"^\d{1,2}$", year):
                return parse_two_digit_year(int(year))
            raise _ParseTwoDigitYearInvalidStringError(year=year)
        case _ as never:
            assert_never(never)


@dataclass(kw_only=True, slots=True)
class ParseTwoDigitYearError(Exception):
    year: int | str


@dataclass(kw_only=True, slots=True)
class _ParseTwoDigitYearInvalidIntegerError(Exception):
    year: int | str

    @override
    def __str__(self) -> str:
        return f"Unable to parse year; got {self.year!r}"


@dataclass(kw_only=True, slots=True)
class _ParseTwoDigitYearInvalidStringError(Exception):
    year: int | str

    @override
    def __str__(self) -> str:
        return f"Unable to parse year; got {self.year!r}"


__all__ = ["MAX_DATE_TWO_DIGIT_YEAR", "MIN_DATE_TWO_DIGIT_YEAR", "parse_two_digit_year"]
