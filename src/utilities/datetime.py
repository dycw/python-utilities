from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, replace
from re import search
from typing import Any, Self, assert_never, overload, override

from utilities.iterables import OneEmptyError, one
from utilities.types import MaybeStr
from utilities.zoneinfo import UTC


def date_to_month(date: dt.date, /) -> Month:
    """Collapse a date into a month."""
    return Month(year=date.year, month=date.month)


##


def ensure_month(month: MonthLike, /) -> Month:
    """Ensure the object is a month."""
    if isinstance(month, Month):
        return month
    try:
        return parse_month(month)
    except ParseMonthError as error:
        raise EnsureMonthError(month=error.month) from None


@dataclass(kw_only=True, slots=True)
class EnsureMonthError(Exception):
    month: str

    @override
    def __str__(self) -> str:
        return f"Unable to ensure month; got {self.month!r}"


##


@dataclass(order=True, unsafe_hash=True, slots=True)
class Month:
    """Represents a month in time."""

    year: int
    month: int

    def __post_init__(self) -> None:
        try:
            _ = dt.date(self.year, self.month, 1)
        except ValueError:
            raise MonthError(year=self.year, month=self.month) from None

    @override
    def __repr__(self) -> str:
        return serialize_month(self)

    @override
    def __str__(self) -> str:
        return repr(self)

    def __add__(self, other: Any, /) -> Self:
        if not isinstance(other, int):  # pragma: no cover
            return NotImplemented
        years, month = divmod(self.month + other - 1, 12)
        month += 1
        year = self.year + years
        return replace(self, year=year, month=month)

    @overload
    def __sub__(self, other: Self, /) -> int: ...
    @overload
    def __sub__(self, other: int, /) -> Self: ...
    def __sub__(self, other: Self | int, /) -> Self | int:
        if isinstance(other, int):  # pragma: no cover
            return self + (-other)
        if isinstance(other, type(self)):
            self_as_int = 12 * self.year + self.month
            other_as_int = 12 * other.year + other.month
            return self_as_int - other_as_int
        return NotImplemented  # pragma: no cover

    @classmethod
    def from_date(cls, date: dt.date, /) -> Self:
        return cls(year=date.year, month=date.month)

    def to_date(self, /, *, day: int = 1) -> dt.date:
        return dt.date(self.year, self.month, day)


@dataclass(kw_only=True, slots=True)
class MonthError(Exception):
    year: int
    month: int

    @override
    def __str__(self) -> str:
        return f"Invalid year and month: {self.year}, {self.month}"


type DateOrMonth = dt.date | Month
type MonthLike = MaybeStr[Month]
MIN_MONTH = Month(dt.date.min.year, dt.date.min.month)
MAX_MONTH = Month(dt.date.max.year, dt.date.max.month)


##


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


##


def serialize_month(month: Month, /) -> str:
    """Serialize a month."""
    return f"{month.year:04}-{month.month:02}"


def parse_month(month: str, /) -> Month:
    """Parse a string into a month."""
    for fmt in ["%Y-%m", "%Y%m", "%Y %m"]:
        try:
            date = dt.datetime.strptime(month, fmt).replace(tzinfo=UTC).date()
        except ValueError:
            pass
        else:
            return Month(date.year, date.month)
    raise ParseMonthError(month=month)


@dataclass(kw_only=True, slots=True)
class ParseMonthError(Exception):
    month: str

    @override
    def __str__(self) -> str:
        return f"Unable to parse month; got {self.month!r}"


__all__ = [
    "MAX_DATE_TWO_DIGIT_YEAR",
    "MAX_MONTH",
    "MIN_DATE_TWO_DIGIT_YEAR",
    "MIN_MONTH",
    "DateOrMonth",
    "EnsureMonthError",
    "Month",
    "MonthError",
    "MonthLike",
    "ParseMonthError",
    "date_to_month",
    "ensure_month",
    "parse_two_digit_year",
]
