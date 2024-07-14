from __future__ import annotations

from typing import TYPE_CHECKING

from hypothesis import given
from hypothesis.strategies import dates, integers

from utilities.datetime_month import date_to_month
from utilities.hypothesis import assume_does_not_raise

if TYPE_CHECKING:
    import datetime as dt


class TestDateToMonth:
    @given(date=dates(), day=integers(1, 31))
    def test_main(self, *, date: dt.date, day: int) -> None:
        result1 = date_to_month(date)
        with assume_does_not_raise(ValueError):
            date2 = date.replace(day=day)
        result2 = date_to_month(date2)
        assert result1 == result2
