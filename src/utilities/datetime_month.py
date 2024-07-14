from __future__ import annotations

from typing import TYPE_CHECKING

from month import Month

if TYPE_CHECKING:
    import datetime as dt


def date_to_month(date: dt.date, /) -> Month:
    """Construct a Month object."""
    return Month(date.year, date.month)


__all__ = ["date_to_month"]
