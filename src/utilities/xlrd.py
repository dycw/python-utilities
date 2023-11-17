from __future__ import annotations

import datetime as dt
from datetime import tzinfo
from typing import Literal

from typing_extensions import assert_never
from xlrd import Book, xldate_as_datetime

from utilities.datetime import UTC
from utilities.platform import SYSTEM, System, UnsupportedSystemError


def to_date(
    date: float, /, *, book: Book | None = None, tzinfo: tzinfo = UTC
) -> dt.date:
    """Convert to a dt.date object."""
    return to_datetime(date, book=book, tzinfo=tzinfo).date()  # os-eq-linux


def to_datetime(
    date: float, /, *, book: Book | None = None, tzinfo: tzinfo = UTC
) -> dt.datetime:
    """Convert to a dt.datetime object."""
    date_mode = _get_date_mode() if book is None else book.datemode  # os-eq-linux
    return xldate_as_datetime(date, date_mode).replace(tzinfo=tzinfo)  # os-eq-linux


def _get_date_mode() -> Literal[0, 1]:
    match SYSTEM:
        case System.windows:  # pragma: os-ne-windows
            return 0
        case System.mac:  # pragma: os-ne-macos
            return 1
        case System.linux:  # pragma: no cover
            raise UnsupportedSystemError(SYSTEM)
        case _:  # pragma: no cover  # type: ignore
            assert_never(SYSTEM)


__all__ = ["to_date", "to_datetime"]
