from __future__ import annotations

import datetime as dt
from datetime import tzinfo
from typing import Literal

from xlrd import Book, xldate_as_datetime

from utilities.datetime import UTC
from utilities.platform import SYSTEM, System, UnsupportedSystemError
from utilities.typing import never


def to_date(
    date: float, /, *, book: Book | None = None, tzinfo: tzinfo = UTC
) -> dt.date:
    return to_datetime(date, book=book, tzinfo=tzinfo).date()  # os-eq-linux


def to_datetime(
    date: float, /, *, book: Book | None = None, tzinfo: tzinfo = UTC
) -> dt.datetime:
    date_mode = _get_date_mode() if book is None else book.datemode  # os-eq-linux
    return xldate_as_datetime(date, date_mode).replace(tzinfo=tzinfo)  # os-eq-linux


def _get_date_mode() -> Literal[0, 1]:
    if SYSTEM is System.windows:  # pragma: os-ne-windows
        return 0
    if SYSTEM is System.mac:  # pragma: os-ne-macos
        return 1
    if SYSTEM is System.linux:  # pragma: no cover
        raise UnsupportedSystemError(SYSTEM)
    return never(SYSTEM)  # pragma: no cover
