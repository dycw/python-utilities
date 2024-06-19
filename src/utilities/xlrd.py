from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from typing_extensions import override
from xlrd import Book, xldate_as_datetime

from utilities.datetime import UTC
from utilities.platform import SYSTEM, System

if TYPE_CHECKING:
    import datetime as dt
    from datetime import tzinfo


def get_date_mode() -> Literal[0, 1]:
    match SYSTEM:
        case System.windows:  # pragma: os-ne-windows
            return 0
        case System.mac:  # pragma: os-ne-macos
            return 1
        case system:  # pragma: no cover
            raise GetDateModeError(system=system)


@dataclass(kw_only=True)
class GetDateModeError(Exception):
    system: System

    @override
    def __str__(self) -> str:
        return (  # pragma: no cover
            f"System must be one of Windows or Darwin; got {self.system} instead"
        )


def to_date(
    date: float, /, *, book: Book | None = None, tzinfo: tzinfo = UTC
) -> dt.date:
    """Convert to a dt.date object."""
    return to_datetime(date, book=book, tzinfo=tzinfo).date()  # os-eq-linux


def to_datetime(
    date: float, /, *, book: Book | None = None, tzinfo: tzinfo = UTC
) -> dt.datetime:
    """Convert to a dt.datetime object."""
    date_mode = get_date_mode() if book is None else book.datemode  # os-eq-linux
    return xldate_as_datetime(date, date_mode).replace(tzinfo=tzinfo)  # os-eq-linux


__all__ = ["GetDateModeError", "get_date_mode", "to_date", "to_datetime"]
