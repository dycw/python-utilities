from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Generic, Self, TypedDict, TypeVar, override
from zoneinfo import ZoneInfo

from whenever import Date, DateDelta, DateTimeDelta, TimeDelta, ZonedDateTime

from utilities.dataclasses import replace_non_sentinel
from utilities.functions import get_class_name
from utilities.sentinel import Sentinel, sentinel

_TPeriod = TypeVar("_TPeriod", Date, ZonedDateTime)


class _PeriodAsDict(TypedDict, Generic[_TPeriod]):
    start: _TPeriod
    end: _TPeriod


@dataclass(repr=False, order=True, unsafe_hash=True, kw_only=True)
class DatePeriod:
    """A period of dates."""

    start: Date
    end: Date

    def __post_init__(self) -> None:
        if self.start > self.end:
            raise _PeriodInvalidError(start=self.start, end=self.end)

    def __add__(self, other: DateDelta, /) -> Self:
        """Offset the period."""
        return self.replace(start=self.start + other, end=self.end + other)

    def __contains__(self, other: Date, /) -> bool:
        """Check if a date/datetime lies in the period."""
        return self.start <= other <= self.end

    @override
    def __repr__(self) -> str:
        cls = get_class_name(self)
        return f"{cls}({self.start}, {self.end})"

    def __sub__(self, other: DateDelta, /) -> Self:
        """Offset the period."""
        return self.replace(start=self.start - other, end=self.end - other)

    @property
    def duration(self) -> DateDelta:
        """The duration of the period."""
        return self.end - self.start

    def replace(
        self, *, start: Date | Sentinel = sentinel, end: Date | Sentinel = sentinel
    ) -> Self:
        """Replace elements of the period."""
        return replace_non_sentinel(self, start=start, end=end)

    def to_dict(self) -> _PeriodAsDict[Date]:
        """Convert the period to a dictionary."""
        return _PeriodAsDict(start=self.start, end=self.end)


@dataclass(repr=False, order=True, unsafe_hash=True, kw_only=True)
class ZonedDateTimePeriod:
    """A period of time."""

    start: ZonedDateTime
    end: ZonedDateTime

    def __post_init__(self) -> None:
        if self.start > self.end:
            raise _PeriodInvalidError(start=self.start, end=self.end)
        if self.start.tz != self.end.tz:
            raise _PeriodTimeZoneError(
                start=ZoneInfo(self.start.tz), end=ZoneInfo(self.end.tz)
            )

    def __add__(self, other: DateTimeDelta, /) -> Any:
        """Offset the period."""
        return self.replace(start=self.start + other, end=self.end + other)

    def __contains__(self, other: ZonedDateTime, /) -> bool:
        """Check if a date/datetime lies in the period."""
        return self.start <= other <= self.end

    @override
    def __repr__(self) -> str:
        cls = get_class_name(self)
        return f"{cls}({self.start.to_plain()}, {self.end})"

    def __sub__(self, other: Any, /) -> Any:
        """Offset the period."""
        return self.replace(start=self.start + other, end=self.end + other)

    @property
    def duration(self) -> TimeDelta:
        """The duration of the period."""
        return self.end - self.start

    def replace(
        self,
        *,
        start: ZonedDateTime | Sentinel = sentinel,
        end: ZonedDateTime | Sentinel = sentinel,
    ) -> Self:
        """Replace elements of the period."""
        return replace_non_sentinel(self, start=start, end=end)

    @property
    def time_zone(self) -> ZoneInfo:
        """The time zone of the period."""
        return ZoneInfo(self.start.tz)

    def to_dict(self) -> _PeriodAsDict[ZonedDateTime]:
        """Convert the period to a dictionary."""
        return _PeriodAsDict(start=self.start, end=self.end)


@dataclass(kw_only=True, slots=True)
class PeriodError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _PeriodInvalidError(PeriodError, Generic[_TPeriod]):
    start: _TPeriod
    end: _TPeriod

    @override
    def __str__(self) -> str:
        return f"Invalid period; got {self.start} > {self.end}"


@dataclass(kw_only=True, slots=True)
class _PeriodTimeZoneError(PeriodError):
    start: ZoneInfo
    end: ZoneInfo

    @override
    def __str__(self) -> str:
        return f"Period must contain exactly one time zone; got {self.start} and {self.end}"


__all__ = ["DatePeriod", "PeriodError", "ZonedDateTimePeriod"]
