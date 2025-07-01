from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Self, TypedDict, overload, override
from zoneinfo import ZoneInfo

from whenever import Date, DateDelta, PlainDateTime, TimeDelta, ZonedDateTime

from utilities.dataclasses import replace_non_sentinel
from utilities.functions import get_class_name
from utilities.sentinel import Sentinel, sentinel
from utilities.whenever import format_compact
from utilities.zoneinfo import get_time_zone_name

if TYPE_CHECKING:
    from utilities.types import TimeZoneLike


class _PeriodAsDict[T: (Date, ZonedDateTime)](TypedDict):
    start: T
    end: T


@dataclass(repr=False, order=True, unsafe_hash=True, kw_only=False)
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
    def delta(self) -> DateDelta:
        """The delta of the period."""
        return self.end - self.start

    def format_compact(self) -> str:
        """Format the period in a compact fashion."""
        fc, start, end = format_compact, self.start, self.end
        if self.start == self.end:
            return f"{fc(start)}="
        if self.start.year_month() == self.end.year_month():
            return f"{fc(start)}-{fc(end, fmt='%d')}"
        if self.start.year == self.end.year:
            return f"{fc(start)}-{fc(end, fmt='%m%d')}"
        return f"{fc(start)}-{fc(end)}"

    def replace(
        self, *, start: Date | Sentinel = sentinel, end: Date | Sentinel = sentinel
    ) -> Self:
        """Replace elements of the period."""
        return replace_non_sentinel(self, start=start, end=end)

    def to_dict(self) -> _PeriodAsDict[Date]:
        """Convert the period to a dictionary."""
        return _PeriodAsDict(start=self.start, end=self.end)


@dataclass(repr=False, order=True, unsafe_hash=True, kw_only=False)
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

    def __add__(self, other: TimeDelta, /) -> Self:
        """Offset the period."""
        return self.replace(start=self.start + other, end=self.end + other)

    def __contains__(self, other: ZonedDateTime, /) -> bool:
        """Check if a date/datetime lies in the period."""
        return self.start <= other <= self.end

    @override
    def __repr__(self) -> str:
        cls = get_class_name(self)
        return f"{cls}({self.start.to_plain()}, {self.end.to_plain()}[{self.time_zone.key}])"

    def __sub__(self, other: TimeDelta, /) -> Self:
        """Offset the period."""
        return self.replace(start=self.start - other, end=self.end - other)

    @property
    def delta(self) -> TimeDelta:
        """The duration of the period."""
        return self.end - self.start

    @overload
    def exact_eq(self, period: ZonedDateTimePeriod, /) -> bool: ...
    @overload
    def exact_eq(self, start: ZonedDateTime, end: ZonedDateTime, /) -> bool: ...
    @overload
    def exact_eq(
        self, start: PlainDateTime, end: PlainDateTime, time_zone: ZoneInfo, /
    ) -> bool: ...
    def exact_eq(self, *args: Any) -> bool:
        """Check if a period is exactly equal to another."""
        if (len(args) == 1) and isinstance(args[0], ZonedDateTimePeriod):
            return self.start.exact_eq(args[0].start) and self.end.exact_eq(args[0].end)
        if (
            (len(args) == 2)
            and isinstance(args[0], ZonedDateTime)
            and isinstance(args[1], ZonedDateTime)
        ):
            return self.exact_eq(ZonedDateTimePeriod(args[0], args[1]))
        if (
            (len(args) == 3)
            and isinstance(args[0], PlainDateTime)
            and isinstance(args[1], PlainDateTime)
            and isinstance(args[2], ZoneInfo)
        ):
            return self.exact_eq(
                ZonedDateTimePeriod(
                    args[0].assume_tz(args[2].key), args[1].assume_tz(args[2].key)
                )
            )
        raise _PeriodExactEqArgumentsError(args=args)

    def format_compact(self) -> str:
        """Format the period in a compact fashion."""
        fc, start, end = format_compact, self.start, self.end
        if start == end:
            if end.second != 0:
                return f"{fc(start)}="
            if end.minute != 0:
                return f"{fc(start, fmt='%Y%m%dT%H%M')}="
            return f"{fc(start, fmt='%Y%m%dT%H')}="
        if start.date() == end.date():
            if end.second != 0:
                return f"{fc(start.to_plain())}-{fc(end, fmt='%H%M%S')}"
            if end.minute != 0:
                return f"{fc(start.to_plain())}-{fc(end, fmt='%H%M')}"
            return f"{fc(start.to_plain())}-{fc(end, fmt='%H')}"
        if start.date().year_month() == end.date().year_month():
            if end.second != 0:
                return f"{fc(start.to_plain())}-{fc(end, fmt='%dT%H%M%S')}"
            if end.minute != 0:
                return f"{fc(start.to_plain())}-{fc(end, fmt='%dT%H%M')}"
            return f"{fc(start.to_plain())}-{fc(end, fmt='%dT%H')}"
        if start.year == end.year:
            if end.second != 0:
                return f"{fc(start.to_plain())}-{fc(end, fmt='%m%dT%H%M%S')}"
            if end.minute != 0:
                return f"{fc(start.to_plain())}-{fc(end, fmt='%m%dT%H%M')}"
            return f"{fc(start.to_plain())}-{fc(end, fmt='%m%dT%H')}"
        if end.second != 0:
            return f"{fc(start.to_plain())}-{fc(end)}"
        if end.minute != 0:
            return f"{fc(start.to_plain())}-{fc(end, fmt='%Y%m%dT%H%M')}"
        return f"{fc(start.to_plain())}-{fc(end, fmt='%Y%m%dT%H')}"

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

    def to_tz(self, time_zone: TimeZoneLike, /) -> Self:
        """Convert the time zone."""
        tz = get_time_zone_name(time_zone)
        return self.replace(start=self.start.to_tz(tz), end=self.end.to_tz(tz))


@dataclass(kw_only=True, slots=True)
class PeriodError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _PeriodInvalidError[T: (Date, ZonedDateTime)](PeriodError):
    start: T
    end: T

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


@dataclass(kw_only=True, slots=True)
class _PeriodExactEqArgumentsError(PeriodError):
    args: tuple[Any, ...]

    @override
    def __str__(self) -> str:
        return f"Invalid arguments; got {self.args}"


__all__ = ["DatePeriod", "PeriodError", "ZonedDateTimePeriod"]
