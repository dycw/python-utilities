from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Generic,
    Literal,
    Self,
    TypeAlias,
    TypedDict,
    TypeVar,
    assert_never,
    cast,
)
from zoneinfo import ZoneInfo

from typing_extensions import override

from utilities.datetime import (
    ZERO_TIME,
    CheckZonedDatetimeError,
    check_zoned_datetime,
    is_instance_date_not_datetime,
)
from utilities.functions import get_class_name
from utilities.iterables import OneNonUniqueError, always_iterable, one
from utilities.sentinel import Sentinel, sentinel
from utilities.whenever import (
    serialize_date,
    serialize_local_datetime,
    serialize_zoned_datetime,
)

if TYPE_CHECKING:
    from utilities.iterables import MaybeIterable

_DateOrDatetime: TypeAlias = Literal["date", "datetime"]
_TPeriod = TypeVar("_TPeriod", dt.date, dt.datetime)


class _PeriodAsDict(TypedDict, Generic[_TPeriod]):
    start: _TPeriod
    end: _TPeriod


@dataclass(repr=False, order=True, unsafe_hash=True, slots=True)
class Period(Generic[_TPeriod]):
    """A period of time."""

    start: _TPeriod
    end: _TPeriod
    req_duration: MaybeIterable[dt.timedelta] | None = field(default=None, kw_only=True)
    min_duration: dt.timedelta | None = field(default=None, kw_only=True)
    max_duration: dt.timedelta | None = field(default=None, kw_only=True)

    def __post_init__(self) -> None:
        start_date_not_datetime, end_date_not_datetime = map(
            is_instance_date_not_datetime, [self.start, self.end]
        )
        if start_date_not_datetime is not end_date_not_datetime:
            raise _PeriodDateAndDatetimeMixedError(start=self.start, end=self.end)
        for date in [self.start, self.end]:
            if isinstance(date, dt.datetime):
                try:
                    check_zoned_datetime(date)
                except CheckZonedDatetimeError:
                    raise _PeriodNaiveDatetimeError(
                        start=self.start, end=self.end
                    ) from None
        duration = self.end - self.start
        if duration < ZERO_TIME:
            raise _PeriodInvalidError(start=self.start, end=self.end)
        if (self.req_duration is not None) and (
            duration not in always_iterable(self.req_duration)
        ):
            raise _PeriodReqDurationError(
                start=self.start,
                end=self.end,
                duration=duration,
                req_duration=self.req_duration,
            )
        if (self.min_duration is not None) and (duration < self.min_duration):
            raise _PeriodMinDurationError(
                start=self.start,
                end=self.end,
                duration=duration,
                min_duration=self.min_duration,
            )
        if (self.max_duration is not None) and (duration > self.max_duration):
            raise _PeriodMaxDurationError(
                start=self.start,
                end=self.end,
                duration=duration,
                max_duration=self.max_duration,
            )

    def __add__(self, other: dt.timedelta, /) -> Self:
        """Offset the period."""
        return self.replace(start=self.start + other, end=self.end + other)

    @override
    def __repr__(self) -> str:
        cls = get_class_name(self)
        match self.kind:
            case "date":
                result = cast(Period[dt.date], self)
                start, end = map(serialize_date, [result.start, result.end])
                return f"{cls}({start}, {end})"
            case "datetime":
                result = cast(Period[dt.datetime], self)
                try:
                    time_zone = result.time_zone
                except _PeriodTimeZoneNonUniqueError:
                    start, end = map(
                        serialize_zoned_datetime, [result.start, result.end]
                    )
                    return f"{cls}({start}, {end})"
                start, end = (
                    serialize_local_datetime(t.replace(tzinfo=None))
                    for t in [result.start, result.end]
                )
                return f"{cls}({start}, {end}, {time_zone})"
            case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
                assert_never(never)

    def __sub__(self, other: dt.timedelta, /) -> Self:
        """Offset the period."""
        return self.replace(start=self.start - other, end=self.end - other)

    def astimezone(self, time_zone: ZoneInfo, /) -> Self:
        """Convert the timezone of the period, if it is a datetime period."""
        match self.kind:
            case "date":
                raise _PeriodAsTimeZoneInapplicableError(start=self.start, end=self.end)
            case "datetime":
                result = cast(Period[dt.datetime], self)
                result = result.replace(
                    start=result.start.astimezone(time_zone),
                    end=result.end.astimezone(time_zone),
                )
                return cast(Self, result)
            case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
                assert_never(never)

    @property
    def duration(self) -> dt.timedelta:
        """The duration of the period."""
        return self.end - self.start

    @property
    def kind(self) -> _DateOrDatetime:
        """The kind of the period."""
        return "date" if is_instance_date_not_datetime(self.start) else "datetime"

    def replace(
        self,
        *,
        start: _TPeriod | None = None,
        end: _TPeriod | None = None,
        req_duration: MaybeIterable[dt.timedelta] | None | Sentinel = sentinel,
        min_duration: dt.timedelta | None | Sentinel = sentinel,
        max_duration: dt.timedelta | None | Sentinel = sentinel,
    ) -> Self:
        """Replace elements of the period."""
        return type(self)(
            self.start if start is None else start,
            self.end if end is None else end,
            req_duration=self.req_duration
            if isinstance(req_duration, Sentinel)
            else req_duration,
            min_duration=self.min_duration
            if isinstance(min_duration, Sentinel)
            else min_duration,
            max_duration=self.max_duration
            if isinstance(max_duration, Sentinel)
            else max_duration,
        )

    @property
    def time_zone(self) -> ZoneInfo:
        """The time zone of the period."""
        match self.kind:
            case "date":
                raise _PeriodTimeZoneInapplicableError(
                    start=self.start, end=self.end
                ) from None
            case "datetime":
                result = cast(Period[dt.datetime], self)
                time_zones = {
                    t
                    for t in (result.start.tzinfo, result.end.tzinfo)
                    if isinstance(t, ZoneInfo)
                }
                try:
                    return one(time_zones)
                except OneNonUniqueError as error:
                    raise _PeriodTimeZoneNonUniqueError(
                        start=self.start,
                        end=self.end,
                        first=error.first,
                        second=error.second,
                    ) from None
            case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
                assert_never(never)

    def to_dict(self) -> _PeriodAsDict:
        """Convert the period to a dictionary."""
        return {"start": self.start, "end": self.end}


@dataclass(kw_only=True, slots=True)
class PeriodError(Generic[_TPeriod], Exception):
    start: _TPeriod
    end: _TPeriod


@dataclass(kw_only=True, slots=True)
class _PeriodDateAndDatetimeMixedError(PeriodError[_TPeriod]):
    @override
    def __str__(self) -> str:
        return f"Invalid period; got date and datetime mix ({self.start}, {self.end})"


@dataclass(kw_only=True, slots=True)
class _PeriodNaiveDatetimeError(PeriodError[_TPeriod]):
    @override
    def __str__(self) -> str:
        return f"Invalid period; got naive datetime(s) ({self.start}, {self.end})"


@dataclass(kw_only=True, slots=True)
class _PeriodInvalidError(PeriodError[_TPeriod]):
    @override
    def __str__(self) -> str:
        return f"Invalid period; got {self.start} > {self.end}"


@dataclass(kw_only=True, slots=True)
class _PeriodReqDurationError(PeriodError[_TPeriod]):
    duration: dt.timedelta
    req_duration: MaybeIterable[dt.timedelta]

    @override
    def __str__(self) -> str:
        return f"Period must have duration {self.req_duration}; got {self.duration})"


@dataclass(kw_only=True, slots=True)
class _PeriodMinDurationError(PeriodError[_TPeriod]):
    duration: dt.timedelta
    min_duration: dt.timedelta

    @override
    def __str__(self) -> str:
        return (
            f"Period must have min duration {self.min_duration}; got {self.duration})"
        )


@dataclass(kw_only=True, slots=True)
class _PeriodMaxDurationError(PeriodError[_TPeriod]):
    duration: dt.timedelta
    max_duration: dt.timedelta

    @override
    def __str__(self) -> str:
        return f"Period must have duration at most {self.max_duration}; got {self.duration})"


@dataclass(kw_only=True, slots=True)
class _PeriodAsTimeZoneInapplicableError(PeriodError[_TPeriod]):
    @override
    def __str__(self) -> str:
        return "Period of dates does not have a timezone attribute"


@dataclass(kw_only=True, slots=True)
class _PeriodTimeZoneInapplicableError(PeriodError[_TPeriod]):
    @override
    def __str__(self) -> str:
        return "Period of dates does not have a timezone attribute"


@dataclass(kw_only=True, slots=True)
class _PeriodTimeZoneNonUniqueError(PeriodError[_TPeriod]):
    first: ZoneInfo
    second: ZoneInfo

    @override
    def __str__(self) -> str:
        return f"Period must contain exactly one time zone; got {self.first} and {self.second}"


__all__ = ["Period", "PeriodError"]