from __future__ import annotations

import datetime as dt
from contextlib import suppress
from enum import Enum
from typing import Any, Generic, Literal, TypeVar

import luigi
from luigi import Parameter
from typing_extensions import override

from utilities.datetime import (
    EPOCH_UTC,
    ensure_date,
    ensure_datetime,
    ensure_time,
    parse_date,
    parse_datetime,
    parse_time,
    round_to_next_weekday,
    round_to_prev_weekday,
    serialize_date,
    serialize_datetime,
    serialize_time,
)
from utilities.enum import ensure_enum, parse_enum


class DateParameter(luigi.DateParameter):
    """A parameter which takes the value of a `dt.date`."""

    @override
    def normalize(self, value: dt.date | str) -> dt.date:
        return ensure_date(value)

    @override
    def parse(self, s: str) -> dt.date:
        return parse_date(s)

    @override
    def serialize(self, dt: dt.date) -> str:
        return serialize_date(dt)


class DateHourParameter(luigi.DateHourParameter):
    """A parameter which takes the value of an hourly `dt.datetime`."""

    def __init__(self, interval: int = 1, **kwargs: Any) -> None:
        super().__init__(interval, EPOCH_UTC, **kwargs)

    @override
    def normalize(self, dt: dt.datetime | str) -> dt.datetime:
        return ensure_datetime(dt)

    @override
    def parse(self, s: str) -> dt.datetime:
        return parse_datetime(s)

    @override
    def serialize(self, dt: dt.datetime) -> str:
        return serialize_datetime(dt)


class DateMinuteParameter(luigi.DateMinuteParameter):
    """A parameter which takes the value of a minutely `dt.datetime`."""

    def __init__(self, interval: int = 1, **kwargs: Any) -> None:
        super().__init__(interval=interval, start=EPOCH_UTC, **kwargs)

    @override
    def normalize(self, dt: dt.datetime | str) -> dt.datetime:
        return ensure_datetime(dt)

    @override
    def parse(self, s: str) -> dt.datetime:
        return parse_datetime(s)

    @override
    def serialize(self, dt: dt.datetime) -> str:
        return serialize_datetime(dt)


class DateSecondParameter(luigi.DateSecondParameter):
    """A parameter which takes the value of a secondly `dt.datetime`."""

    def __init__(self, interval: int = 1, **kwargs: Any) -> None:
        super().__init__(interval, EPOCH_UTC, **kwargs)

    @override
    def normalize(self, dt: dt.datetime | str) -> dt.datetime:
        return ensure_datetime(dt)

    @override
    def parse(self, s: str) -> dt.datetime:
        return parse_datetime(s)

    @override
    def serialize(self, dt: dt.datetime) -> str:
        return serialize_datetime(dt)


_E = TypeVar("_E", bound=Enum)


class EnumParameter(Parameter, Generic[_E]):
    """A parameter which takes the value of an Enum."""

    def __init__(
        self, enum: type[_E], /, *args: Any, case_sensitive: bool = True, **kwargs: Any
    ) -> None:
        super().__init__(*args, **kwargs)
        self._enum = enum
        self._case_sensitive = case_sensitive

    @override
    def normalize(self, x: _E | str) -> _E:
        return ensure_enum(self._enum, x, case_sensitive=self._case_sensitive)

    @override
    def parse(self, x: str) -> _E:
        return parse_enum(self._enum, x, case_sensitive=self._case_sensitive)

    @override
    def serialize(self, x: _E) -> str:
        return x.name


class TimeParameter(Parameter):
    """A parameter which takes the value of a `dt.time`."""

    @override
    def normalize(self, x: dt.time | str) -> dt.time:
        return ensure_time(x)

    @override
    def parse(self, x: str) -> dt.time:
        return parse_time(x)

    @override
    def serialize(self, x: dt.time) -> str:
        return serialize_time(x)


class WeekdayParameter(Parameter):
    """A parameter which takes the valeu of the previous/next weekday."""

    def __init__(
        self, *args: Any, rounding: Literal["prev", "next"] = "prev", **kwargs: Any
    ) -> None:
        super().__init__(*args, **kwargs)
        if rounding == "prev":
            self._rounder = round_to_prev_weekday
        else:
            self._rounder = round_to_next_weekday

    @override
    def normalize(self, x: dt.date | str) -> dt.date:
        with suppress(AttributeError, ModuleNotFoundError):
            from utilities.pandas import timestamp_to_date

            x = timestamp_to_date(x)
        return self._rounder(ensure_date(x))

    @override
    def parse(self, x: str) -> dt.date:
        return parse_date(x)

    @override
    def serialize(self, x: dt.date) -> str:
        return serialize_date(x)


__all__ = [
    "DateHourParameter",
    "DateMinuteParameter",
    "DateParameter",
    "DateSecondParameter",
    "EnumParameter",
    "TimeParameter",
    "WeekdayParameter",
]
