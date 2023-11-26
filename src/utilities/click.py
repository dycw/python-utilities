from __future__ import annotations

import datetime as dt
from enum import Enum as _Enum
from typing import Any, Generic, TypeVar

from click import Context, Parameter, ParamType, option
from typing_extensions import override

from utilities.datetime import (
    ParseDateError,
    ParseDateTimeError,
    ParseTimedeltaError,
    ParseTimeError,
    ensure_date,
    ensure_datetime,
    ensure_time,
    ensure_timedelta,
)
from utilities.enum import ParseEnumError, ensure_enum
from utilities.logging import LogLevel


class Date(ParamType):
    """A date-valued parameter."""

    name = "date"

    @override
    def convert(
        self, value: Any, param: Parameter | None, ctx: Context | None
    ) -> dt.date:
        """Convert a value into the `Date` type."""
        try:
            return ensure_date(value)
        except ParseDateError:
            self.fail(f"Unable to parse {value}", param, ctx)


class DateTime(ParamType):
    """A datetime-valued parameter."""

    name = "datetime"

    @override
    def convert(
        self, value: Any, param: Parameter | None, ctx: Context | None
    ) -> dt.date:
        """Convert a value into the `DateTime` type."""
        try:
            return ensure_datetime(value)
        except ParseDateTimeError:
            self.fail(f"Unable to parse {value}", param, ctx)


class Time(ParamType):
    """A time-valued parameter."""

    name = "time"

    @override
    def convert(
        self, value: Any, param: Parameter | None, ctx: Context | None
    ) -> dt.time:
        """Convert a value into the `Time` type."""
        try:
            return ensure_time(value)
        except ParseTimeError:
            self.fail(f"Unable to parse {value}", param, ctx)


class Timedelta(ParamType):
    """A timedelta-valued parameter."""

    name = "timedelta"

    @override
    def convert(
        self, value: Any, param: Parameter | None, ctx: Context | None
    ) -> dt.timedelta:
        """Convert a value into the `Timedelta` type."""
        try:
            return ensure_timedelta(value)
        except ParseTimedeltaError:
            self.fail(f"Unable to parse {value}", param, ctx)


_E = TypeVar("_E", bound=_Enum)


class Enum(ParamType, Generic[_E]):
    """An enum-valued parameter."""

    name = "enum"

    def __init__(self, enum: type[_E], /, *, case_sensitive: bool = True) -> None:
        super().__init__()
        self._enum = enum
        self._case_sensitive = case_sensitive

    @override
    def convert(self, value: Any, param: Parameter | None, ctx: Context | None) -> _E:
        """Convert a value into the `Enum` type."""
        try:
            return ensure_enum(self._enum, value, case_sensitive=self._case_sensitive)
        except ParseEnumError:
            return self.fail(f"Unable to parse {value}", param, ctx)


log_level_option = option(
    "-ll",
    "--log-level",
    type=Enum(LogLevel, case_sensitive=False),
    default=LogLevel.INFO,
    show_default=True,
    help="The logging level",
)


__all__ = ["Date", "DateTime", "Enum", "Time", "Timedelta", "log_level_option"]


try:
    from utilities._click.luigi import (
        local_scheduler_option_default_central,
        local_scheduler_option_default_local,
        workers_option,
    )
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += [
        "local_scheduler_option_default_central",
        "local_scheduler_option_default_local",
        "workers_option",
    ]


try:
    from utilities._click.sqlalchemy import Engine
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += ["Engine"]
