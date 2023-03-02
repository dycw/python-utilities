import datetime as dt
from enum import Enum as _Enum
from typing import Any, Generic, Optional, TypeVar

from beartype import beartype
from click import Context, Parameter, ParamType, option

from utilities.datetime import (
    ParseDateError,
    ParseDateTimeError,
    ParseTimeError,
    TimedeltaError,
    ensure_date,
    ensure_datetime,
    ensure_time,
    ensure_timedelta,
)
from utilities.enum import (
    MultipleMatchingMembersError,
    NoMatchingMemberError,
    ensure_enum,
)
from utilities.logging import LogLevel


class Date(ParamType):
    """A date-valued parameter."""

    name = "date"

    @beartype
    def convert(
        self, value: Any, param: Optional[Parameter], ctx: Optional[Context]
    ) -> dt.date:
        """Convert a value into the `Date` type."""
        try:
            return ensure_date(value)
        except ParseDateError:
            self.fail(f"Unable to parse {value}", param, ctx)


class DateTime(ParamType):
    """A datetime-valued parameter."""

    name = "datetime"

    @beartype
    def convert(
        self, value: Any, param: Optional[Parameter], ctx: Optional[Context]
    ) -> dt.date:
        """Convert a value into the `DateTime` type."""
        try:
            return ensure_datetime(value)
        except ParseDateTimeError:
            self.fail(f"Unable to parse {value}", param, ctx)


class Time(ParamType):
    """A time-valued parameter."""

    name = "time"

    @beartype
    def convert(
        self, value: Any, param: Optional[Parameter], ctx: Optional[Context]
    ) -> dt.time:
        """Convert a value into the `Time` type."""
        try:
            return ensure_time(value)
        except ParseTimeError:
            self.fail(f"Unable to parse {value}", param, ctx)


class Timedelta(ParamType):
    """A timedelta-valued parameter."""

    name = "timedelta"

    @beartype
    def convert(
        self, value: Any, param: Optional[Parameter], ctx: Optional[Context]
    ) -> dt.timedelta:
        """Convert a value into the `Timedelta` type."""
        try:
            return ensure_timedelta(value)
        except TimedeltaError:
            self.fail(f"Unable to parse {value}", param, ctx)


_E = TypeVar("_E", bound=_Enum)


class Enum(ParamType, Generic[_E]):
    """An enum-valued parameter."""

    name = "enum"

    @beartype
    def __init__(self, enum: type[_E], /, *, case_sensitive: bool = True) -> None:
        super().__init__()
        self._enum = enum
        self._case_sensitive = case_sensitive

    @beartype
    def convert(
        self, value: Any, param: Optional[Parameter], ctx: Optional[Context]
    ) -> _E:
        """Convert a value into the `Enum` type."""
        try:
            return ensure_enum(self._enum, value, case_sensitive=self._case_sensitive)
        except (NoMatchingMemberError, MultipleMatchingMembersError):
            return self.fail(f"Unable to parse {value}", param, ctx)


log_level_option = option(
    "-ll",
    "--log-level",
    type=Enum(LogLevel, case_sensitive=False),
    default=LogLevel.INFO,
    show_default=True,
    help="The logging level",
)
