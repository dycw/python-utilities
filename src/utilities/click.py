import datetime as dt
from contextlib import suppress
from enum import Enum as _Enum
from typing import Any
from typing import Generic
from typing import TypeVar

from beartype import beartype
from click import Context
from click import ParamType
from click import Parameter
from click import option

from utilities.datetime import InvalidDate
from utilities.datetime import InvalidDateTime
from utilities.datetime import InvalidTime
from utilities.datetime import InvalidTimedelta
from utilities.datetime import parse_date
from utilities.datetime import parse_datetime
from utilities.datetime import parse_time
from utilities.datetime import parse_timedelta
from utilities.logging import LogLevel


class Date(ParamType):
    """A date-valued parameter."""

    name = "date"

    @beartype
    def convert(
        self, value: Any, param: Parameter | None, ctx: Context | None
    ) -> dt.date:
        try:
            return parse_date(value)
        except InvalidDate:
            self.fail(f"Unable to parse {value}", param, ctx)


class DateTime(ParamType):
    """A datetime-valued parameter."""

    name = "datetime"

    @beartype
    def convert(
        self, value: Any, param: Parameter | None, ctx: Context | None
    ) -> dt.date:
        try:
            return parse_datetime(value)
        except InvalidDateTime:
            self.fail(f"Unable to parse {value}", param, ctx)


class Time(ParamType):
    """A time-valued parameter."""

    name = "time"

    @beartype
    def convert(
        self, value: Any, param: Parameter | None, ctx: Context | None
    ) -> dt.time:
        try:
            return parse_time(value)
        except InvalidTime:
            self.fail(f"Unable to parse {value}", param, ctx)


class Timedelta(ParamType):
    """A timedelta-valued parameter."""

    name = "timedelta"

    @beartype
    def convert(
        self, value: Any, param: Parameter | None, ctx: Context | None
    ) -> dt.timedelta:
        try:
            return parse_timedelta(value)
        except InvalidTimedelta:
            self.fail(f"Unable to parse {value}", param, ctx)


_E = TypeVar("_E", bound=_Enum)


class Enum(ParamType, Generic[_E]):
    """An enum-valued parameter."""

    name = "enum"

    @beartype
    def __init__(self, enum: type[_E]) -> None:
        super().__init__()
        self._enum = enum

    @beartype
    def convert(
        self, value: Any, param: Parameter | None, ctx: Context | None
    ) -> _E:
        els = {el for el in self._enum if el.name.lower() == value.lower()}
        with suppress(ValueError):
            (el,) = els
            return el
        self.fail(f"Unable to parse {value}", param, ctx)


log_level_option = option(
    "--log-level",
    type=Enum(LogLevel),
    default=LogLevel.INFO,
    show_default=True,
    help="The logging level",
)
