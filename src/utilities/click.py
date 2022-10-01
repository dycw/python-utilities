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

from utilities.logging import LogLevel


class Date(ParamType):
    """A date-valued parameter."""

    name = "date"

    @beartype
    def convert(
        self, value: Any, param: Parameter | None, ctx: Context | None
    ) -> dt.date:
        with suppress(ValueError):
            return dt.date.fromisoformat(value)
        with suppress(ValueError):
            return dt.datetime.strptime(value, "%Y%m%d").date()
        self.fail(f"Unable to parse {value}", param, ctx)


class DateTime(ParamType):
    """A datetime-valued parameter."""

    name = "datetime"

    @beartype
    def convert(
        self, value: Any, param: Parameter | None, ctx: Context | None
    ) -> dt.date:
        with suppress(ValueError):
            return dt.datetime.fromisoformat(value)
        with suppress(ValueError):
            return dt.datetime.strptime(value, "%Y%m%d%H%M%S")
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
