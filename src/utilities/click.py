from __future__ import annotations

import enum
import pathlib
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import click
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

if TYPE_CHECKING:
    import datetime as dt

    from sqlalchemy import Engine as _Engine


FilePath = click.Path(file_okay=True, dir_okay=False, path_type=pathlib.Path)
DirPath = click.Path(file_okay=False, dir_okay=True, path_type=pathlib.Path)
ExistingFilePath = click.Path(
    exists=True, file_okay=True, dir_okay=False, path_type=pathlib.Path
)
ExistingDirPath = click.Path(
    exists=True, file_okay=False, dir_okay=True, path_type=pathlib.Path
)


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


_E = TypeVar("_E", bound=enum.Enum)


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


# luigi


(local_scheduler_option_default_local, local_scheduler_option_default_central) = (
    option(
        "-ls/-nls",
        "--local-scheduler/--no-local-scheduler",
        is_flag=True,
        default=default,
        show_default=True,
        help=f"Pass {flag!r} to use the {desc} scheduler",
    )
    for default, flag, desc in [(True, "-nls", "central"), (False, "-ls", "local")]
)
workers_option = option(
    "-w",
    "--workers",
    type=int,
    default=None,
    show_default=True,
    help="The number of workers to use",
)


# sqlalchemy


class Engine(ParamType):
    """An engine-valued parameter."""

    name = "engine"

    @override
    def convert(
        self, value: Any, param: Parameter | None, ctx: Context | None
    ) -> _Engine:
        """Convert a value into the `Engine` type."""
        from utilities.sqlalchemy import ParseEngineError, ensure_engine

        try:
            return ensure_engine(value)
        except ParseEngineError:
            self.fail(f"Unable to parse {value}", param, ctx)


__all__ = [
    "Date",
    "DateTime",
    "DirPath",
    "Engine",
    "Enum",
    "ExistingDirPath",
    "ExistingFilePath",
    "FilePath",
    "Time",
    "Timedelta",
    "local_scheduler_option_default_central",
    "local_scheduler_option_default_local",
    "log_level_option",
    "workers_option",
]
