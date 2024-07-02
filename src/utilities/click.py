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
from utilities.iterables import OneStrError, one_str
from utilities.logging import LogLevel
from utilities.sentinel import sentinel
from utilities.text import split_str

if TYPE_CHECKING:
    import datetime as dt
    from collections.abc import Sequence

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
        self, value: dt.date | str, param: Parameter | None, ctx: Context | None
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
        self, value: dt.datetime | str, param: Parameter | None, ctx: Context | None
    ) -> dt.date:
        """Convert a value into the `DateTime` type."""
        try:
            return ensure_datetime(value)
        except ParseDateTimeError:
            self.fail(f"Unable to parse {value}", param, ctx)


_E = TypeVar("_E", bound=enum.Enum)


class Enum(ParamType, Generic[_E]):
    """An enum-valued parameter."""

    name = "enum"

    def __init__(self, enum: type[_E], /, *, case_sensitive: bool = True) -> None:
        self._enum = enum
        self._case_sensitive = case_sensitive
        super().__init__()

    @override
    def __repr__(self) -> str:
        return f"Enum({self._enum})"

    @override
    def convert(
        self, value: _E | str, param: Parameter | None, ctx: Context | None
    ) -> _E:
        """Convert a value into the `Enum` type."""
        try:
            return ensure_enum(self._enum, value, case_sensitive=self._case_sensitive)
        except ParseEnumError:
            return self.fail(f"Unable to parse {value}", param, ctx)

    @override
    def get_metavar(self, param: Parameter) -> str | None:
        desc = "|".join(e.name for e in self._enum)
        req_arg = param.required and param.param_type_name == "argument"
        return f"{{{desc}}}" if req_arg else f"[{desc}]"


class ListChoices(ParamType):
    """A list-of-choices-valued parameter."""

    name = "choices"

    def __init__(
        self,
        choices: Sequence[str],
        /,
        *,
        separator: str = ",",
        empty: str = str(sentinel),
        case_sensitive: bool = True,
    ) -> None:
        self._choices = choices
        self._separator = separator
        self._empty = empty
        self._case_sensitive = case_sensitive
        super().__init__()

    @override
    def __repr__(self) -> str:
        return f"ListChoices({list(self._choices)})"

    @override
    def convert(
        self, value: list[str] | str, param: Parameter | None, ctx: Context | None
    ) -> list[str]:
        """Convert a value into the `ListChoices` type."""
        if isinstance(value, list):
            return self._convert_list_of_strs(value, param, ctx)
        texts = split_str(value, separator=self._separator, empty=self._empty)
        return self._convert_list_of_strs(texts, param, ctx)

    def _convert_list_of_strs(
        self, texts: list[str], param: Parameter | None, ctx: Context | None, /
    ) -> list[str]:
        results: list[str] = []
        errors: list[str] = []
        for text in texts:
            try:
                result = one_str(
                    self._choices, text, case_sensitive=self._case_sensitive
                )
            except OneStrError:
                errors.append(text)
            else:
                results.append(result)
        if len(errors) >= 1:
            return self.fail(
                f"{errors} must be a subset of {self._choices}", param, ctx
            )
        return results

    @override
    def get_metavar(self, param: Parameter) -> str | None:
        joined = "|".join(self._choices)
        desc = f"{joined}; sep={self._separator!r}"
        req_arg = param.required and param.param_type_name == "argument"
        return f"{{{desc}}}" if req_arg else f"[{desc}]"


class ListInts(ParamType):
    """A list-of-ints-valued parameter."""

    name = "ints"

    def __init__(self, *, separator: str = ",", empty: str = str(sentinel)) -> None:
        self._separator = separator
        self._empty = empty
        super().__init__()

    @override
    def convert(
        self, value: list[int] | str, param: Parameter | None, ctx: Context | None
    ) -> list[int]:
        """Convert a value into the `ListInts` type."""
        if isinstance(value, list):
            return value
        strs = split_str(value, separator=self._separator, empty=self._empty)
        try:
            return list(map(int, strs))
        except ValueError:
            return self.fail(f"Unable to parse {value}", param, ctx)

    @override
    def get_metavar(self, param: Parameter) -> str | None:
        desc = f"INTS; sep={self._separator!r}"
        req_arg = param.required and param.param_type_name == "argument"
        return f"{{{desc}}}" if req_arg else f"[{desc}]"


class Time(ParamType):
    """A time-valued parameter."""

    name = "time"

    @override
    def convert(
        self, value: dt.time | str, param: Parameter | None, ctx: Context | None
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
        self, value: dt.timedelta | str, param: Parameter | None, ctx: Context | None
    ) -> dt.timedelta:
        """Convert a value into the `Timedelta` type."""
        try:
            return ensure_timedelta(value)
        except ParseTimedeltaError:
            self.fail(f"Unable to parse {value}", param, ctx)


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
    "ListChoices",
    "ListInts",
    "Time",
    "Timedelta",
    "local_scheduler_option_default_central",
    "local_scheduler_option_default_local",
    "log_level_option",
    "workers_option",
]
