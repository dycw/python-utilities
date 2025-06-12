from __future__ import annotations

import pathlib
from typing import TYPE_CHECKING, Generic, TypedDict, TypeVar, assert_never, override

import click
import whenever
from click import Choice, Context, Parameter, ParamType
from click.types import StringParamType

from utilities.datetime import EnsureMonthError, ensure_month
from utilities.enum import EnsureEnumError, ensure_enum
from utilities.functions import EnsureStrError, ensure_str, get_class_name
from utilities.iterables import is_iterable_not_str
from utilities.text import split_str
from utilities.types import (
    DateDeltaLike,
    DateLike,
    DateTimeDeltaLike,
    EnumLike,
    MaybeStr,
    PlainDateTimeLike,
    TEnum,
    TimeDeltaLike,
    TimeLike,
    ZonedDateTimeLike,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    import utilities.datetime
    from utilities.datetime import MonthLike


_T = TypeVar("_T")
_TParam = TypeVar("_TParam", bound=ParamType)


FilePath = click.Path(file_okay=True, dir_okay=False, path_type=pathlib.Path)
DirPath = click.Path(file_okay=False, dir_okay=True, path_type=pathlib.Path)
ExistingFilePath = click.Path(
    exists=True, file_okay=True, dir_okay=False, path_type=pathlib.Path
)
ExistingDirPath = click.Path(
    exists=True, file_okay=False, dir_okay=True, path_type=pathlib.Path
)


class _HelpOptionNames(TypedDict):
    help_option_names: Sequence[str]


class _ContextSettings(TypedDict):
    context_settings: _HelpOptionNames


CONTEXT_SETTINGS_HELP_OPTION_NAMES = _ContextSettings(
    context_settings=_HelpOptionNames(help_option_names=["-h", "--help"])
)


# parameters


class Date(ParamType):
    """A date-valued parameter."""

    name = "date"

    @override
    def __repr__(self) -> str:
        return self.name.upper()

    @override
    def convert(
        self, value: DateLike, param: Parameter | None, ctx: Context | None
    ) -> whenever.Date:
        """Convert a value into the `Date` type."""
        match value:
            case whenever.Date():
                return value
            case str():
                try:
                    return whenever.Date.parse_common_iso(value)
                except ValueError as error:
                    self.fail(str(error), param, ctx)
            case _ as never:
                assert_never(never)


class DateDelta(ParamType):
    """A date-delta-valued parameter."""

    name = "date delta"

    @override
    def __repr__(self) -> str:
        return self.name.upper()

    @override
    def convert(
        self, value: DateDeltaLike, param: Parameter | None, ctx: Context | None
    ) -> whenever.DateDelta:
        """Convert a value into the `DateDelta` type."""
        match value:
            case whenever.DateDelta():
                return value
            case str():
                try:
                    return whenever.DateDelta.parse_common_iso(value)
                except ValueError as error:
                    self.fail(str(error), param, ctx)
            case _ as never:
                assert_never(never)


class DateTimeDelta(ParamType):
    """A date-delta-valued parameter."""

    name = "date-time delta"

    @override
    def __repr__(self) -> str:
        return self.name.upper()

    @override
    def convert(
        self, value: DateTimeDeltaLike, param: Parameter | None, ctx: Context | None
    ) -> whenever.DateTimeDelta:
        """Convert a value into the `DateTimeDelta` type."""
        match value:
            case whenever.DateTimeDelta():
                return value
            case str():
                try:
                    return whenever.DateTimeDelta.parse_common_iso(value)
                except ValueError as error:
                    self.fail(str(error), param, ctx)
            case _ as never:
                assert_never(never)


class Enum(ParamType, Generic[TEnum]):
    """An enum-valued parameter."""

    def __init__(self, enum: type[TEnum], /, *, case_sensitive: bool = False) -> None:
        cls = get_class_name(enum)
        self.name = f"ENUM[{cls}]"
        self._enum = enum
        self._case_sensitive = case_sensitive
        super().__init__()

    @override
    def __repr__(self) -> str:
        cls = get_class_name(self._enum)
        return f"ENUM[{cls}]"

    @override
    def convert(
        self, value: EnumLike[TEnum], param: Parameter | None, ctx: Context | None
    ) -> TEnum:
        """Convert a value into the `Enum` type."""
        try:
            return ensure_enum(value, self._enum, case_sensitive=self._case_sensitive)
        except EnsureEnumError as error:
            self.fail(str(error), param, ctx)

    @override
    def get_metavar(self, param: Parameter, ctx: Context) -> str | None:
        _ = ctx
        desc = ",".join(e.name for e in self._enum)
        return _make_metavar(param, desc)


class Month(ParamType):
    """A month-valued parameter."""

    name = "month"

    @override
    def __repr__(self) -> str:
        return self.name.upper()

    @override
    def convert(
        self, value: MonthLike, param: Parameter | None, ctx: Context | None
    ) -> utilities.datetime.Month:
        """Convert a value into the `Month` type."""
        try:
            return ensure_month(value)
        except EnsureMonthError as error:
            self.fail(str(error), param, ctx)


class PlainDateTime(ParamType):
    """A local-datetime-valued parameter."""

    name = "plain date-time"

    @override
    def __repr__(self) -> str:
        return self.name.upper()

    @override
    def convert(
        self, value: PlainDateTimeLike, param: Parameter | None, ctx: Context | None
    ) -> whenever.PlainDateTime:
        """Convert a value into the `PlainDateTime` type."""
        match value:
            case whenever.PlainDateTime():
                return value
            case str():
                try:
                    return whenever.PlainDateTime.parse_common_iso(value)
                except ValueError as error:
                    self.fail(str(error), param, ctx)
            case _ as never:
                assert_never(never)


class Time(ParamType):
    """A time-valued parameter."""

    name = "time"

    @override
    def __repr__(self) -> str:
        return self.name.upper()

    @override
    def convert(
        self, value: TimeLike, param: Parameter | None, ctx: Context | None
    ) -> whenever.Time:
        """Convert a value into the `Time` type."""
        match value:
            case whenever.Time():
                return value
            case str():
                try:
                    return whenever.Time.parse_common_iso(value)
                except ValueError as error:
                    self.fail(str(error), param, ctx)
            case _ as never:
                assert_never(never)


class TimeDelta(ParamType):
    """A timedelta-valued parameter."""

    name = "time-delta"

    @override
    def __repr__(self) -> str:
        return self.name.upper()

    @override
    def convert(
        self, value: TimeDeltaLike, param: Parameter | None, ctx: Context | None
    ) -> whenever.TimeDelta:
        """Convert a value into the `TimeDelta` type."""
        match value:
            case whenever.TimeDelta():
                return value
            case str():
                try:
                    return whenever.TimeDelta.parse_common_iso(value)
                except ValueError as error:
                    self.fail(str(error), param, ctx)
            case _ as never:
                assert_never(never)


class ZonedDateTime(ParamType):
    """A zoned-datetime-valued parameter."""

    name = "zoned date-time"

    @override
    def __repr__(self) -> str:
        return self.name.upper()

    @override
    def convert(
        self, value: ZonedDateTimeLike, param: Parameter | None, ctx: Context | None
    ) -> whenever.ZonedDateTime:
        """Convert a value into the `ZonedDateTime` type."""
        match value:
            case whenever.ZonedDateTime():
                return value
            case str():
                try:
                    return whenever.ZonedDateTime.parse_common_iso(value)
                except ValueError as error:
                    self.fail(str(error), param, ctx)
            case _ as never:
                assert_never(never)


# parameters - frozenset


class FrozenSetParameter(ParamType, Generic[_TParam, _T]):
    """A frozenset-valued parameter."""

    def __init__(self, param: _TParam, /, *, separator: str = ",") -> None:
        self.name = f"FROZENSET[{param.name}]"
        self._param = param
        self._separator = separator
        super().__init__()

    @override
    def __repr__(self) -> str:
        desc = repr(self._param)
        return f"FROZENSET[{desc}]"

    @override
    def convert(
        self,
        value: MaybeStr[Iterable[_T]],
        param: Parameter | None,
        ctx: Context | None,
    ) -> frozenset[_T]:
        """Convert a value into the `ListDates` type."""
        if is_iterable_not_str(value):
            return frozenset(value)
        try:
            text = ensure_str(value)
        except EnsureStrError as error:
            return self.fail(str(error), param=param, ctx=ctx)
        values = split_str(text, separator=self._separator)
        return frozenset(self._param.convert(v, param, ctx) for v in values)

    @override
    def get_metavar(self, param: Parameter, ctx: Context) -> str | None:
        if (metavar := self._param.get_metavar(param, ctx)) is None:
            name = self.name.upper()
        else:
            name = f"FROZENSET{metavar}"
        sep = f"SEP={self._separator}"
        desc = f"{name} {sep}"
        return _make_metavar(param, desc)


class FrozenSetChoices(FrozenSetParameter[Choice, str]):
    """A frozenset-of-choices-valued parameter."""

    def __init__(
        self,
        choices: Sequence[str],
        /,
        *,
        case_sensitive: bool = False,
        separator: str = ",",
    ) -> None:
        super().__init__(
            Choice(choices, case_sensitive=case_sensitive), separator=separator
        )


class FrozenSetEnums(FrozenSetParameter[Enum[TEnum], TEnum]):
    """A frozenset-of-enums-valued parameter."""

    def __init__(
        self,
        enum: type[TEnum],
        /,
        *,
        case_sensitive: bool = False,
        separator: str = ",",
    ) -> None:
        super().__init__(Enum(enum, case_sensitive=case_sensitive), separator=separator)


class FrozenSetStrs(FrozenSetParameter[StringParamType, str]):
    """A frozenset-of-strs-valued parameter."""

    def __init__(self, *, separator: str = ",") -> None:
        super().__init__(StringParamType(), separator=separator)


# parameters - list


class ListParameter(ParamType, Generic[_TParam, _T]):
    """A list-valued parameter."""

    def __init__(self, param: _TParam, /, *, separator: str = ",") -> None:
        self.name = f"LIST[{param.name}]"
        self._param = param
        self._separator = separator
        super().__init__()

    @override
    def __repr__(self) -> str:
        desc = repr(self._param)
        return f"LIST[{desc}]"

    @override
    def convert(
        self,
        value: MaybeStr[Iterable[_T]],
        param: Parameter | None,
        ctx: Context | None,
    ) -> list[_T]:
        """Convert a value into the `List` type."""
        if is_iterable_not_str(value):
            return list(value)
        try:
            text = ensure_str(value)
        except EnsureStrError as error:
            return self.fail(str(error), param=param, ctx=ctx)
        values = split_str(text, separator=self._separator)
        return [self._param.convert(v, param, ctx) for v in values]

    @override
    def get_metavar(self, param: Parameter, ctx: Context) -> str | None:
        if (metavar := self._param.get_metavar(param, ctx)) is None:
            name = self.name.upper()
        else:
            name = f"LIST{metavar}"
        sep = f"SEP={self._separator}"
        desc = f"{name} {sep}"
        return _make_metavar(param, desc)


class ListEnums(ListParameter[Enum[TEnum], TEnum]):
    """A list-of-enums-valued parameter."""

    def __init__(
        self,
        enum: type[TEnum],
        /,
        *,
        case_sensitive: bool = False,
        separator: str = ",",
    ) -> None:
        super().__init__(Enum(enum, case_sensitive=case_sensitive), separator=separator)


class ListStrs(ListParameter[StringParamType, str]):
    """A list-of-strs-valued parameter."""

    def __init__(self, *, separator: str = ",") -> None:
        super().__init__(StringParamType(), separator=separator)


# private


def _make_metavar(param: Parameter, desc: str, /) -> str:
    req_arg = param.required and param.param_type_name == "argument"
    return f"{{{desc}}}" if req_arg else f"[{desc}]"


__all__ = [
    "CONTEXT_SETTINGS_HELP_OPTION_NAMES",
    "Date",
    "DateDelta",
    "DateTimeDelta",
    "DirPath",
    "Enum",
    "ExistingDirPath",
    "ExistingFilePath",
    "FilePath",
    "FrozenSetChoices",
    "FrozenSetEnums",
    "FrozenSetParameter",
    "FrozenSetStrs",
    "ListEnums",
    "ListParameter",
    "ListStrs",
    "PlainDateTime",
    "Time",
    "TimeDelta",
    "ZonedDateTime",
]
