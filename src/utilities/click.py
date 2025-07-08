from __future__ import annotations

import enum
import ipaddress
import pathlib
from typing import TYPE_CHECKING, TypedDict, assert_never, override

import click
import whenever
from click import Choice, Context, Parameter, ParamType
from click.types import IntParamType, StringParamType

from utilities.enum import EnsureEnumError, ensure_enum
from utilities.functions import EnsureStrError, ensure_str, get_class_name
from utilities.iterables import is_iterable_not_str
from utilities.parse import ParseObjectError, parse_object
from utilities.text import split_str

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from utilities.types import (
        DateDeltaLike,
        DateLike,
        DateTimeDeltaLike,
        EnumLike,
        IPv4AddressLike,
        IPv6AddressLike,
        MaybeStr,
        MonthDayLike,
        PlainDateTimeLike,
        TimeDeltaLike,
        TimeLike,
        YearMonthLike,
        ZonedDateTimeLike,
    )


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


class Enum[E: enum.Enum](ParamType):
    """An enum-valued parameter."""

    @override
    def __init__(self, enum: type[E], /, *, case_sensitive: bool = False) -> None:
        cls = get_class_name(enum)
        self.name = f"enum[{cls}]"
        self._enum = enum
        self._case_sensitive = case_sensitive
        super().__init__()

    @override
    def __repr__(self) -> str:
        cls = get_class_name(self._enum)
        return f"ENUM[{cls}]"

    @override
    def convert(
        self, value: EnumLike[E], param: Parameter | None, ctx: Context | None
    ) -> E:
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


class IPv4Address(ParamType):
    """An IPv4 address-valued parameter."""

    name = "ipv4 address"

    @override
    def __repr__(self) -> str:
        return self.name.upper()

    @override
    def convert(
        self, value: IPv4AddressLike, param: Parameter | None, ctx: Context | None
    ) -> ipaddress.IPv4Address:
        """Convert a value into the `IPv4Address` type."""
        match value:
            case ipaddress.IPv4Address():
                return value
            case str():
                try:
                    return parse_object(ipaddress.IPv4Address, value)
                except ParseObjectError as error:
                    self.fail(str(error), param, ctx)
            case _ as never:
                assert_never(never)


class IPv6Address(ParamType):
    """An IPv6 address-valued parameter."""

    name = "ipv6 address"

    @override
    def __repr__(self) -> str:
        return self.name.upper()

    @override
    def convert(
        self, value: IPv6AddressLike, param: Parameter | None, ctx: Context | None
    ) -> ipaddress.IPv6Address:
        """Convert a value into the `IPv6Address` type."""
        match value:
            case ipaddress.IPv6Address():
                return value
            case str():
                try:
                    return parse_object(ipaddress.IPv6Address, value)
                except ParseObjectError as error:
                    self.fail(str(error), param, ctx)
            case _ as never:
                assert_never(never)


class MonthDay(ParamType):
    """A month-day parameter."""

    name = "month-day"

    @override
    def __repr__(self) -> str:
        return self.name.upper()

    @override
    def convert(
        self, value: MonthDayLike, param: Parameter | None, ctx: Context | None
    ) -> whenever.MonthDay:
        """Convert a value into the `MonthDay` type."""
        match value:
            case whenever.MonthDay():
                return value
            case str():
                try:
                    return whenever.MonthDay.parse_common_iso(value)
                except ValueError as error:
                    self.fail(str(error), param, ctx)
            case _ as never:
                assert_never(never)


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


class YearMonth(ParamType):
    """A year-month parameter."""

    name = "year-month"

    @override
    def __repr__(self) -> str:
        return self.name.upper()

    @override
    def convert(
        self, value: YearMonthLike, param: Parameter | None, ctx: Context | None
    ) -> whenever.YearMonth:
        """Convert a value into the `YearMonth` type."""
        match value:
            case whenever.YearMonth():
                return value
            case str():
                try:
                    return whenever.YearMonth.parse_common_iso(value)
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


class FrozenSetParameter[P: ParamType, T](ParamType):
    """A frozenset-valued parameter."""

    @override
    def __init__(self, param: P, /, *, separator: str = ",") -> None:
        self.name = f"frozenset[{param.name}]"
        self._param = param
        self._separator = separator
        super().__init__()

    @override
    def __repr__(self) -> str:
        return f"FROZENSET[{self._param!r}]"

    @override
    def convert(
        self, value: MaybeStr[Iterable[T]], param: Parameter | None, ctx: Context | None
    ) -> frozenset[T]:
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

    @override
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


class FrozenSetEnums[E: enum.Enum](FrozenSetParameter[Enum[E], E]):
    """A frozenset-of-enums-valued parameter."""

    @override
    def __init__(
        self, enum: type[E], /, *, case_sensitive: bool = False, separator: str = ","
    ) -> None:
        super().__init__(Enum(enum, case_sensitive=case_sensitive), separator=separator)


class FrozenSetInts(FrozenSetParameter[IntParamType, int]):
    """A frozenset-of-ints-valued parameter."""

    @override
    def __init__(self, *, separator: str = ",") -> None:
        super().__init__(IntParamType(), separator=separator)


class FrozenSetStrs(FrozenSetParameter[StringParamType, str]):
    """A frozenset-of-strs-valued parameter."""

    @override
    def __init__(self, *, separator: str = ",") -> None:
        super().__init__(StringParamType(), separator=separator)


# parameters - list


class ListParameter[P: ParamType, T](ParamType):
    """A list-valued parameter."""

    @override
    def __init__(self, param: P, /, *, separator: str = ",") -> None:
        self.name = f"list[{param.name}]"
        self._param = param
        self._separator = separator
        super().__init__()

    @override
    def __repr__(self) -> str:
        return f"LIST[{self._param!r}]"

    @override
    def convert(
        self, value: MaybeStr[Iterable[T]], param: Parameter | None, ctx: Context | None
    ) -> list[T]:
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


class ListChoices(ListParameter[Choice, str]):
    """A frozenset-of-choices-valued parameter."""

    @override
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


class ListEnums[E: enum.Enum](ListParameter[Enum[E], E]):
    """A list-of-enums-valued parameter."""

    @override
    def __init__(
        self, enum: type[E], /, *, case_sensitive: bool = False, separator: str = ","
    ) -> None:
        super().__init__(Enum(enum, case_sensitive=case_sensitive), separator=separator)


class ListInts(ListParameter[IntParamType, int]):
    """A list-of-ints-valued parameter."""

    @override
    def __init__(self, *, separator: str = ",") -> None:
        super().__init__(IntParamType(), separator=separator)


class ListStrs(ListParameter[StringParamType, str]):
    """A list-of-strs-valued parameter."""

    @override
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
    "IPv4Address",
    "IPv6Address",
    "ListChoices",
    "ListEnums",
    "ListInts",
    "ListParameter",
    "ListStrs",
    "MonthDay",
    "PlainDateTime",
    "Time",
    "TimeDelta",
    "YearMonth",
    "ZonedDateTime",
]
