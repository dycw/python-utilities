from __future__ import annotations

import datetime as dt
import pathlib
from typing import TYPE_CHECKING, Generic, TypeVar, assert_never, override
from uuid import UUID

import click
from click import Choice, Context, Parameter, ParamType
from click.types import (
    BoolParamType,
    FloatParamType,
    IntParamType,
    StringParamType,
    UUIDParameterType,
)

import utilities.datetime
import utilities.types
from utilities.datetime import EnsureMonthError, ensure_month
from utilities.enum import ParseEnumError, parse_enum
from utilities.functions import ensure_str, get_class_name
from utilities.iterables import is_iterable_not_str
from utilities.sentinel import SENTINEL_REPR
from utilities.text import split_str
from utilities.types import EnumOrStr, MaybeStr, TEnum

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

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
    ) -> dt.date:
        """Convert a value into the `Date` type."""
        match value:
            case dt.date() as date:
                return date
            case str() as text:
                from utilities.whenever import ParseDateError, parse_date

                try:
                    return parse_date(text)
                except ParseDateError as error:
                    return self.fail(str(error), param=param, ctx=ctx)
            case _ as never:
                assert_never(never)


class Duration(ParamType):
    """A duration-valued parameter."""

    name = "duration"

    @override
    def __repr__(self) -> str:
        return self.name.upper()

    @override
    def convert(
        self,
        value: MaybeStr[utilities.types.Duration],
        param: Parameter | None,
        ctx: Context | None,
    ) -> utilities.types.Duration:
        """Convert a value into the `Duration` type."""
        match value:
            case int() | float() | dt.timedelta() as duration:
                return duration
            case str() as text:
                from utilities.whenever import ParseDurationError, parse_duration

                try:
                    return parse_duration(text)
                except ParseDurationError as error:
                    return self.fail(str(error), param=param, ctx=ctx)
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
        if isinstance(value, self._enum):
            return value
        text = ensure_str(value)
        try:
            return parse_enum(text, self._enum, case_sensitive=self._case_sensitive)
        except ParseEnumError as error:
            return self.fail(str(error), param=param, ctx=ctx)

    @override
    def get_metavar(self, param: Parameter) -> str | None:
        desc = ",".join(e.name for e in self._enum)
        return _make_metavar(param, desc)


class LocalDateTime(ParamType):
    """A local-datetime-valued parameter."""

    name = "local datetime"

    @override
    def __repr__(self) -> str:
        return self.name.upper()

    @override
    def convert(
        self, value: DateTimeLike, param: Parameter | None, ctx: Context | None
    ) -> dt.date:
        """Convert a value into the `LocalDateTime` type."""
        match value:
            case dt.datetime() as datetime:
                return datetime
            case str() as text:
                from utilities.whenever import (
                    ParseLocalDateTimeError,
                    parse_local_datetime,
                )

                try:
                    return parse_local_datetime(text)
                except ParseLocalDateTimeError as error:
                    return self.fail(str(error), param=param, ctx=ctx)
            case _ as never:
                assert_never(never)


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


class Time(ParamType):
    """A time-valued parameter."""

    name = "time"

    @override
    def __repr__(self) -> str:
        return self.name.upper()

    @override
    def convert(
        self, value: TimeLike, param: Parameter | None, ctx: Context | None
    ) -> dt.time:
        """Convert a value into the `Time` type."""
        from utilities.whenever import EnsureTimeError, ensure_time

        try:
            return ensure_time(value)
        except EnsureTimeError as error:
            return self.fail(str(error), param=param, ctx=ctx)


class Timedelta(ParamType):
    """A timedelta-valued parameter."""

    name = "timedelta"

    @override
    def __repr__(self) -> str:
        return self.name.upper()

    @override
    def convert(
        self, value: TimeDeltaLike, param: Parameter | None, ctx: Context | None
    ) -> dt.timedelta:
        """Convert a value into the `Timedelta` type."""
        match value:
            case dt.timedelta() as timedelta:
                return timedelta
            case str() as text:
                from utilities.whenever import ParseTimedeltaError, parse_timedelta

                try:
                    return parse_timedelta(text)
                except ParseTimedeltaError as error:
                    return self.fail(str(error), param=param, ctx=ctx)
            case _ as never:
                assert_never(never)


class ZonedDateTime(ParamType):
    """A zoned-datetime-valued parameter."""

    name = "zoned datetime"

    @override
    def __repr__(self) -> str:
        return self.name.upper()

    @override
    def convert(
        self, value: DateTimeLike, param: Parameter | None, ctx: Context | None
    ) -> dt.date:
        """Convert a value into the `DateTime` type."""
        from utilities.whenever import EnsureZonedDateTimeError, ensure_zoned_datetime

        try:
            return ensure_zoned_datetime(value)
        except EnsureZonedDateTimeError as error:
            self.fail(str(error), param, ctx)


# parameters - frozenset


class FrozenSetParameter(ParamType, Generic[_TParam, _T]):
    """A frozenset-valued parameter."""

    def __init__(
        self, param: _TParam, /, *, separator: str = ",", empty: str = SENTINEL_REPR
    ) -> None:
        self.name = f"FROZENSET[{param.name}]"
        self._param = param
        self._separator = separator
        self._empty = empty
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
        values = split_str(text, separator=self._separator, empty=self._empty)
        return frozenset(self._param.convert(v, param, ctx) for v in values)

    @override
    def get_metavar(self, param: Parameter) -> str | None:
        if (metavar := self._param.get_metavar(param)) is None:
            name = self.name.upper()
        else:
            name = f"FROZENSET{metavar}"
        sep = f"SEP={self._separator}"
        desc = f"{name} {sep}"
        return _make_metavar(param, desc)


class FrozenSetBools(FrozenSetParameter[BoolParamType, str]):
    """A frozenset-of-bools-valued parameter."""

    def __init__(self, *, separator: str = ",", empty: str = SENTINEL_REPR) -> None:
        super().__init__(BoolParamType(), separator=separator, empty=empty)


class FrozenSetDates(FrozenSetParameter[Date, dt.date]):
    """A frozenset-of-dates-valued parameter."""

    def __init__(self, *, separator: str = ",", empty: str = SENTINEL_REPR) -> None:
        super().__init__(Date(), separator=separator, empty=empty)


class FrozenSetChoices(FrozenSetParameter[Choice, str]):
    """A frozenset-of-choices-valued parameter."""

    def __init__(
        self,
        choices: Sequence[str],
        /,
        *,
        case_sensitive: bool = False,
        separator: str = ",",
        empty: str = SENTINEL_REPR,
    ) -> None:
        super().__init__(
            Choice(choices, case_sensitive=case_sensitive),
            separator=separator,
            empty=empty,
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
        empty: str = SENTINEL_REPR,
    ) -> None:
        super().__init__(
            Enum(enum, case_sensitive=case_sensitive), separator=separator, empty=empty
        )


class FrozenSetFloats(FrozenSetParameter[FloatParamType, float]):
    """A frozenset-of-floats-valued parameter."""

    def __init__(self, *, separator: str = ",", empty: str = SENTINEL_REPR) -> None:
        super().__init__(FloatParamType(), separator=separator, empty=empty)


class FrozenSetInts(FrozenSetParameter[IntParamType, int]):
    """A frozenset-of-ints-valued parameter."""

    def __init__(self, *, separator: str = ",", empty: str = SENTINEL_REPR) -> None:
        super().__init__(IntParamType(), separator=separator, empty=empty)


class FrozenSetMonths(FrozenSetParameter[Month, utilities.datetime.Month]):
    """A frozenset-of-months-valued parameter."""

    def __init__(self, *, separator: str = ",", empty: str = SENTINEL_REPR) -> None:
        super().__init__(Month(), separator=separator, empty=empty)


class FrozenSetStrs(FrozenSetParameter[StringParamType, str]):
    """A frozenset-of-strs-valued parameter."""

    def __init__(self, *, separator: str = ",", empty: str = SENTINEL_REPR) -> None:
        super().__init__(StringParamType(), separator=separator, empty=empty)


class FrozenSetUUIDs(FrozenSetParameter[UUIDParameterType, UUID]):
    """A frozenset-of-UUIDs-valued parameter."""

    def __init__(self, *, separator: str = ",", empty: str = SENTINEL_REPR) -> None:
        super().__init__(UUIDParameterType(), separator=separator, empty=empty)


# parameters - list


class ListParameter(ParamType, Generic[_TParam, _T]):
    """A list-valued parameter."""

    def __init__(
        self, param: _TParam, /, *, separator: str = ",", empty: str = SENTINEL_REPR
    ) -> None:
        self.name = f"LIST[{param.name}]"
        self._param = param
        self._separator = separator
        self._empty = empty
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
        text = ensure_str(value)
        values = split_str(text, separator=self._separator, empty=self._empty)
        return [self._param.convert(v, param, ctx) for v in values]

    @override
    def get_metavar(self, param: Parameter) -> str | None:
        if (metavar := self._param.get_metavar(param)) is None:
            name = self.name.upper()
        else:
            name = f"LIST{metavar}"
        sep = f"SEP={self._separator}"
        desc = f"{name} {sep}"
        return _make_metavar(param, desc)


class ListBools(ListParameter[BoolParamType, str]):
    """A list-of-bools-valued parameter."""

    def __init__(self, *, separator: str = ",", empty: str = SENTINEL_REPR) -> None:
        super().__init__(BoolParamType(), separator=separator, empty=empty)


class ListDates(ListParameter[Date, dt.date]):
    """A list-of-dates-valued parameter."""

    def __init__(self, *, separator: str = ",", empty: str = SENTINEL_REPR) -> None:
        super().__init__(Date(), separator=separator, empty=empty)


class ListEnums(ListParameter[Enum[TEnum], TEnum]):
    """A list-of-enums-valued parameter."""

    def __init__(
        self,
        enum: type[TEnum],
        /,
        *,
        case_sensitive: bool = False,
        separator: str = ",",
        empty: str = SENTINEL_REPR,
    ) -> None:
        super().__init__(
            Enum(enum, case_sensitive=case_sensitive), separator=separator, empty=empty
        )


class ListFloats(ListParameter[FloatParamType, float]):
    """A list-of-floats-valued parameter."""

    def __init__(self, *, separator: str = ",", empty: str = SENTINEL_REPR) -> None:
        super().__init__(FloatParamType(), separator=separator, empty=empty)


class ListInts(ListParameter[IntParamType, int]):
    """A list-of-ints-valued parameter."""

    def __init__(self, *, separator: str = ",", empty: str = SENTINEL_REPR) -> None:
        super().__init__(IntParamType(), separator=separator, empty=empty)


class ListMonths(ListParameter[Month, utilities.datetime.Month]):
    """A list-of-months-valued parameter."""

    def __init__(self, *, separator: str = ",", empty: str = SENTINEL_REPR) -> None:
        super().__init__(Month(), separator=separator, empty=empty)


class ListStrs(ListParameter[StringParamType, str]):
    """A list-of-strs-valued parameter."""

    def __init__(self, *, separator: str = ",", empty: str = SENTINEL_REPR) -> None:
        super().__init__(StringParamType(), separator=separator, empty=empty)


class ListUUIDs(ListParameter[UUIDParameterType, UUID]):
    """A list-of-UUIDs-valued parameter."""

    def __init__(self, *, separator: str = ",", empty: str = SENTINEL_REPR) -> None:
        super().__init__(UUIDParameterType(), separator=separator, empty=empty)


# private


def _make_metavar(param: Parameter, desc: str, /) -> str:
    req_arg = param.required and param.param_type_name == "argument"
    return f"{{{desc}}}" if req_arg else f"[{desc}]"


__all__ = [
    "Date",
    "DirPath",
    "Duration",
    "Enum",
    "ExistingDirPath",
    "ExistingFilePath",
    "FilePath",
    "FrozenSetBools",
    "FrozenSetChoices",
    "FrozenSetDates",
    "FrozenSetEnums",
    "FrozenSetFloats",
    "FrozenSetInts",
    "FrozenSetMonths",
    "FrozenSetParameter",
    "FrozenSetStrs",
    "FrozenSetUUIDs",
    "ListBools",
    "ListDates",
    "ListEnums",
    "ListFloats",
    "ListInts",
    "ListMonths",
    "ListParameter",
    "ListStrs",
    "ListUUIDs",
    "LocalDateTime",
    "Month",
    "Time",
    "Timedelta",
    "ZonedDateTime",
]
