from __future__ import annotations

import enum
import ipaddress
import pathlib
import uuid
from collections.abc import Iterable
from enum import StrEnum
from typing import TYPE_CHECKING, Literal, TypedDict, assert_never, cast, override

import whenever
from click import Choice, Context, Parameter, ParamType
from click.types import IntParamType, StringParamType

from utilities.core import get_class_name
from utilities.enum import ParseEnumError, parse_enum
from utilities.text import ParseBoolError, parse_bool, split_str

if TYPE_CHECKING:
    import pydantic

    from utilities.types import (
        DateDeltaLike,
        DateLike,
        DateTimeDeltaLike,
        EnumLike,
        IPv4AddressLike,
        IPv6AddressLike,
        MaybeStr,
        MonthDayLike,
        PathLike,
        PlainDateTimeLike,
        SecretLike,
        TimeDeltaLike,
        TimeLike,
        YearMonthLike,
        ZonedDateTimeLike,
    )


class _ContextSettings(TypedDict):
    context_settings: _ContextSettingsInner


class _ContextSettingsInner(TypedDict):
    max_content_width: int
    help_option_names: list[str]
    show_default: bool


_MAX_CONTENT_WIDTH = 120
_CONTEXT_SETTINGS_INNER = _ContextSettingsInner(
    max_content_width=_MAX_CONTENT_WIDTH,
    help_option_names=["-h", "--help"],
    show_default=True,
)
CONTEXT_SETTINGS = _ContextSettings(context_settings=_CONTEXT_SETTINGS_INNER)


# parameters


class Bool(ParamType):
    """A boolean-valued parameter."""

    name = "bool"

    @override
    def __repr__(self) -> str:
        return self.name.upper()

    @override
    def convert(
        self, value: str, param: Parameter | None, ctx: Context | None
    ) -> bool | None:
        """Convert a value into a Boolean, or None."""
        match value:
            case bool():
                return value
            case "":
                return None
            case str():
                try:
                    return parse_bool(value)
                except ParseBoolError as error:
                    self.fail(str(error), param, ctx)
            case never:
                assert_never(never)


##


class Date(ParamType):
    """A date-valued parameter."""

    name = "date"

    @override
    def __repr__(self) -> str:
        return self.name.upper()

    @override
    def convert(
        self, value: DateLike, param: Parameter | None, ctx: Context | None
    ) -> whenever.Date | None:
        """Convert a value into a Date, or None."""
        match value:
            case whenever.Date():
                return value
            case "":
                return None
            case str():
                try:
                    return whenever.Date.parse_iso(value)
                except ValueError as error:
                    self.fail(str(error), param, ctx)
            case never:
                assert_never(never)


##


class DateDelta(ParamType):
    """A date-delta-valued parameter."""

    name = "date delta"

    @override
    def __repr__(self) -> str:
        return self.name.upper()

    @override
    def convert(
        self, value: DateDeltaLike, param: Parameter | None, ctx: Context | None
    ) -> whenever.DateDelta | None:
        """Convert a value into a DateDelta, or None."""
        match value:
            case whenever.DateDelta():
                return value
            case "":
                return None
            case str():
                try:
                    return whenever.DateDelta.parse_iso(value)
                except ValueError as error:
                    self.fail(str(error), param, ctx)
            case never:
                assert_never(never)


##


class DateTimeDelta(ParamType):
    """A date-delta-valued parameter."""

    name = "date-time delta"

    @override
    def __repr__(self) -> str:
        return self.name.upper()

    @override
    def convert(
        self, value: DateTimeDeltaLike, param: Parameter | None, ctx: Context | None
    ) -> whenever.DateTimeDelta | None:
        """Convert a value into a DateTimeDelta, or None."""
        match value:
            case whenever.DateTimeDelta():
                return value
            case "":
                return None
            case str():
                try:
                    return whenever.DateTimeDelta.parse_iso(value)
                except ValueError as error:
                    self.fail(str(error), param, ctx)
            case never:
                assert_never(never)


##


class Enum[E: enum.Enum](ParamType):
    """An enum-valued parameter."""

    @override
    def __init__(
        self, enum: type[E], /, *, value: bool = False, case_sensitive: bool = False
    ) -> None:
        cls = get_class_name(enum)
        self.name = f"enum[{cls}]"
        self._enum = enum
        self._value = issubclass(self._enum, StrEnum) or value
        self._case_sensitive = case_sensitive
        super().__init__()

    @override
    def __repr__(self) -> str:
        cls = get_class_name(self._enum)
        return f"ENUM[{cls}]"

    @override
    def convert(
        self, value: EnumLike[E], param: Parameter | None, ctx: Context | None
    ) -> E | None:
        """Convert a value into an Enum, or None."""
        match value:
            case enum.Enum():
                if isinstance(value, self._enum):
                    return value
                return self.fail(
                    f"Enum member {value.name!r} of type {get_class_name(value)!r} is not an instance of {get_class_name(self._enum)!r}",
                    param,
                    ctx,
                )
            case "":
                return None
            case str():
                try:
                    return parse_enum(
                        value, self._enum, case_sensitive=self._case_sensitive
                    )
                except ParseEnumError as error:
                    return self.fail(str(error), param, ctx)
            case never:
                assert_never(never)

    @override
    def get_metavar(self, param: Parameter, ctx: Context) -> str | None:
        _ = ctx
        desc = ",".join(str(e.value) if self._value else e.name for e in self._enum)
        return _make_metavar(param, desc)


##


class IPv4Address(ParamType):
    """An IPv4 address-valued parameter."""

    name = "ipv4"

    @override
    def __repr__(self) -> str:
        return self.name.upper()

    @override
    def convert(
        self, value: IPv4AddressLike, param: Parameter | None, ctx: Context | None
    ) -> ipaddress.IPv4Address | None:
        """Convert a value into an IPv4Address, or None."""
        match value:
            case ipaddress.IPv4Address():
                return value
            case "":
                return None
            case str():
                try:
                    return ipaddress.IPv4Address(value)
                except ValueError as error:
                    self.fail(str(error), param, ctx)
            case never:
                assert_never(never)


##


class IPv6Address(ParamType):
    """An IPv6 address-valued parameter."""

    name = "ipv6"

    @override
    def __repr__(self) -> str:
        return self.name.upper()

    @override
    def convert(
        self, value: IPv6AddressLike, param: Parameter | None, ctx: Context | None
    ) -> ipaddress.IPv6Address | None:
        """Convert a value into an IPv6Address, or None."""
        match value:
            case ipaddress.IPv6Address():
                return value
            case "":
                return None
            case str():
                try:
                    return ipaddress.IPv6Address(value)
                except ValueError as error:
                    self.fail(str(error), param, ctx)
            case never:
                assert_never(never)


##


class MonthDay(ParamType):
    """A month-day parameter."""

    name = "month-day"

    @override
    def __repr__(self) -> str:
        return self.name.upper()

    @override
    def convert(
        self, value: MonthDayLike, param: Parameter | None, ctx: Context | None
    ) -> whenever.MonthDay | None:
        """Convert a value into a MonthDay, or None."""
        match value:
            case whenever.MonthDay():
                return value
            case "":
                return None
            case str():
                try:
                    return whenever.MonthDay.parse_iso(value)
                except ValueError as error:
                    self.fail(str(error), param, ctx)
            case never:
                assert_never(never)


##


type _PathExist = Literal[
    True, False, "existing file", "existing dir", "file if exists", "dir if exists"
]


class Path(ParamType):
    """A path-valued parameter."""

    name = "path"

    @override
    def __init__(self, *, exist: _PathExist | None = None) -> None:
        self._exist = exist
        super().__init__()

    @override
    def __repr__(self) -> str:
        return self.name.upper()

    @override
    def convert(
        self, value: PathLike, param: Parameter | None, ctx: Context | None
    ) -> pathlib.Path | None:
        """Convert a value into a Path, or None."""
        match value:
            case pathlib.Path():
                self._check_path(value, param, ctx)
                return value
            case "":
                return None
            case str():
                self._check_path(value, param, ctx)
                return pathlib.Path(value)
            case never:
                assert_never(never)

    def _check_path(
        self, path: PathLike, param: Parameter | None, ctx: Context | None, /
    ) -> None:
        path = pathlib.Path(path)
        match cast("_PathExist", self._exist):
            case True:
                if not path.exists():
                    self.fail(f"{str(path)!r} does not exist", param, ctx)
            case False:
                if path.exists():
                    self.fail(f"{str(path)!r} exists", param, ctx)
            case "existing file":
                if not path.is_file():
                    self.fail(f"{str(path)!r} is not a file", param, ctx)
            case "existing dir":
                if not path.is_dir():
                    self.fail(f"{str(path)!r} is not a directory", param, ctx)
            case "file if exists":
                if path.exists() and not path.is_file():
                    self.fail(f"{str(path)!r} exists but is not a file", param, ctx)
            case "dir if exists":
                if path.exists() and not path.is_dir():
                    self.fail(
                        f"{str(path)!r} exists but is not a directory", param, ctx
                    )
            case None:
                ...
            case never:
                assert_never(never)


##


class PlainDateTime(ParamType):
    """A local-datetime-valued parameter."""

    name = "plain date-time"

    @override
    def __repr__(self) -> str:
        return self.name.upper()

    @override
    def convert(
        self, value: PlainDateTimeLike, param: Parameter | None, ctx: Context | None
    ) -> whenever.PlainDateTime | None:
        """Convert a value into a PlainDateTime, or None."""
        match value:
            case whenever.PlainDateTime():
                return value
            case "":
                return None
            case str():
                try:
                    return whenever.PlainDateTime.parse_iso(value)
                except ValueError as error:
                    self.fail(str(error), param, ctx)
            case never:
                assert_never(never)


##


class SecretStr(ParamType):
    """A secret-str-valued parameter."""

    name = "secret str"

    @override
    def __repr__(self) -> str:
        return self.name.upper()

    @override
    def convert(
        self, value: SecretLike, param: Parameter | None, ctx: Context | None
    ) -> pydantic.SecretStr | None:
        """Convert a value into a PlainDateTime, or None."""
        import pydantic

        _ = (param, ctx)
        match value:
            case pydantic.SecretStr():
                return value
            case "":
                return None
            case str():
                return pydantic.SecretStr(value)
            case never:
                assert_never(never)


##


class Str(ParamType):
    """A string-valued parameter."""

    name = "text"

    @override
    def __repr__(self) -> str:
        return self.name.upper()

    @override
    def convert(
        self, value: str, param: Parameter | None, ctx: Context | None
    ) -> str | None:
        """Convert a value into a string, or None."""
        _ = (param, ctx)
        return None if value == "" else value


##


class Time(ParamType):
    """A time-valued parameter."""

    name = "time"

    @override
    def __repr__(self) -> str:
        return self.name.upper()

    @override
    def convert(
        self, value: TimeLike, param: Parameter | None, ctx: Context | None
    ) -> whenever.Time | None:
        """Convert a value into a Time, or None."""
        match value:
            case whenever.Time():
                return value
            case "":
                return None
            case str():
                try:
                    return whenever.Time.parse_iso(value)
                except ValueError as error:
                    self.fail(str(error), param, ctx)
            case never:
                assert_never(never)


##


class TimeDelta(ParamType):
    """A timedelta-valued parameter."""

    name = "time-delta"

    @override
    def __repr__(self) -> str:
        return self.name.upper()

    @override
    def convert(
        self, value: TimeDeltaLike, param: Parameter | None, ctx: Context | None
    ) -> whenever.TimeDelta | None:
        """Convert a value into a TimeDelta, or None."""
        match value:
            case whenever.TimeDelta():
                return value
            case "":
                return None
            case str():
                try:
                    return whenever.TimeDelta.parse_iso(value)
                except ValueError as error:
                    self.fail(str(error), param, ctx)
            case never:
                assert_never(never)


##


class UUID(ParamType):
    """A UUID-valued parameter."""

    name = "uuid"

    @override
    def __repr__(self) -> str:
        return self.name.upper()

    @override
    def convert(
        self, value: uuid.UUID | str, param: Parameter | None, ctx: Context | None
    ) -> uuid.UUID | None:
        """Convert a value into a UUID, or None."""
        match value:
            case uuid.UUID():
                return value
            case "":
                return None
            case str():
                try:
                    return uuid.UUID(value)
                except ValueError:
                    self.fail(f"Invalid UUID: got {value!r}", param, ctx)
            case never:
                assert_never(never)


##


class YearMonth(ParamType):
    """A year-month parameter."""

    name = "year-month"

    @override
    def __repr__(self) -> str:
        return self.name.upper()

    @override
    def convert(
        self, value: YearMonthLike, param: Parameter | None, ctx: Context | None
    ) -> whenever.YearMonth | None:
        """Convert a value into a YearMonth, or None."""
        match value:
            case whenever.YearMonth():
                return value
            case "":
                return None
            case str():
                try:
                    return whenever.YearMonth.parse_iso(value)
                except ValueError as error:
                    self.fail(str(error), param, ctx)
            case never:
                assert_never(never)


##


class ZonedDateTime(ParamType):
    """A zoned-datetime-valued parameter."""

    name = "zoned date-time"

    @override
    def __repr__(self) -> str:
        return self.name.upper()

    @override
    def convert(
        self, value: ZonedDateTimeLike, param: Parameter | None, ctx: Context | None
    ) -> whenever.ZonedDateTime | None:
        """Convert a value into a ZonedDateTime, or None."""
        match value:
            case whenever.ZonedDateTime():
                return value
            case "":
                return None
            case str():
                try:
                    return whenever.ZonedDateTime.parse_iso(value)
                except ValueError as error:
                    self.fail(str(error), param, ctx)
            case never:
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
        self,
        value: MaybeStr[Iterable[T]] | None,
        param: Parameter | None,
        ctx: Context | None,
    ) -> frozenset[T] | None:
        """Convert a value into a frozenset, or None."""
        match value:
            case "":
                return None
            case str():
                strings = split_str(value, separator=self._separator)
                return frozenset(self._param.convert(s, param, ctx) for s in strings)
            case Iterable():
                as_list = list(value)
                return frozenset(as_list) if len(as_list) >= 1 else None
            case never:
                self.fail(
                    f"Object {str(value)!r} of type {get_class_name(value)!r} must be a frozenset",
                    param,
                    ctx,
                )
                assert_never(never)

    @override
    def get_metavar(self, param: Parameter, ctx: Context) -> str | None:
        if (metavar := self._param.get_metavar(param, ctx)) is None:
            name = self.name.upper()
        else:
            name = f"FROZENSET{metavar}"
        sep = f"SEP={self._separator}"
        desc = f"{name} {sep}"
        return _make_metavar(param, desc)


##


class FrozenSetChoices(FrozenSetParameter[Choice, str]):
    """A frozenset-of-choices-valued parameter."""

    @override
    def __init__(
        self,
        choices: list[str],
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
    ) -> list[T] | None:
        """Convert a value into a list, or None."""
        match value:
            case "":
                return None
            case str():
                strings = split_str(value, separator=self._separator)
                return [self._param.convert(s, param, ctx) for s in strings]
            case Iterable():
                as_list = list(value)
                return as_list if len(as_list) >= 1 else None
            case never:
                self.fail(
                    f"Object {str(value)!r} of type {get_class_name(value)!r} must be a list",
                    param,
                    ctx,
                )
                assert_never(never)

    @override
    def get_metavar(self, param: Parameter, ctx: Context) -> str | None:
        if (metavar := self._param.get_metavar(param, ctx)) is None:
            name = self.name.upper()
        else:
            name = f"LIST{metavar}"
        sep = f"SEP={self._separator}"
        desc = f"{name} {sep}"
        return _make_metavar(param, desc)


##


class ListChoices(ListParameter[Choice, str]):
    """A frozenset-of-choices-valued parameter."""

    @override
    def __init__(
        self,
        choices: list[str],
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
    "CONTEXT_SETTINGS",
    "UUID",
    "Bool",
    "Date",
    "DateDelta",
    "DateTimeDelta",
    "Enum",
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
    "Path",
    "Path",
    "PlainDateTime",
    "SecretStr",
    "Str",
    "Time",
    "TimeDelta",
    "YearMonth",
    "ZonedDateTime",
]
