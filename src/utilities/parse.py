from __future__ import annotations

import datetime as dt
from contextlib import suppress
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from re import DOTALL
from types import NoneType
from typing import TYPE_CHECKING, Any, TypeVar, override

from utilities.datetime import is_subclass_date_not_datetime
from utilities.enum import ParseEnumError, parse_enum
from utilities.functions import is_subclass_int_not_bool
from utilities.iterables import OneEmptyError, OneNonUniqueError, one, one_str
from utilities.re import ExtractGroupError, extract_group
from utilities.sentinel import ParseSentinelError, Sentinel, parse_sentinel
from utilities.text import ParseBoolError, ParseNoneError, parse_bool, parse_none
from utilities.types import Duration
from utilities.typing import (
    get_args,
    is_literal_type,
    is_optional_type,
    is_tuple_type,
    is_union_type,
)
from utilities.version import ParseVersionError, Version, parse_version

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping


_T = TypeVar("_T")


def parse_text(
    obj: Any,
    text: str,
    /,
    *,
    case_sensitive: bool = False,
    head: bool = False,
    extra: Mapping[type[_T], Callable[[str], _T]] | None = None,
) -> Any:
    """Parse text."""
    if obj is None:
        try:
            return parse_none(text)
        except ParseNoneError:
            raise _ParseTextParseError(obj=obj, text=text) from None
    if isinstance(obj, type):
        return _parse_text_type(obj, text, case_sensitive=case_sensitive, extra=extra)
    if is_literal_type(obj):
        return one_str(get_args(obj), text, head=head, case_sensitive=case_sensitive)
    if is_optional_type(obj):
        with suppress(ParseNoneError):
            return parse_none(text)
        inner = one(arg for arg in get_args(obj) if arg is not NoneType)
        if isinstance(
            inner := one(arg for arg in get_args(obj) if arg is not NoneType), type
        ):
            try:
                return _parse_text_type(inner, text, case_sensitive=case_sensitive)
            except _ParseTextParseError:
                raise _ParseTextParseError(obj=obj, text=text) from None
    if is_tuple_type(obj):
        args = get_args(obj)
        try:
            texts = extract_group(r"^\((.*)\)$", text, flags=DOTALL).split(", ")
        except ExtractGroupError:
            raise _ParseTextParseError(obj=obj, text=text) from None
        if len(args) != len(texts):
            raise _ParseTextParseError(obj=obj, text=text)
        return tuple(
            parse_text(arg, text, case_sensitive=case_sensitive, head=head)
            for arg, text in zip(args, texts, strict=True)
        )
    if is_union_type(obj) and (obj is Duration):
        from utilities.whenever import ParseDurationError, parse_duration

        try:
            return parse_duration(text)
        except ParseDurationError:
            raise _ParseTextParseError(obj=obj, text=text) from None
    raise _ParseTextParseError(obj=obj, text=text) from None


def _parse_text_type(
    cls: type[Any],
    text: str,
    /,
    *,
    case_sensitive: bool = False,
    extra: Mapping[type[_T], Callable[[str], _T]] | None = None,
) -> Any:
    """Parse text."""
    if issubclass(cls, NoneType):
        try:
            return parse_none(text)
        except ParseNoneError:
            raise _ParseTextParseError(obj=cls, text=text) from None
    if issubclass(cls, str):
        return text
    if issubclass(cls, bool):
        try:
            return parse_bool(text)
        except ParseBoolError:
            raise _ParseTextParseError(obj=cls, text=text) from None
    if is_subclass_int_not_bool(cls):
        try:
            return int(text)
        except ValueError:
            raise _ParseTextParseError(obj=cls, text=text) from None
    if issubclass(cls, float):
        try:
            return float(text)
        except ValueError:
            raise _ParseTextParseError(obj=cls, text=text) from None
    if issubclass(cls, Enum):
        try:
            return parse_enum(text, cls, case_sensitive=case_sensitive)
        except ParseEnumError:
            raise _ParseTextParseError(obj=cls, text=text) from None
    if issubclass(cls, Path):
        return Path(text).expanduser()
    if issubclass(cls, Sentinel):
        try:
            return parse_sentinel(text)
        except ParseSentinelError:
            raise _ParseTextParseError(obj=cls, text=text) from None
    if issubclass(cls, Version):
        try:
            return parse_version(text)
        except ParseVersionError:
            raise _ParseTextParseError(obj=cls, text=text) from None
    if is_subclass_date_not_datetime(cls):
        from utilities.whenever import ParseDateError, parse_date

        try:
            return parse_date(text)
        except ParseDateError:
            raise _ParseTextParseError(obj=cls, text=text) from None
    if issubclass(cls, dt.datetime):
        from utilities.whenever import ParseDateTimeError, parse_datetime

        try:
            return parse_datetime(text)
        except ParseDateTimeError:
            raise _ParseTextParseError(obj=cls, text=text) from None
    if issubclass(cls, dt.time):
        from utilities.whenever import ParseTimeError, parse_time

        try:
            return parse_time(text)
        except ParseTimeError:
            raise _ParseTextParseError(obj=cls, text=text) from None
    if issubclass(cls, dt.timedelta):
        from utilities.whenever import ParseTimedeltaError, parse_timedelta

        try:
            return parse_timedelta(text)
        except ParseTimedeltaError:
            raise _ParseTextParseError(obj=cls, text=text) from None
    if extra is not None:
        try:
            parser = one(p for c, p in extra.items() if issubclass(cls, c))
        except OneEmptyError:
            pass
        except OneNonUniqueError as error:
            raise _ParseTextExtraNonUniqueError(
                obj=cls, text=text, first=error.first, second=error.second
            ) from None
        else:
            return parser(text)
    raise _ParseTextParseError(obj=cls, text=text) from None


@dataclass
class ParseTextError(Exception):
    obj: Any
    text: str


@dataclass
class _ParseTextParseError(ParseTextError):
    @override
    def __str__(self) -> str:
        return f"Unable to parse {self.obj!r}; got {self.text!r}"


@dataclass
class _ParseTextExtraNonUniqueError(ParseTextError):
    first: type[Any]
    second: type[Any]

    @override
    def __str__(self) -> str:
        return f"Unable to parse {self.obj!r} since `extra` must contain exactly one parent class; got {self.first!r}, {self.second!r} and perhaps more"
