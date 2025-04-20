from __future__ import annotations

import datetime as dt
from contextlib import suppress
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from types import NoneType
from typing import Any, override

from utilities.datetime import is_subclass_date_not_datetime
from utilities.enum import ParseEnumError, parse_enum
from utilities.functions import is_subclass_int_not_bool
from utilities.iterables import one, one_str
from utilities.sentinel import ParseSentinelError, Sentinel, parse_sentinel
from utilities.text import ParseBoolError, ParseNoneError, parse_bool, parse_none
from utilities.typing import get_args, is_literal_type, is_optional_type
from utilities.version import ParseVersionError, Version, parse_version


def parse_text(
    obj: Any, text: str, /, *, case_sensitive: bool = False, head: bool = False
) -> Any:
    """Parse text."""
    if obj is None:
        try:
            return parse_none(text)
        except ParseNoneError:
            raise ParseTextError(obj=obj, text=text) from None
    if isinstance(obj, type):
        return _parse_text_type(obj, text, case_sensitive=case_sensitive)
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
            except ParseTextError:
                raise ParseTextError(obj=obj, text=text) from None
    raise ParseTextError(obj=obj, text=text) from None


def _parse_text_type(
    cls: type[Any], text: str, /, *, case_sensitive: bool = False
) -> Any:
    """Parse text."""
    if issubclass(cls, NoneType):
        try:
            return parse_none(text)
        except ParseNoneError:
            raise ParseTextError(obj=cls, text=text) from None
    if issubclass(cls, str):
        return text
    if issubclass(cls, bool):
        try:
            return parse_bool(text)
        except ParseBoolError:
            raise ParseTextError(obj=cls, text=text) from None
    if is_subclass_int_not_bool(cls):
        try:
            return int(text)
        except ValueError:
            raise ParseTextError(obj=cls, text=text) from None
    if issubclass(cls, float):
        try:
            return float(text)
        except ValueError:
            raise ParseTextError(obj=cls, text=text) from None
    if issubclass(cls, Enum):
        try:
            return parse_enum(text, cls, case_sensitive=case_sensitive)
        except ParseEnumError:
            raise ParseTextError(obj=cls, text=text) from None
    if issubclass(cls, Path):
        return Path(text).expanduser()
    if issubclass(cls, Sentinel):
        try:
            return parse_sentinel(text)
        except ParseSentinelError:
            raise ParseTextError(obj=cls, text=text) from None
    if issubclass(cls, Version):
        try:
            return parse_version(text)
        except ParseVersionError:
            raise ParseTextError(obj=cls, text=text) from None
    if is_subclass_date_not_datetime(cls):
        from utilities.whenever import ParseDateError, parse_date

        try:
            return parse_date(text)
        except ParseDateError:
            raise ParseTextError(obj=cls, text=text) from None
    if issubclass(cls, dt.datetime):
        from utilities.whenever import ParseDateTimeError, parse_datetime

        try:
            return parse_datetime(text)
        except ParseDateTimeError:
            raise ParseTextError(obj=cls, text=text) from None
    if issubclass(cls, dt.time):
        from utilities.whenever import ParseTimeError, parse_time

        try:
            return parse_time(text)
        except ParseTimeError:
            raise ParseTextError(obj=cls, text=text) from None
    if issubclass(cls, dt.timedelta):
        from utilities.whenever import ParseTimedeltaError, parse_timedelta

        try:
            return parse_timedelta(text)
        except ParseTimedeltaError:
            raise ParseTextError(obj=cls, text=text) from None
    raise ParseTextError(obj=cls, text=text) from None


@dataclass
class ParseTextError(Exception):
    obj: Any
    text: str

    @override
    def __str__(self) -> str:
        return f"Unable to parse {self.obj!r}; got {self.text!r}"
