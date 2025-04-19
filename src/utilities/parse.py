from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, override

from utilities.datetime import is_subclass_date_not_datetime
from utilities.enum import ParseEnumError, parse_enum
from utilities.functions import is_subclass_int_not_bool
from utilities.text import ParseBoolError, parse_bool
from utilities.version import ParseVersionError, Version, parse_version


def parse_text(cls: Any, text: str, /, *, case_sensitive: bool = False) -> Any:
    """Parse text."""
    if isinstance(cls, type):
        return _parse_text_type(cls, text, case_sensitive=case_sensitive)
    return None


def _parse_text_type(
    cls: type[Any], text: str, /, *, case_sensitive: bool = False
) -> Any:
    """Parse text."""
    if issubclass(cls, str):
        return text
    if issubclass(cls, bool):
        try:
            return parse_bool(text)
        except ParseBoolError:
            raise ParseTextError(cls=cls, text=text) from None
    if is_subclass_int_not_bool(cls):
        try:
            return int(text)
        except ValueError:
            raise ParseTextError(cls=cls, text=text) from None
    if issubclass(cls, float):
        try:
            return float(text)
        except ValueError:
            raise ParseTextError(cls=cls, text=text) from None
    if issubclass(cls, Enum):
        try:
            return parse_enum(text, cls, case_sensitive=case_sensitive)
        except ParseEnumError:
            raise ParseTextError(cls=cls, text=text) from None
    if issubclass(cls, Path):
        try:
            return Path(text).expanduser()
        except TypeError:
            raise ParseTextError(cls=cls, text=text) from None
    if issubclass(cls, Version):
        try:
            return parse_version(text)
        except ParseVersionError:
            raise ParseTextError(cls=cls, text=text) from None
    if is_subclass_date_not_datetime(cls):
        from utilities.whenever import ParseDateError, parse_date

        try:
            return parse_date(text)
        except ParseDateError:
            raise ParseTextError(cls=cls, text=text) from None
    if issubclass(cls, dt.datetime):
        from utilities.whenever import ParseDateTimeError, parse_datetime

        try:
            return parse_datetime(text)
        except ParseDateTimeError:
            raise ParseTextError(cls=cls, text=text) from None
    if issubclass(cls, dt.time):
        from utilities.whenever import ParseTimeError, parse_time

        try:
            return parse_time(text)
        except ParseTimeError:
            raise ParseTextError(cls=cls, text=text) from None
    if issubclass(cls, dt.timedelta):
        from utilities.whenever import ParseTimedeltaError, parse_timedelta

        try:
            return parse_timedelta(text)
        except ParseTimedeltaError:
            raise ParseTextError(cls=cls, text=text) from None
    raise ParseTextError(cls=cls, text=text) from None


@dataclass
class ParseTextError(Exception):
    cls: Any
    text: str

    @override
    def __str__(self) -> str:
        return f"Unable to parse {self.text!r} into an instance of {self.cls!r}"
