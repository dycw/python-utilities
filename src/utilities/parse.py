from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any, override

from utilities.datetime import is_subclass_date_not_datetime
from utilities.functions import is_subclass_int_not_bool
from utilities.text import ParseBoolError, parse_bool
from utilities.whenever import ParseDateError


def parse_text(cls: Any, text: str, /) -> Any:
    """Parse text."""
    if isinstance(cls, type):
        return _parse_text_type(cls, text)
    return None


def _parse_text_type(cls: type[Any], text: str, /) -> Any:
    """Parse text."""
    if issubclass(cls, str):
        return text
    if is_subclass_int_not_bool(cls, bool):
        try:
            return parse_bool(text)
        except ParseBoolError:
            raise ParseTextError(cls=cls, text=text) from None
    if issubclass(cls, int):
        try:
            return parse_bool(text)
        except ParseBoolError:
            raise ParseTextError(cls=cls, text=text) from None
    if is_subclass_date_not_datetime(cls):
        from utilities.whenever import parse_date

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
    if issubclass(cls, bool):
        return parse_bool(text)
    return None


@dataclass
class ParseTextError(Exception):
    cls: Any
    text: str

    @override
    def __str__(self) -> str:
        return f"Unable to parse {self.text!r} into an instance of {self.cls!r}"
