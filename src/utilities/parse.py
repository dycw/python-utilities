from __future__ import annotations

import datetime as dt
from contextlib import suppress
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from re import DOTALL
from types import NoneType
from typing import TYPE_CHECKING, Any, override

from utilities.datetime import (
    is_instance_date_not_datetime,
    is_subclass_date_not_datetime,
)
from utilities.enum import ParseEnumError, parse_enum
from utilities.functions import is_subclass_int_not_bool
from utilities.iterables import OneEmptyError, OneNonUniqueError, one, one_str
from utilities.math import ParseNumberError, parse_number
from utilities.re import ExtractGroupError, extract_group
from utilities.sentinel import ParseSentinelError, Sentinel, parse_sentinel
from utilities.text import (
    ParseBoolError,
    ParseNoneError,
    join_strs,
    parse_bool,
    parse_none,
    split_key_value_pairs,
    split_str,
)
from utilities.types import Duration, Number, ParseTextExtra
from utilities.typing import (
    get_args,
    is_dict_type,
    is_frozenset_type,
    is_list_type,
    is_literal_type,
    is_optional_type,
    is_set_type,
    is_tuple_type,
    is_union_type,
)
from utilities.version import ParseVersionError, Version, parse_version

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from collections.abc import Set as AbstractSet


def parse_text(
    type_: Any,
    text: str,
    /,
    *,
    list_separator: str = ",",
    pair_separator: str = "=",
    head: bool = False,
    case_sensitive: bool = False,
    extra: ParseTextExtra | None = None,
) -> Any:
    """Parse text."""
    if type_ is None:
        try:
            return parse_none(text)
        except ParseNoneError:
            raise _ParseTextParseError(type_=type_, text=text) from None
    if isinstance(type_, type):
        return _parse_text_type(type_, text, case_sensitive=case_sensitive, extra=extra)
    if is_dict_type(type_):
        return _parse_text_dict_type(
            type_,
            text,
            list_separator=list_separator,
            pair_separator=pair_separator,
            head=head,
            case_sensitive=case_sensitive,
            extra=extra,
        )
    if is_frozenset_type(type_):
        return frozenset(
            _parse_text_set_type(
                type_,
                text,
                list_separator=list_separator,
                pair_separator=pair_separator,
                head=head,
                case_sensitive=case_sensitive,
                extra=extra,
            )
        )
    if is_list_type(type_):
        return _parse_text_list_type(
            type_,
            text,
            list_separator=list_separator,
            pair_separator=pair_separator,
            head=head,
            case_sensitive=case_sensitive,
            extra=extra,
        )
    if is_literal_type(type_):
        return one_str(get_args(type_), text, head=head, case_sensitive=case_sensitive)
    if is_optional_type(type_):
        with suppress(ParseNoneError):
            return parse_none(text)
        inner = one(arg for arg in get_args(type_) if arg is not NoneType)
        try:
            return parse_text(
                inner,
                text,
                list_separator=list_separator,
                pair_separator=pair_separator,
                head=head,
                case_sensitive=case_sensitive,
                extra=extra,
            )
        except _ParseTextParseError:
            raise _ParseTextParseError(type_=type_, text=text) from None
    if is_set_type(type_):
        return _parse_text_set_type(
            type_,
            text,
            list_separator=list_separator,
            pair_separator=pair_separator,
            head=head,
            case_sensitive=case_sensitive,
            extra=extra,
        )
    if is_tuple_type(type_):
        return _parse_text_tuple_type(
            type_,
            text,
            list_separator=list_separator,
            pair_separator=pair_separator,
            head=head,
            case_sensitive=case_sensitive,
            extra=extra,
        )
    if is_union_type(type_):
        return _parse_text_union_type(type_, text, extra=extra)
    raise _ParseTextParseError(type_=type_, text=text) from None


def _parse_text_type(
    cls: type[Any],
    text: str,
    /,
    *,
    case_sensitive: bool = False,
    extra: ParseTextExtra | None = None,
) -> Any:
    """Parse text."""
    if issubclass(cls, NoneType):
        try:
            return parse_none(text)
        except ParseNoneError:
            raise _ParseTextParseError(type_=cls, text=text) from None
    if issubclass(cls, str):
        return text
    if issubclass(cls, bool):
        try:
            return parse_bool(text)
        except ParseBoolError:
            raise _ParseTextParseError(type_=cls, text=text) from None
    if is_subclass_int_not_bool(cls):
        try:
            return int(text)
        except ValueError:
            raise _ParseTextParseError(type_=cls, text=text) from None
    if issubclass(cls, float):
        try:
            return float(text)
        except ValueError:
            raise _ParseTextParseError(type_=cls, text=text) from None
    if issubclass(cls, Enum):
        try:
            return parse_enum(text, cls, case_sensitive=case_sensitive)
        except ParseEnumError:
            raise _ParseTextParseError(type_=cls, text=text) from None
    if issubclass(cls, Path):
        return Path(text).expanduser()
    if issubclass(cls, Sentinel):
        try:
            return parse_sentinel(text)
        except ParseSentinelError:
            raise _ParseTextParseError(type_=cls, text=text) from None
    if issubclass(cls, Version):
        try:
            return parse_version(text)
        except ParseVersionError:
            raise _ParseTextParseError(type_=cls, text=text) from None
    if is_subclass_date_not_datetime(cls):
        from utilities.whenever import ParseDateError, parse_date

        try:
            return parse_date(text)
        except ParseDateError:
            raise _ParseTextParseError(type_=cls, text=text) from None
    if issubclass(cls, dt.datetime):
        from utilities.whenever import ParseDateTimeError, parse_datetime

        try:
            return parse_datetime(text)
        except ParseDateTimeError:
            raise _ParseTextParseError(type_=cls, text=text) from None
    if issubclass(cls, dt.time):
        from utilities.whenever import ParseTimeError, parse_time

        try:
            return parse_time(text)
        except ParseTimeError:
            raise _ParseTextParseError(type_=cls, text=text) from None
    if issubclass(cls, dt.timedelta):
        from utilities.whenever import ParseTimedeltaError, parse_timedelta

        try:
            return parse_timedelta(text)
        except ParseTimedeltaError:
            raise _ParseTextParseError(type_=cls, text=text) from None
    if extra is not None:
        try:
            parser = one(p for c, p in extra.items() if issubclass(cls, c))
        except OneEmptyError:
            pass
        except OneNonUniqueError as error:
            raise _ParseTextExtraNonUniqueError(
                type_=cls, text=text, first=error.first, second=error.second
            ) from None
        else:
            return parser(text)
    raise _ParseTextParseError(type_=cls, text=text) from None


def _parse_text_dict_type(
    type_: Any,
    text: str,
    /,
    *,
    list_separator: str = ",",
    pair_separator: str = "=",
    head: bool = False,
    case_sensitive: bool = False,
    extra: ParseTextExtra | None = None,
) -> dict[Any, Any]:
    key_type, value_type = get_args(type_)
    try:
        inner_text = extract_group(r"^{(.*)}$", text, flags=DOTALL)
    except ExtractGroupError:
        raise _ParseTextParseError(type_=type_, text=text) from None
    pairs = split_key_value_pairs(
        inner_text,
        list_separator=list_separator,
        pair_separator=pair_separator,
        mapping=True,
    )
    keys = (
        parse_text(
            key_type,
            k,
            list_separator=list_separator,
            pair_separator=pair_separator,
            head=head,
            case_sensitive=case_sensitive,
            extra=extra,
        )
        for k in pairs
    )
    values = (
        parse_text(
            value_type,
            v,
            list_separator=list_separator,
            pair_separator=pair_separator,
            head=head,
            case_sensitive=case_sensitive,
            extra=extra,
        )
        for v in pairs.values()
    )
    try:
        return dict(zip(keys, values, strict=True))
    except _ParseTextParseError:
        raise _ParseTextParseError(type_=type_, text=text) from None


def _parse_text_list_type(
    type_: Any,
    text: str,
    /,
    *,
    list_separator: str = ",",
    pair_separator: str = "=",
    head: bool = False,
    case_sensitive: bool = False,
    extra: ParseTextExtra | None = None,
) -> list[Any]:
    inner_type = one(get_args(type_))
    try:
        inner_text = extract_group(r"^\[(.*)\]$", text, flags=DOTALL)
    except ExtractGroupError:
        raise _ParseTextParseError(type_=type_, text=text) from None
    texts = split_str(inner_text, separator=list_separator)
    try:
        return [
            parse_text(
                inner_type,
                t,
                list_separator=list_separator,
                pair_separator=pair_separator,
                head=head,
                case_sensitive=case_sensitive,
                extra=extra,
            )
            for t in texts
        ]
    except _ParseTextParseError:
        raise _ParseTextParseError(type_=type_, text=text) from None


def _parse_text_set_type(
    type_: Any,
    text: str,
    /,
    *,
    list_separator: str = ",",
    pair_separator: str = "=",
    head: bool = False,
    case_sensitive: bool = False,
    extra: ParseTextExtra | None = None,
) -> set[Any]:
    inner_type = one(get_args(type_))
    try:
        inner_text = extract_group(r"^{(.*)}$", text, flags=DOTALL)
    except ExtractGroupError:
        raise _ParseTextParseError(type_=type_, text=text) from None
    texts = split_str(inner_text, separator=list_separator)
    try:
        return {
            parse_text(
                inner_type,
                t,
                list_separator=list_separator,
                pair_separator=pair_separator,
                head=head,
                case_sensitive=case_sensitive,
                extra=extra,
            )
            for t in texts
        }
    except _ParseTextParseError:
        raise _ParseTextParseError(type_=type_, text=text) from None


def _parse_text_union_type(
    type_: Any, text: str, /, *, extra: ParseTextExtra | None = None
) -> Any:
    if type_ is Number:
        try:
            return parse_number(text)
        except ParseNumberError:
            raise _ParseTextParseError(type_=type_, text=text) from None
    if type_ is Duration:
        from utilities.whenever import ParseDurationError, parse_duration

        try:
            return parse_duration(text)
        except ParseDurationError:
            raise _ParseTextParseError(type_=type_, text=text) from None
    if extra is not None:
        try:
            parser = one(p for c, p in extra.items() if c is type_)
        except OneEmptyError:
            pass
        else:
            return parser(text)
    raise _ParseTextParseError(type_=type_, text=text) from None


def _parse_text_tuple_type(
    type_: Any,
    text: str,
    /,
    *,
    list_separator: str = ",",
    pair_separator: str = "=",
    head: bool = False,
    case_sensitive: bool = False,
    extra: ParseTextExtra | None = None,
) -> tuple[Any, ...]:
    args = get_args(type_)
    try:
        inner = extract_group(r"^\((.*)\)$", text, flags=DOTALL)
    except ExtractGroupError:
        raise _ParseTextParseError(type_=type_, text=text) from None
    texts = inner.split(",")
    if len(args) != len(texts):
        raise _ParseTextParseError(type_=type_, text=text)
    try:
        return tuple(
            parse_text(
                arg,
                text,
                list_separator=list_separator,
                pair_separator=pair_separator,
                head=head,
                case_sensitive=case_sensitive,
                extra=extra,
            )
            for arg, text in zip(args, texts, strict=True)
        )
    except _ParseTextParseError:
        raise _ParseTextParseError(type_=type_, text=text) from None


@dataclass
class ParseTextError(Exception):
    type_: Any
    text: str


@dataclass
class _ParseTextParseError(ParseTextError):
    @override
    def __str__(self) -> str:
        return f"Unable to parse {self.type_!r}; got {self.text!r}"


@dataclass
class _ParseTextExtraNonUniqueError(ParseTextError):
    first: type[Any]
    second: type[Any]

    @override
    def __str__(self) -> str:
        return f"Unable to parse {self.type_!r} since `extra` must contain exactly one parent class; got {self.first!r}, {self.second!r} and perhaps more"


##


def to_text(
    obj: Any, /, *, list_separator: str = ",", pair_separator: str = "="
) -> str:
    """Convert an object to text."""
    if (obj is None) or isinstance(
        obj, bool | int | float | str | Path | Sentinel | Version
    ):
        return str(obj)
    if is_instance_date_not_datetime(obj):
        from utilities.whenever import serialize_date

        return serialize_date(obj)
    if isinstance(obj, dt.datetime):
        from utilities.whenever import serialize_datetime

        return serialize_datetime(obj)
    if isinstance(obj, dt.time):
        from utilities.whenever import serialize_time

        return serialize_time(obj)
    if isinstance(obj, dt.timedelta):
        from utilities.whenever import serialize_timedelta

        return serialize_timedelta(obj)
    if isinstance(obj, Enum):
        return obj.name
    if isinstance(obj, dict):
        return _to_text_dict(
            obj, list_separator=list_separator, pair_separator=pair_separator
        )
    if isinstance(obj, list):
        return _to_text_list(
            obj, list_separator=list_separator, pair_separator=pair_separator
        )
    if isinstance(obj, tuple):
        return _to_text_tuple(
            obj, list_separator=list_separator, pair_separator=pair_separator
        )
    if isinstance(obj, set | frozenset):
        return _to_text_set(
            obj, list_separator=list_separator, pair_separator=pair_separator
        )
    raise NotImplementedError(obj)


def _to_text_dict(
    obj: Mapping[Any, Any], /, *, list_separator: str = ",", pair_separator: str = "="
) -> str:
    keys = (
        to_text(k, list_separator=list_separator, pair_separator=pair_separator)
        for k in obj
    )
    values = (
        to_text(v, list_separator=list_separator, pair_separator=pair_separator)
        for v in obj.values()
    )
    items = zip(keys, values, strict=True)
    joined_items = (join_strs(item, separator=pair_separator) for item in items)
    joined = join_strs(joined_items, separator=list_separator)
    return f"{{{joined}}}"


def _to_text_list(
    obj: Sequence[Any], /, *, list_separator: str = ",", pair_separator: str = "="
) -> str:
    items = (
        to_text(i, list_separator=list_separator, pair_separator=pair_separator)
        for i in obj
    )
    joined = join_strs(items, separator=list_separator)
    return f"[{joined}]"


def _to_text_set(
    obj: AbstractSet[Any], /, *, list_separator: str = ",", pair_separator: str = "="
) -> str:
    items = (
        to_text(i, list_separator=list_separator, pair_separator=pair_separator)
        for i in obj
    )
    joined = join_strs(items, sort=True, separator=list_separator)
    return f"{{{joined}}}"


def _to_text_tuple(
    obj: tuple[Any, ...], /, *, list_separator: str = ",", pair_separator: str = "="
) -> str:
    items = (
        to_text(i, list_separator=list_separator, pair_separator=pair_separator)
        for i in obj
    )
    joined = join_strs(items, separator=list_separator)
    return f"({joined})"


__all__ = ["parse_text"]
