from __future__ import annotations

import datetime as dt
from contextlib import suppress
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from re import DOTALL
from types import NoneType
from typing import TYPE_CHECKING, Any, override

from utilities.enum import ParseEnumError, parse_enum
from utilities.iterables import OneEmptyError, OneNonUniqueError, one, one_str
from utilities.math import ParseNumberError, parse_number
from utilities.re import ExtractGroupError, extract_group
from utilities.sentinel import ParseSentinelError, Sentinel, parse_sentinel
from utilities.text import (
    BRACKETS,
    LIST_SEPARATOR,
    PAIR_SEPARATOR,
    ParseBoolError,
    ParseNoneError,
    join_strs,
    parse_bool,
    parse_none,
    split_key_value_pairs,
    split_str,
)
from utilities.types import Duration, Number, ParseObjectExtra, SerializeObjectExtra
from utilities.typing import (
    get_args,
    is_dict_type,
    is_frozenset_type,
    is_instance_gen,
    is_list_type,
    is_literal_type,
    is_optional_type,
    is_set_type,
    is_subclass_gen,
    is_tuple_type,
    is_union_type,
)
from utilities.version import ParseVersionError, Version, parse_version

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence
    from collections.abc import Set as AbstractSet


def parse_object(
    type_: Any,
    text: str,
    /,
    *,
    list_separator: str = LIST_SEPARATOR,
    pair_separator: str = PAIR_SEPARATOR,
    brackets: Iterable[tuple[str, str]] | None = BRACKETS,
    head: bool = False,
    case_sensitive: bool = False,
    extra: ParseObjectExtra | None = None,
) -> Any:
    """Parse text."""
    if extra is not None:
        with suppress(_ParseObjectParseError):
            return _parse_object_extra(type_, text, extra)
    if type_ is None:
        try:
            return parse_none(text)
        except ParseNoneError:
            raise _ParseObjectParseError(type_=type_, text=text) from None
    if isinstance(type_, type):
        return _parse_object_type(type_, text, case_sensitive=case_sensitive)
    if is_dict_type(type_):
        return _parse_object_dict_type(
            type_,
            text,
            list_separator=list_separator,
            pair_separator=pair_separator,
            brackets=brackets,
            head=head,
            case_sensitive=case_sensitive,
            extra=extra,
        )
    if is_frozenset_type(type_):
        return frozenset(
            _parse_object_set_type(
                type_,
                text,
                list_separator=list_separator,
                pair_separator=pair_separator,
                brackets=brackets,
                head=head,
                case_sensitive=case_sensitive,
                extra=extra,
            )
        )
    if is_list_type(type_):
        return _parse_object_list_type(
            type_,
            text,
            list_separator=list_separator,
            pair_separator=pair_separator,
            brackets=brackets,
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
            return parse_object(
                inner,
                text,
                list_separator=list_separator,
                pair_separator=pair_separator,
                brackets=brackets,
                head=head,
                case_sensitive=case_sensitive,
                extra=extra,
            )
        except _ParseObjectParseError:
            raise _ParseObjectParseError(type_=type_, text=text) from None
    if is_set_type(type_):
        return _parse_object_set_type(
            type_,
            text,
            list_separator=list_separator,
            pair_separator=pair_separator,
            brackets=brackets,
            head=head,
            case_sensitive=case_sensitive,
            extra=extra,
        )
    if is_tuple_type(type_):
        return _parse_object_tuple_type(
            type_,
            text,
            list_separator=list_separator,
            pair_separator=pair_separator,
            brackets=brackets,
            head=head,
            case_sensitive=case_sensitive,
            extra=extra,
        )
    if is_union_type(type_):
        return _parse_object_union_type(type_, text)
    raise _ParseObjectParseError(type_=type_, text=text) from None


def _parse_object_type(
    cls: type[Any], text: str, /, *, case_sensitive: bool = False
) -> Any:
    """Parse text."""
    if issubclass(cls, NoneType):
        try:
            return parse_none(text)
        except ParseNoneError:
            raise _ParseObjectParseError(type_=cls, text=text) from None
    if issubclass(cls, str):
        return text
    if is_subclass_gen(cls, bool):
        try:
            return parse_bool(text)
        except ParseBoolError:
            raise _ParseObjectParseError(type_=cls, text=text) from None
    if is_subclass_gen(cls, int):
        try:
            return int(text)
        except ValueError:
            raise _ParseObjectParseError(type_=cls, text=text) from None
    if issubclass(cls, float):
        try:
            return float(text)
        except ValueError:
            raise _ParseObjectParseError(type_=cls, text=text) from None
    if issubclass(cls, Enum):
        try:
            return parse_enum(text, cls, case_sensitive=case_sensitive)
        except ParseEnumError:
            raise _ParseObjectParseError(type_=cls, text=text) from None
    if issubclass(cls, Path):
        return Path(text).expanduser()
    if issubclass(cls, Sentinel):
        try:
            return parse_sentinel(text)
        except ParseSentinelError:
            raise _ParseObjectParseError(type_=cls, text=text) from None
    if issubclass(cls, Version):
        try:
            return parse_version(text)
        except ParseVersionError:
            raise _ParseObjectParseError(type_=cls, text=text) from None
    if is_subclass_gen(cls, dt.date):
        from utilities.whenever import ParseDateError, parse_date

        try:
            return parse_date(text)
        except ParseDateError:
            raise _ParseObjectParseError(type_=cls, text=text) from None
    if is_subclass_gen(cls, dt.datetime):
        from utilities.whenever import ParseDateTimeError, parse_datetime

        try:
            return parse_datetime(text)
        except ParseDateTimeError:
            raise _ParseObjectParseError(type_=cls, text=text) from None
    if issubclass(cls, dt.time):
        from utilities.whenever import ParseTimeError, parse_time

        try:
            return parse_time(text)
        except ParseTimeError:
            raise _ParseObjectParseError(type_=cls, text=text) from None
    if issubclass(cls, dt.timedelta):
        from utilities.whenever import ParseTimedeltaError, parse_timedelta

        try:
            return parse_timedelta(text)
        except ParseTimedeltaError:
            raise _ParseObjectParseError(type_=cls, text=text) from None
    raise _ParseObjectParseError(type_=cls, text=text)


def _parse_object_dict_type(
    type_: Any,
    text: str,
    /,
    *,
    list_separator: str = LIST_SEPARATOR,
    pair_separator: str = PAIR_SEPARATOR,
    brackets: Iterable[tuple[str, str]] | None = BRACKETS,
    head: bool = False,
    case_sensitive: bool = False,
    extra: ParseObjectExtra | None = None,
) -> dict[Any, Any]:
    key_type, value_type = get_args(type_)
    try:
        inner_text = extract_group(r"^{(.*)}$", text, flags=DOTALL)
    except ExtractGroupError:
        raise _ParseObjectParseError(type_=type_, text=text) from None
    pairs = split_key_value_pairs(
        inner_text,
        list_separator=list_separator,
        pair_separator=pair_separator,
        brackets=brackets,
        mapping=True,
    )
    keys = (
        parse_object(
            key_type,
            k,
            list_separator=list_separator,
            pair_separator=pair_separator,
            brackets=brackets,
            head=head,
            case_sensitive=case_sensitive,
            extra=extra,
        )
        for k in pairs
    )
    values = (
        parse_object(
            value_type,
            v,
            list_separator=list_separator,
            pair_separator=pair_separator,
            brackets=brackets,
            head=head,
            case_sensitive=case_sensitive,
            extra=extra,
        )
        for v in pairs.values()
    )
    try:
        return dict(zip(keys, values, strict=True))
    except _ParseObjectParseError:
        raise _ParseObjectParseError(type_=type_, text=text) from None


def _parse_object_extra(cls: Any, text: str, extra: ParseObjectExtra, /) -> Any:
    try:
        parser = extra[cls]
    except KeyError:
        try:
            parser = one(p for c, p in extra.items() if is_subclass_gen(cls, c))
        except (OneEmptyError, TypeError):
            raise _ParseObjectParseError(type_=cls, text=text) from None
        except OneNonUniqueError as error:
            raise _ParseObjectExtraNonUniqueError(
                type_=cls, text=text, first=error.first, second=error.second
            ) from None
    return parser(text)


def _parse_object_list_type(
    type_: Any,
    text: str,
    /,
    *,
    list_separator: str = LIST_SEPARATOR,
    pair_separator: str = PAIR_SEPARATOR,
    brackets: Iterable[tuple[str, str]] | None = BRACKETS,
    head: bool = False,
    case_sensitive: bool = False,
    extra: ParseObjectExtra | None = None,
) -> list[Any]:
    inner_type = one(get_args(type_))
    try:
        inner_text = extract_group(r"^\[(.*)\]$", text, flags=DOTALL)
    except ExtractGroupError:
        raise _ParseObjectParseError(type_=type_, text=text) from None
    texts = split_str(inner_text, separator=list_separator)
    try:
        return [
            parse_object(
                inner_type,
                t,
                list_separator=list_separator,
                pair_separator=pair_separator,
                brackets=brackets,
                head=head,
                case_sensitive=case_sensitive,
                extra=extra,
            )
            for t in texts
        ]
    except _ParseObjectParseError:
        raise _ParseObjectParseError(type_=type_, text=text) from None


def _parse_object_set_type(
    type_: Any,
    text: str,
    /,
    *,
    list_separator: str = LIST_SEPARATOR,
    pair_separator: str = PAIR_SEPARATOR,
    brackets: Iterable[tuple[str, str]] | None = BRACKETS,
    head: bool = False,
    case_sensitive: bool = False,
    extra: ParseObjectExtra | None = None,
) -> set[Any]:
    inner_type = one(get_args(type_))
    try:
        inner_text = extract_group(r"^{(.*)}$", text, flags=DOTALL)
    except ExtractGroupError:
        raise _ParseObjectParseError(type_=type_, text=text) from None
    texts = split_str(inner_text, separator=list_separator)
    try:
        return {
            parse_object(
                inner_type,
                t,
                list_separator=list_separator,
                pair_separator=pair_separator,
                brackets=brackets,
                head=head,
                case_sensitive=case_sensitive,
                extra=extra,
            )
            for t in texts
        }
    except _ParseObjectParseError:
        raise _ParseObjectParseError(type_=type_, text=text) from None


def _parse_object_union_type(type_: Any, text: str, /) -> Any:
    if type_ is Number:
        try:
            return parse_number(text)
        except ParseNumberError:
            raise _ParseObjectParseError(type_=type_, text=text) from None
    if type_ is Duration:
        from utilities.whenever import ParseDurationError, parse_duration

        try:
            return parse_duration(text)
        except ParseDurationError:
            raise _ParseObjectParseError(type_=type_, text=text) from None
    raise _ParseObjectParseError(type_=type_, text=text) from None


def _parse_object_tuple_type(
    type_: Any,
    text: str,
    /,
    *,
    list_separator: str = LIST_SEPARATOR,
    pair_separator: str = PAIR_SEPARATOR,
    brackets: Iterable[tuple[str, str]] | None = BRACKETS,
    head: bool = False,
    case_sensitive: bool = False,
    extra: ParseObjectExtra | None = None,
) -> tuple[Any, ...]:
    args = get_args(type_)
    try:
        inner = extract_group(r"^\((.*)\)$", text, flags=DOTALL)
    except ExtractGroupError:
        raise _ParseObjectParseError(type_=type_, text=text) from None
    texts = inner.split(list_separator)
    if len(args) != len(texts):
        raise _ParseObjectParseError(type_=type_, text=text)
    try:
        return tuple(
            parse_object(
                arg,
                text,
                list_separator=list_separator,
                pair_separator=pair_separator,
                brackets=brackets,
                head=head,
                case_sensitive=case_sensitive,
                extra=extra,
            )
            for arg, text in zip(args, texts, strict=True)
        )
    except _ParseObjectParseError:
        raise _ParseObjectParseError(type_=type_, text=text) from None


@dataclass
class ParseObjectError(Exception):
    type_: Any
    text: str


@dataclass
class _ParseObjectParseError(ParseObjectError):
    @override
    def __str__(self) -> str:
        return f"Unable to parse {self.type_!r}; got {self.text!r}"


@dataclass
class _ParseObjectExtraNonUniqueError(ParseObjectError):
    first: type[Any]
    second: type[Any]

    @override
    def __str__(self) -> str:
        return f"Unable to parse {self.type_!r} since `extra` must contain exactly one parent class; got {self.first!r}, {self.second!r} and perhaps more"


##


def serialize_object(
    obj: Any,
    /,
    *,
    list_separator: str = LIST_SEPARATOR,
    pair_separator: str = PAIR_SEPARATOR,
    extra: SerializeObjectExtra | None = None,
) -> str:
    """Convert an object to text."""
    if extra is not None:
        with suppress(_SerializeObjectSerializeError):
            return _serialize_object_extra(obj, extra)
    if (obj is None) or isinstance(
        obj, bool | int | float | str | Path | Sentinel | Version
    ):
        return str(obj)
    if is_instance_gen(obj, dt.date):
        from utilities.whenever import serialize_date

        return serialize_date(obj)
    if is_instance_gen(obj, dt.datetime):
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
        return _serialize_object_dict(
            obj, list_separator=list_separator, pair_separator=pair_separator
        )
    if isinstance(obj, list):
        return _serialize_object_list(
            obj, list_separator=list_separator, pair_separator=pair_separator
        )
    if isinstance(obj, tuple):
        return _serialize_object_tuple(
            obj, list_separator=list_separator, pair_separator=pair_separator
        )
    if isinstance(obj, set | frozenset):
        return _serialize_object_set(
            obj, list_separator=list_separator, pair_separator=pair_separator
        )
    raise _SerializeObjectSerializeError(obj=obj)


def _serialize_object_dict(
    obj: Mapping[Any, Any],
    /,
    *,
    list_separator: str = LIST_SEPARATOR,
    pair_separator: str = PAIR_SEPARATOR,
) -> str:
    keys = (
        serialize_object(
            k, list_separator=list_separator, pair_separator=pair_separator
        )
        for k in obj
    )
    values = (
        serialize_object(
            v, list_separator=list_separator, pair_separator=pair_separator
        )
        for v in obj.values()
    )
    items = zip(keys, values, strict=True)
    joined_items = (join_strs(item, separator=pair_separator) for item in items)
    joined = join_strs(joined_items, separator=list_separator)
    return f"{{{joined}}}"


def _serialize_object_extra(obj: Any, extra: SerializeObjectExtra, /) -> str:
    try:
        serializer = one(
            s for c, s in extra.items() if (obj is c) or is_instance_gen(obj, c)
        )
    except OneEmptyError:
        raise _SerializeObjectSerializeError(obj=obj) from None
    except OneNonUniqueError as error:
        raise _SerializeObjectExtraNonUniqueError(
            obj=obj, first=error.first, second=error.second
        ) from None
    else:
        return serializer(obj)


def _serialize_object_list(
    obj: Sequence[Any],
    /,
    *,
    list_separator: str = LIST_SEPARATOR,
    pair_separator: str = PAIR_SEPARATOR,
) -> str:
    items = (
        serialize_object(
            i, list_separator=list_separator, pair_separator=pair_separator
        )
        for i in obj
    )
    joined = join_strs(items, separator=list_separator)
    return f"[{joined}]"


def _serialize_object_set(
    obj: AbstractSet[Any],
    /,
    *,
    list_separator: str = LIST_SEPARATOR,
    pair_separator: str = PAIR_SEPARATOR,
) -> str:
    items = (
        serialize_object(
            i, list_separator=list_separator, pair_separator=pair_separator
        )
        for i in obj
    )
    joined = join_strs(items, sort=True, separator=list_separator)
    return f"{{{joined}}}"


def _serialize_object_tuple(
    obj: tuple[Any, ...],
    /,
    *,
    list_separator: str = LIST_SEPARATOR,
    pair_separator: str = PAIR_SEPARATOR,
) -> str:
    items = (
        serialize_object(
            i, list_separator=list_separator, pair_separator=pair_separator
        )
        for i in obj
    )
    joined = join_strs(items, separator=list_separator)
    return f"({joined})"


@dataclass(kw_only=True, slots=True)
class SerializeObjectError(Exception):
    obj: Any


class _SerializeObjectSerializeError(SerializeObjectError):
    @override
    def __str__(self) -> str:
        return f"Unable to serialize object {self.obj!r} of type {type(self.obj)!r}"


@dataclass
class _SerializeObjectExtraNonUniqueError(SerializeObjectError):
    first: type[Any]
    second: type[Any]

    @override
    def __str__(self) -> str:
        return f"Unable to serialize object {self.obj!r} of type {type(self.obj)!r} since `extra` must contain exactly one parent class; got {self.first!r}, {self.second!r} and perhaps more"


__all__ = ["parse_object", "serialize_object"]
