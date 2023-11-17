from __future__ import annotations

import datetime as dt
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from fractions import Fraction
from ipaddress import IPv4Address, IPv6Address
from json import dumps, loads
from pathlib import Path
from typing import Any, cast
from uuid import UUID

from utilities.datetime import (
    UTC,
    parse_date,
    parse_datetime,
    parse_time,
    parse_timedelta,
    serialize_date,
    serialize_datetime,
    serialize_time,
    serialize_timedelta,
)

_CLASS = "__class__"
_VALUE = "__value__"


def serialize(
    x: Any,
    /,
    *,
    skipkeys: bool = False,
    ensure_ascii: bool = True,
    check_circular: bool = True,
    allow_nan: bool = True,
    indent: int | str | None = None,
    separators: tuple[str, str] | None = None,
    sort_keys: bool = False,
    **kwargs: Any,
) -> str:
    """Serialize an object."""
    return dumps(
        _pre_process(x),
        skipkeys=skipkeys,
        ensure_ascii=ensure_ascii,
        check_circular=check_circular,
        allow_nan=allow_nan,
        indent=indent,
        separators=separators,
        default=_default,
        sort_keys=sort_keys,
        **kwargs,
    )


@dataclass
class _DictWrapper:
    value: dict[Any, Any]


@dataclass
class _TupleWrapper:
    value: tuple[Any, ...]


def _pre_process(obj: Any) -> Any:
    if isinstance(obj, float):
        return _positive_zero(obj)
    if isinstance(obj, dict):
        return _DictWrapper(obj)
    if isinstance(obj, tuple):
        return _TupleWrapper(obj)
    return obj


def _positive_zero(x: float, /) -> float:
    return abs(x) if x == 0.0 else x  # noqa: PLR2004


def _default(obj: Any, /) -> Any:  # noqa: PLR0911, PLR0912
    """Extension for the JSON serializer."""
    if isinstance(obj, bytes):
        return {_CLASS: "bytes", _VALUE: obj.decode()}
    if isinstance(obj, complex):
        return {
            _CLASS: "complex",
            _VALUE: (_positive_zero(obj.real), _positive_zero(obj.imag)),
        }
    if isinstance(obj, Decimal):
        return {_CLASS: "Decimal", _VALUE: str(obj)}
    if isinstance(obj, _DictWrapper):
        return {_CLASS: "dict", _VALUE: list(obj.value.items())}
    if isinstance(obj, dt.date) and not isinstance(obj, dt.datetime):
        return {_CLASS: "dt.date", _VALUE: serialize_date(obj)}
    if isinstance(obj, dt.datetime):
        if (tzinfo := obj.tzinfo) is None:
            return {_CLASS: "dt.datetime|naive", _VALUE: obj.isoformat()}
        if tzinfo is UTC:
            return {_CLASS: "dt.datetime|UTC", _VALUE: serialize_datetime(obj)}
        msg = f"{tzinfo=}"
        raise InvalidTimeZoneError(msg)
    if isinstance(obj, dt.time):
        return {_CLASS: "dt.time", _VALUE: serialize_time(obj)}
    if isinstance(obj, dt.timedelta):
        return {_CLASS: "dt.timedelta", _VALUE: serialize_timedelta(obj)}
    if isinstance(obj, Fraction):
        return {_CLASS: "Fraction", _VALUE: obj.as_integer_ratio()}
    if isinstance(obj, frozenset):
        return {_CLASS: "frozenset", _VALUE: list(obj)}
    if isinstance(obj, IPv4Address):
        return {_CLASS: "IPv4Address", _VALUE: str(obj)}
    if isinstance(obj, IPv6Address):
        return {_CLASS: "IPv6Address", _VALUE: str(obj)}
    if isinstance(obj, Path):
        return {_CLASS: "Path", _VALUE: str(obj)}
    if isinstance(obj, set):
        return {_CLASS: "set", _VALUE: list(obj)}
    if isinstance(obj, slice):
        return {_CLASS: "slice", _VALUE: (obj.start, obj.stop, obj.step)}
    if isinstance(obj, _TupleWrapper):
        return {_CLASS: "tuple", _VALUE: list(obj.value)}
    if isinstance(obj, UUID):
        return {_CLASS: "UUID", _VALUE: str(obj)}
    msg = f"{type(obj)=}"
    raise JsonSerializationError(msg)


class InvalidTimeZoneError(ValueError):
    """Raised when an invalid timezone is encountered."""


class JsonSerializationError(ValueError):
    """Raised when an object cannot be serialized."""


def deserialize(text: str | bytes, /) -> Any:
    return loads(text, object_hook=_object_hook)


def _object_hook(mapping: Mapping[str, Any], /) -> Any:  # noqa: PLR0911
    try:
        cls = mapping[_CLASS]
    except KeyError:
        return mapping
    value = mapping[_VALUE]
    match cls:
        case "bytes":
            value = cast(str, value)
            return value.encode()
        case "complex":
            value = cast(list[int], value)
            real, imag = value
            return complex(real, imag)
        case "Decimal":
            value = cast(str, value)
            return Decimal(value)
        case "dict":
            value = cast(list[list[Any]], value)
            return dict(value)
        case "dt.date":
            value = cast(str, value)
            return parse_date(value)
        case "dt.datetime|naive":
            value = cast(str, value)
            return dt.datetime.fromisoformat(value)
        case "dt.datetime|UTC":
            value = cast(str, value)
            return parse_datetime(value)
        case "dt.time":
            value = cast(str, value)
            return parse_time(value)
        case "dt.timedelta":
            value = cast(str, value)
            return parse_timedelta(value)
        case "Fraction":
            value = cast(list[int], value)
            numerator, denominator = value
            return Fraction(numerator=numerator, denominator=denominator)
        case "frozenset":
            value = cast(list[Any], value)
            return frozenset(value)
        case "IPv4Address":
            value = cast(str, value)
            return IPv4Address(value)
        case "IPv6Address":
            value = cast(str, value)
            return IPv6Address(value)
        case "Path":
            value = cast(str, value)
            return Path(value)
        case "set":
            value = cast(list[Any], value)
            return set(value)
        case "slice":
            value = cast(list[int | None], value)
            start, stop, step = value
            return slice(start, stop, step)
        case "tuple":
            value = cast(list[Any], value)
            return tuple(value)
        case "UUID":
            value = cast(str, value)
            return UUID(value)
        case _:
            msg = f"{cls=}, {value=}"
            raise JsonDeserializationError(msg)


class JsonDeserializationError(ValueError):
    """Raised when an object cannot be deserialized."""


__all__ = ["deserialize", "serialize"]
