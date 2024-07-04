from __future__ import annotations

import datetime as dt
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from fractions import Fraction
from functools import partial
from ipaddress import IPv4Address, IPv6Address
from json import dumps, loads
from operator import itemgetter
from pathlib import Path
from typing import Any, TypeVar, cast
from uuid import UUID

from typing_extensions import override

from utilities.datetime import (
    parse_date,
    parse_datetime,
    parse_time,
    parse_timedelta,
    serialize_date,
    serialize_datetime,
    serialize_time,
    serialize_timedelta,
)
from utilities.types import get_class_name
from utilities.zoneinfo import UTC

_T = TypeVar("_T")
_ExtraSer = Mapping[type[_T], tuple[str, Callable[[_T], Any]]]
_ExtraDes = Mapping[str, Callable[[Any], Any]]
_CLASS = "__class__"
_VALUE = "__value__"


def serialize(obj: Any, /, *, extra: _ExtraSer[Any] | None = None) -> str:
    """Serialize an object."""
    return dumps(_pre_process(obj), default=partial(_default, extra=extra))


@dataclass(kw_only=True)
class _DictWrapper:
    value: dict[Any, Any]


@dataclass(kw_only=True)
class _TupleWrapper:
    value: tuple[Any, ...]


def _pre_process(obj: Any) -> Any:
    if isinstance(obj, float):
        return _positive_zero(obj)
    if isinstance(obj, dict):
        return _DictWrapper(value=obj)
    if isinstance(obj, tuple):
        return _TupleWrapper(value=obj)
    return obj


def _positive_zero(x: float, /) -> float:
    return abs(x) if x == 0.0 else x


def _default(obj: Any, /, *, extra: _ExtraSer[Any] | None = None) -> Any:
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
        return _default_dict(obj)
    if isinstance(obj, dt.date) and not isinstance(obj, dt.datetime):
        return {_CLASS: "dt.date", _VALUE: serialize_date(obj)}
    if isinstance(obj, dt.datetime):
        return _default_datetime(obj)
    if isinstance(obj, dt.time):
        return {_CLASS: "dt.time", _VALUE: serialize_time(obj)}
    if isinstance(obj, dt.timedelta):
        return {_CLASS: "dt.timedelta", _VALUE: serialize_timedelta(obj)}
    if isinstance(obj, Fraction):
        return {_CLASS: "Fraction", _VALUE: obj.as_integer_ratio()}
    if isinstance(obj, frozenset):
        return _default_frozenset(obj)
    if isinstance(obj, IPv4Address):
        return {_CLASS: "IPv4Address", _VALUE: str(obj)}
    if isinstance(obj, IPv6Address):
        return {_CLASS: "IPv6Address", _VALUE: str(obj)}
    if isinstance(obj, Path):
        return {_CLASS: "Path", _VALUE: str(obj)}
    if isinstance(obj, set) and not isinstance(obj, frozenset):
        return _default_set(obj)
    if isinstance(obj, slice):
        return {_CLASS: "slice", _VALUE: (obj.start, obj.stop, obj.step)}
    if isinstance(obj, _TupleWrapper):
        return {_CLASS: "tuple", _VALUE: list(obj.value)}
    if isinstance(obj, UUID):
        return {_CLASS: "UUID", _VALUE: str(obj)}
    if extra is not None:
        return _default_extra(obj, extra)
    if (result := _default_engine(obj)) is not None:
        return result
    raise _JsonSerializationTypeError(obj=obj)


def _default_datetime(obj: dt.datetime, /) -> Any:
    if (tzinfo := obj.tzinfo) is None:
        return {_CLASS: "dt.datetime|naive", _VALUE: obj.isoformat()}
    if tzinfo is UTC:
        return {_CLASS: "dt.datetime|UTC", _VALUE: serialize_datetime(obj)}
    raise _JsonSerializationTimeZoneError(obj=obj, tzinfo=tzinfo)


def _default_dict(obj: _DictWrapper, /) -> Any:
    try:
        value = sorted(obj.value.items(), key=itemgetter(0))
    except TypeError:
        value = list(obj.value.items())
    return {_CLASS: "dict", _VALUE: value}


def _default_engine(obj: Any, /) -> Any:
    try:
        from sqlalchemy import Engine
    except ModuleNotFoundError:  # pragma: no cover
        pass
    else:
        if isinstance(obj, Engine):
            return {
                _CLASS: "sqlalchemy.Engine",
                _VALUE: obj.url.render_as_string(hide_password=False),
            }
    return None


def _default_extra(obj: Any, extra: _ExtraSer[Any], /) -> Any:
    cls = type(obj)
    try:
        key, func = extra[cls]
    except KeyError:
        raise _JsonSerializationTypeError(obj=obj) from None
    return {_CLASS: key, _VALUE: func(obj)}


def _default_frozenset(obj: frozenset[Any], /) -> Any:
    try:
        value = sorted(obj)
    except TypeError:
        value = list(obj)
    return {_CLASS: "frozenset", _VALUE: value}


def _default_set(obj: set[Any], /) -> Any:
    try:
        value = sorted(obj)
    except TypeError:
        value = list(obj)
    return {_CLASS: "set", _VALUE: value}


@dataclass(kw_only=True)
class JsonSerializationError(Exception):
    obj: Any


@dataclass(kw_only=True)
class _JsonSerializationTimeZoneError(JsonSerializationError):
    tzinfo: dt.tzinfo

    @override
    def __str__(self) -> str:
        return f"Invalid timezone: {self.tzinfo}"


@dataclass(kw_only=True)
class _JsonSerializationTypeError(JsonSerializationError):
    @override
    def __str__(self) -> str:
        return f"Unsupported type: {get_class_name(self.obj)}"


def deserialize(text: str | bytes, /, *, extra: _ExtraDes | None = None) -> Any:
    return loads(text, object_hook=partial(_object_hook, extra=extra))


def _object_hook(
    mapping: Mapping[str, Any], /, *, extra: _ExtraDes | None = None
) -> Any:
    try:
        cls = cast(str, mapping[_CLASS])
    except KeyError:
        return mapping
    value = mapping[_VALUE]
    match cls:
        case "bytes":
            return cast(str, value).encode()
        case "complex":
            real, imag = cast(list[int], value)
            return complex(real, imag)
        case "Decimal":
            return Decimal(cast(str, value))
        case "dict":
            return dict(cast(list[list[Any]], value))
        case "dt.date":
            return parse_date(cast(str, value))
        case "dt.datetime|naive":
            return dt.datetime.fromisoformat(cast(str, value))
        case "dt.datetime|UTC":
            return parse_datetime(cast(str, value))
        case "dt.time":
            return parse_time(cast(str, value))
        case "dt.timedelta":
            return parse_timedelta(cast(str, value))
        case "Fraction":
            numerator, denominator = cast(list[int], value)
            return Fraction(numerator=numerator, denominator=denominator)
        case "frozenset":
            return frozenset(cast(list[Any], value))
        case "IPv4Address":
            return IPv4Address(cast(str, value))
        case "IPv6Address":
            return IPv6Address(cast(str, value))
        case "Path":
            return Path(cast(str, value))
        case "set":
            return set(cast(list[Any], value))
        case "slice":
            start, stop, step = cast(list[int | None], value)
            return slice(start, stop, step)
        case "tuple":
            return tuple(cast(list[Any], value))
        case "UUID":
            return UUID(cast(str, value))
        case "sqlalchemy.Engine":
            from sqlalchemy import create_engine

            return create_engine(cast(str, value))
        case _:
            if extra is not None:
                try:
                    func = extra[cls]
                except KeyError:
                    raise _JsonDeserializationWithExtraTypeError(
                        cls=cls, extra=extra
                    ) from None
                return func(value)
            raise _JsonDeserializationTypeError(cls=cls, value=value)


@dataclass(kw_only=True)
class JsonDeserializationError(Exception):
    cls: str


@dataclass(kw_only=True)
class _JsonDeserializationWithExtraTypeError(JsonDeserializationError):
    extra: _ExtraDes

    @override
    def __str__(self) -> str:
        return f"Unsupported type: {self.cls}; extras were {set(self.extra)}"


@dataclass(kw_only=True)
class _JsonDeserializationTypeError(JsonDeserializationError):
    value: Any

    @override
    def __str__(self) -> str:
        return f"Unsupported type: {self.cls}; value was {self.value}"


__all__ = ["JsonDeserializationError", "deserialize", "serialize"]
