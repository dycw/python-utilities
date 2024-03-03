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
from utilities.errors import redirect_error
from utilities.types import get_class_name

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


def _default(  # noqa: PLR0911, PLR0912
    obj: Any, /, *, extra: _ExtraSer[Any] | None = None
) -> Any:
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
        try:
            value = sorted(obj.value.items(), key=itemgetter(0))
        except TypeError:
            value = list(obj.value.items())
        return {_CLASS: "dict", _VALUE: value}
    if isinstance(obj, dt.date) and not isinstance(obj, dt.datetime):
        return {_CLASS: "dt.date", _VALUE: serialize_date(obj)}
    if isinstance(obj, dt.datetime):
        if (tzinfo := obj.tzinfo) is None:
            return {_CLASS: "dt.datetime|naive", _VALUE: obj.isoformat()}
        if tzinfo is UTC:
            return {_CLASS: "dt.datetime|UTC", _VALUE: serialize_datetime(obj)}
        raise _JsonSerializationTimeZoneError(obj=obj, tzinfo=tzinfo)
    if isinstance(obj, dt.time):
        return {_CLASS: "dt.time", _VALUE: serialize_time(obj)}
    if isinstance(obj, dt.timedelta):
        return {_CLASS: "dt.timedelta", _VALUE: serialize_timedelta(obj)}
    if isinstance(obj, Fraction):
        return {_CLASS: "Fraction", _VALUE: obj.as_integer_ratio()}
    if isinstance(obj, frozenset):
        try:
            value = sorted(obj)
        except TypeError:
            value = list(obj)
        return {_CLASS: "frozenset", _VALUE: value}
    if isinstance(obj, IPv4Address):
        return {_CLASS: "IPv4Address", _VALUE: str(obj)}
    if isinstance(obj, IPv6Address):
        return {_CLASS: "IPv6Address", _VALUE: str(obj)}
    if isinstance(obj, Path):
        return {_CLASS: "Path", _VALUE: str(obj)}
    if isinstance(obj, set):
        try:
            value = sorted(obj)
        except TypeError:
            value = list(obj)
        return {_CLASS: "set", _VALUE: value}
    if isinstance(obj, slice):
        return {_CLASS: "slice", _VALUE: (obj.start, obj.stop, obj.step)}
    if isinstance(obj, _TupleWrapper):
        return {_CLASS: "tuple", _VALUE: list(obj.value)}
    if isinstance(obj, UUID):
        return {_CLASS: "UUID", _VALUE: str(obj)}
    if extra is not None:
        cls = type(obj)
        try:
            key, func = extra[cls]
        except KeyError:
            raise _JsonSerializationTypeError(obj=obj) from None
        return {_CLASS: key, _VALUE: func(obj)}
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
    raise _JsonSerializationTypeError(obj=obj)


@dataclass(kw_only=True)
class JsonSerializationError(Exception):
    obj: Any


@dataclass(kw_only=True)
class _JsonSerializationTimeZoneError(JsonSerializationError):
    tzinfo: dt.tzinfo

    @override
    def __str__(self) -> str:
        return f"Invalid timezone: {self.tzinfo}."


@dataclass(kw_only=True)
class _JsonSerializationTypeError(JsonSerializationError):
    @override
    def __str__(self) -> str:
        return f"Unsupported type: {get_class_name(self.obj)}."


def deserialize(text: str | bytes, /, *, extra: _ExtraDes | None = None) -> Any:
    return loads(text, object_hook=partial(_object_hook, extra=extra))


def _object_hook(  # noqa: PLR0911
    mapping: Mapping[str, Any], /, *, extra: _ExtraDes | None = None
) -> Any:
    try:
        cls = cast(str, mapping[_CLASS])
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
        case "sqlalchemy.Engine":
            from sqlalchemy import create_engine

            value = cast(str, value)
            return create_engine(value)
        case _:
            if extra is not None:
                with redirect_error(
                    KeyError, JsonDeserializationError(f"{cls=}, {extra=}")
                ):
                    func = extra[cls]
                return func(value)
            msg = f"{cls=}, {value=}"
            raise JsonDeserializationError(msg)


class JsonDeserializationError(Exception): ...


__all__ = ["deserialize", "serialize"]
