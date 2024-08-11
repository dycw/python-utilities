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
from typing import Any, Generic, Literal, TypedDict, TypeVar, assert_never, cast
from uuid import UUID

from typing_extensions import override

from utilities.datetime import check_date_not_datetime, isinstance_date_not_datetime
from utilities.types import EnsureMemberError, ensure_member, get_class_name
from utilities.typing import get_args

_Class = Literal[
    "bytes",
    "complex",
    "date",
    "decimal",
    "dict",
    "fraction",
    "frozenset",
    "IPv4Address",
    "IPv6Address",
    "local_datetime",
    "Path",
    "set",
    "slice",
    "sqlalchemy.Engine",
    "time",
    "timedelta",
    "tuple",
    "UUID",
    "zoned_datetime",
]
_T = TypeVar("_T")
_CLASSES: tuple[_Class, ...] = get_args(_Class)
_CLASS = "CLASS"
_VALUE = "VALUE"


class _ClassValueMapping(Generic[_T], TypedDict):
    CLASS: _T
    VALUE: Any


_ExtraSer = Mapping[type[_T], tuple[str, Callable[[_T], Any]]]
_ExtraDes = Mapping[str, Callable[[Any], Any]]


def serialize(obj: Any, /, *, extra: _ExtraSer[Any] | None = None) -> str:
    """Serialize an object."""
    return dumps(_pre_process(obj), default=partial(_default, extra=extra))


@dataclass(kw_only=True, slots=True)
class _DictWrapper:
    value: dict[Any, Any]


@dataclass(kw_only=True, slots=True)
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


def _default(
    obj: Any, /, *, extra: _ExtraSer[Any] | None = None
) -> _ClassValueMapping[str]:
    """Extension for the JSON serializer."""
    if (result := _default_standard(obj)) is not None:
        return cast(_ClassValueMapping[str], result)
    if extra is not None:
        return _default_extra(obj, extra)
    raise JsonSerializationError(obj=obj)


def _default_standard(obj: Any, /) -> _ClassValueMapping[_Class] | None:
    """Extension for the JSON serializer."""
    if isinstance(obj, bytes):
        return {_CLASS: "bytes", _VALUE: obj.decode()}
    if isinstance(obj, complex):
        return {
            _CLASS: "complex",
            _VALUE: (_positive_zero(obj.real), _positive_zero(obj.imag)),
        }
    if isinstance(obj, Decimal):
        return {_CLASS: "decimal", _VALUE: str(obj)}
    if isinstance(obj, _DictWrapper):
        return _default_dict(obj)
    if isinstance_date_not_datetime(obj):
        return _default_date(obj)
    if isinstance(obj, dt.datetime) and (obj.tzinfo is None):
        return _default_local_datetime(obj)
    if isinstance(obj, dt.datetime) and (obj.tzinfo is not None):
        return _default_zoned_datetime(obj)
    if isinstance(obj, dt.time):
        return _default_time(obj)
    if isinstance(obj, dt.timedelta):
        return _default_timedelta(obj)
    if isinstance(obj, Fraction):
        return {_CLASS: "fraction", _VALUE: obj.as_integer_ratio()}
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
    if (result := _default_engine(obj)) is not None:
        return result
    return None


def _default_date(obj: dt.date, /) -> _ClassValueMapping[_Class]:
    from utilities.whenever import serialize_date

    check_date_not_datetime(obj)
    return {_CLASS: "date", _VALUE: serialize_date(obj)}


def _default_dict(obj: _DictWrapper, /) -> _ClassValueMapping[_Class]:
    try:
        value = sorted(obj.value.items(), key=itemgetter(0))
    except TypeError:
        value = list(obj.value.items())
    return {_CLASS: "dict", _VALUE: value}


def _default_engine(obj: Any, /) -> _ClassValueMapping[_Class] | None:
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


def _default_extra(obj: Any, extra: _ExtraSer[Any], /) -> _ClassValueMapping[str]:
    cls = type(obj)
    try:
        key, func = extra[cls]
    except KeyError:
        raise JsonSerializationError(obj=obj) from None
    return {_CLASS: key, _VALUE: func(obj)}


def _default_frozenset(obj: frozenset[Any], /) -> _ClassValueMapping[_Class]:
    try:
        value = sorted(obj)
    except TypeError:
        value = list(obj)
    return {_CLASS: "frozenset", _VALUE: value}


def _default_local_datetime(obj: dt.datetime, /) -> _ClassValueMapping[_Class]:
    from utilities.whenever import serialize_local_datetime

    return {_CLASS: "local_datetime", _VALUE: serialize_local_datetime(obj)}


def _default_set(obj: set[Any], /) -> _ClassValueMapping[_Class]:
    try:
        value = sorted(obj)
    except TypeError:
        value = list(obj)
    return {_CLASS: "set", _VALUE: value}


def _default_time(obj: dt.time, /) -> _ClassValueMapping[_Class]:
    from utilities.whenever import serialize_time

    return {_CLASS: "time", _VALUE: serialize_time(obj)}


def _default_timedelta(obj: dt.timedelta, /) -> _ClassValueMapping[_Class]:
    from utilities.whenever import serialize_timedelta

    return {_CLASS: "timedelta", _VALUE: serialize_timedelta(obj)}


def _default_zoned_datetime(obj: dt.datetime, /) -> _ClassValueMapping[_Class]:
    from utilities.whenever import serialize_zoned_datetime

    return {_CLASS: "zoned_datetime", _VALUE: serialize_zoned_datetime(obj)}


@dataclass(kw_only=True)
class JsonSerializationError(Exception):
    obj: Any

    @override
    def __str__(self) -> str:
        return f"Unsupported type: {get_class_name(self.obj)}"


def deserialize(text: str | bytes, /, *, extra: _ExtraDes | None = None) -> Any:
    return loads(text, object_hook=partial(_object_hook, extra=extra))


def _object_hook(mapping: Any, /, *, extra: _ExtraDes | None = None) -> Any:
    try:
        cls = mapping[_CLASS]
        value = mapping[_VALUE]
    except KeyError:
        return mapping
    try:
        cls = ensure_member(cls, _CLASSES)
    except EnsureMemberError:
        if extra is not None:
            try:
                func = extra[cls]
            except KeyError:
                raise _JsonDeserializationWithExtraTypeError(
                    cls=cls, extra=extra
                ) from None
            return func(value)
        raise _JsonDeserializationTypeError(cls=cls, value=value) from None
    match cls:
        case "bytes":
            return cast(str, value).encode()
        case "complex":
            return _object_hook_complex(value)
        case "date":
            return _object_hook_date(value)
        case "decimal":
            return Decimal(value)
        case "dict":
            return dict(value)
        case "fraction":
            return _object_hook_fraction(value)
        case "frozenset":
            return frozenset(value)
        case "IPv4Address":
            return IPv4Address(value)
        case "IPv6Address":
            return IPv6Address(value)
        case "local_datetime":
            return _object_hook_local_datetime(value)
        case "Path":
            return Path(value)
        case "set":
            return set(value)
        case "slice":
            return _object_hook_slice(value)
        case "time":
            return _object_hook_time(value)
        case "timedelta":
            return _object_hook_timedelta(value)
        case "tuple":
            return tuple(value)
        case "UUID":
            return UUID(value)
        case "zoned_datetime":
            return _object_hook_zoned_datetime(value)
        case "sqlalchemy.Engine":
            return _object_hook_sqlalchemy_engine(value)
        case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
            assert_never(never)


def _object_hook_complex(value: tuple[int, int], /) -> complex:
    real, imag = value
    return complex(real, imag)


def _object_hook_fraction(value: tuple[int, int], /) -> Fraction:
    numerator, denominator = value
    return Fraction(numerator=numerator, denominator=denominator)


def _object_hook_date(value: str, /) -> dt.date:
    from utilities.whenever import parse_date

    return parse_date(value)


def _object_hook_local_datetime(value: str, /) -> dt.date:
    from utilities.whenever import parse_local_datetime

    return parse_local_datetime(value)


def _object_hook_slice(value: tuple[int | None, int | None, int | None], /) -> slice:
    start, stop, step = value
    return slice(start, stop, step)


def _object_hook_sqlalchemy_engine(value: str, /) -> Any:
    from sqlalchemy import create_engine

    return create_engine(value)


def _object_hook_time(value: str, /) -> dt.time:
    from utilities.whenever import parse_time

    return parse_time(value)


def _object_hook_timedelta(value: str, /) -> dt.timedelta:
    from utilities.whenever import parse_timedelta

    return parse_timedelta(value)


def _object_hook_zoned_datetime(value: str, /) -> dt.date:
    from utilities.whenever import parse_zoned_datetime

    return parse_zoned_datetime(value)


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
