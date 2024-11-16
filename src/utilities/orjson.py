from __future__ import annotations

import datetime as dt
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from decimal import Decimal
from enum import Enum, StrEnum, unique
from fractions import Fraction
from functools import partial
from ipaddress import IPv4Address, IPv6Address
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    TypedDict,
    TypeVar,
    _LiteralGenericAlias,
    cast,
    overload,
)

from dacite import WrongTypeError
from orjson import (
    OPT_NON_STR_KEYS,
    OPT_PASSTHROUGH_DATETIME,
    OPT_SORT_KEYS,
    dumps,
    loads,
)
from typing_extensions import override

from utilities.dataclasses import is_dataclass_instance
from utilities.functions import get_class_name
from utilities.iterables import OneError, one
from utilities.math import MAX_INT64, MIN_INT64
from utilities.timer import Timer
from utilities.typing import get_args, is_namedtuple_class, is_namedtuple_instance

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable


_T = TypeVar("_T")
_SCHEMA_KEY = "_k"
_SCHEMA_VALUE = "_v"


@unique
class _Key(StrEnum):
    any = "any"
    bytes = "byt"
    complex = "cmp"
    date = "dat"
    decimal = "dec"
    fraction = "frc"
    frozenset = "frz"
    ipv4_address = "ip4"
    ipv6_address = "ip6"
    local_datetime = "ldt"
    path = "pth"
    set = "set"
    slice = "slc"
    time = "tim"
    timedelta = "td"
    zoned_datetime = "zdt"


def serialize(obj: Any, /, *, fallback: bool = False) -> bytes:
    """Serialize an object."""
    if is_dataclass_instance(obj):
        obj_use = asdict(obj)
    elif is_namedtuple_instance(obj):
        obj_use = tuple(obj)
    elif isinstance(obj, bytes):
        if not _is_serializable_binary(obj):
            raise _SerializeInvalidBinaryError(obj=obj)
        obj_use = obj
    elif isinstance(obj, Fraction):
        if not _is_serializable_fraction(obj):
            raise _SerializeInvalidFractionError(obj=obj)
        obj_use = obj
    else:
        obj_use = obj
    try:
        return dumps(
            obj_use,
            default=partial(_serialize_default, fallback=fallback),
            option=OPT_NON_STR_KEYS | OPT_PASSTHROUGH_DATETIME | OPT_SORT_KEYS,
        )
    except TypeError:
        raise _SerializeTypeError(obj=obj) from None


def _is_serializable_binary(binary: bytes, /) -> bool:
    try:
        _ = binary.decode()
    except UnicodeDecodeError:
        return False
    return True


def _is_serializable_fraction(frac: Fraction, /) -> bool:
    return (MIN_INT64 <= frac.numerator <= MAX_INT64) and (
        MIN_INT64 <= frac.denominator <= MAX_INT64
    )


@dataclass(kw_only=True, slots=True)
class SerializeError(Exception):
    obj: Any


@dataclass(kw_only=True, slots=True)
class _SerializeTypeError(SerializeError):
    @override
    def __str__(self) -> str:
        cls = get_class_name(self.obj)
        return f"Unable to serialize object of type {cls!r}"


@dataclass(kw_only=True, slots=True)
class _SerializeInvalidBinaryError(SerializeError):
    @override
    def __str__(self) -> str:
        return f"Unable to serialize binary data {self.obj!r}"


@dataclass(kw_only=True, slots=True)
class _SerializeInvalidFractionError(SerializeError):
    @override
    def __str__(self) -> str:
        return f"Unable to serialize fraction {self.obj!r}"


class _SchemaDict(Generic[_T], TypedDict):
    _k: _Key
    _v: _T


def _serialize_default(obj: Any, /, *, fallback: bool = False) -> _SchemaDict:
    schema = _get_schema(obj, fallback=fallback)
    return {_SCHEMA_KEY: schema.key, _SCHEMA_VALUE: schema.serializer(obj)}


@dataclass(kw_only=True, slots=True)
class _Schema(Generic[_T]):
    key: _Key
    serializer: Callable[[_T], Any]


def _get_schema(obj: _T, /, *, fallback: bool = False) -> _Schema[_T]:
    # standard library
    if isinstance(obj, bytes):
        return cast(_Schema[_T], _get_schema_bytes())
    if isinstance(obj, complex):
        return cast(_Schema[_T], _get_schema_complex())
    if isinstance(obj, dt.date) and not isinstance(obj, dt.datetime):
        return cast(_Schema[_T], _get_schema_date())
    if isinstance(obj, dt.datetime) and (obj.tzinfo is None):
        return cast(_Schema[_T], _get_schema_local_datetime())
    if isinstance(obj, dt.datetime) and (obj.tzinfo is not None):
        return cast(_Schema[_T], _get_schema_zoned_datetime())  # skipif-ci-and-windows
    if isinstance(obj, Decimal):
        return cast(_Schema[_T], _get_schema_decimal())
    if isinstance(obj, Fraction):
        return cast(_Schema[_T], _get_schema_fraction())
    if isinstance(obj, IPv4Address):
        return cast(_Schema[_T], _get_schema_ipv4adress())
    if isinstance(obj, IPv6Address):
        return cast(_Schema[_T], _get_schema_ipv6adress())
    if isinstance(obj, Path):
        return cast(_Schema[_T], _get_schema_path())
    if isinstance(obj, slice):
        return cast(_Schema[_T], _get_schema_slice())
    if isinstance(obj, dt.time):
        return cast(_Schema[_T], _get_schema_time())
    if isinstance(obj, dt.timedelta):
        return cast(_Schema[_T], _get_schema_timedelta())
    # collections
    if isinstance(obj, frozenset):
        return cast(_Schema[_T], _get_schema_frozenset())
    if isinstance(obj, set):
        return cast(_Schema[_T], _get_schema_set())
    # first party
    if isinstance(obj, Timer):
        return cast(_Schema[_T], _get_schema_timer())
    # fallback
    if fallback:
        return cast(_Schema[_T], _get_schema_fallback())
    raise _GetSchemaError(obj=obj)


def _get_schema_bytes() -> _Schema[bytes]:
    return _Schema(key=_Key.bytes, serializer=lambda b: b.decode())


def _get_schema_complex() -> _Schema[complex]:
    return _Schema(key=_Key.complex, serializer=lambda c: (c.real, c.imag))


def _get_schema_date() -> _Schema[dt.date]:
    from utilities.whenever import serialize_date

    return _Schema(key=_Key.date, serializer=serialize_date)


def _get_schema_decimal() -> _Schema[Decimal]:
    return _Schema(key=_Key.decimal, serializer=str)


def _get_schema_fallback() -> _Schema[Any]:
    return _Schema(key=_Key.any, serializer=str)


def _get_schema_fraction() -> _Schema[Fraction]:
    return _Schema(key=_Key.fraction, serializer=lambda f: (f.numerator, f.denominator))


def _get_schema_frozenset() -> _Schema[frozenset[Any]]:
    def serializer(obj: frozenset[_T], /) -> list[_T]:
        try:
            return sorted(cast(Any, obj))
        except TypeError:
            return list(obj)

    return _Schema(key=_Key.frozenset, serializer=serializer)


def _get_schema_ipv4adress() -> _Schema[IPv4Address]:
    return _Schema(key=_Key.ipv4_address, serializer=str)


def _get_schema_ipv6adress() -> _Schema[IPv6Address]:
    return _Schema(key=_Key.ipv6_address, serializer=str)


def _get_schema_local_datetime() -> _Schema[dt.datetime]:
    from utilities.whenever import serialize_local_datetime

    return _Schema(key=_Key.local_datetime, serializer=serialize_local_datetime)


def _get_schema_path() -> _Schema[Path]:
    return _Schema(key=_Key.path, serializer=str)


def _get_schema_set() -> _Schema[set[Any]]:
    def serializer(obj: set[_T], /) -> list[_T]:
        try:
            return sorted(cast(Any, obj))
        except TypeError:
            return list(obj)

    return _Schema(key=_Key.set, serializer=serializer)


def _get_schema_slice() -> _Schema[slice]:
    return _Schema(key=_Key.slice, serializer=lambda s: (s.start, s.stop, s.step))


def _get_schema_time() -> _Schema[dt.time]:
    from utilities.whenever import serialize_time

    return _Schema(key=_Key.time, serializer=serialize_time)


def _get_schema_timer() -> _Schema[Timer]:
    from utilities.whenever import serialize_timedelta

    def serializer(obj: Timer, /) -> str:
        return serialize_timedelta(obj.timedelta)

    return _Schema(key=_Key.timedelta, serializer=serializer)


def _get_schema_timedelta() -> _Schema[dt.timedelta]:
    from utilities.whenever import serialize_timedelta

    return _Schema(key=_Key.timedelta, serializer=serialize_timedelta)


def _get_schema_zoned_datetime() -> _Schema[dt.datetime]:
    from utilities.whenever import serialize_zoned_datetime  # skipif-ci-and-windows

    return _Schema(  # skipif-ci-and-windows
        key=_Key.zoned_datetime, serializer=serialize_zoned_datetime
    )


@dataclass(kw_only=True, slots=True)
class _GetSchemaError(Exception):
    obj: Any

    @override
    def __str__(self) -> str:
        return f"Unsupported type: {get_class_name(self.obj)!r}"  # pragma: no cover


@overload
def deserialize(
    data: bytes,
    /,
    *,
    cls: type[_T],
    enum_subsets: Iterable[_LiteralGenericAlias] | None = ...,
) -> _T: ...
@overload
def deserialize(
    data: bytes,
    /,
    *,
    cls: None = ...,
    enum_subsets: Iterable[_LiteralGenericAlias] | None = ...,
) -> Any: ...
def deserialize(
    data: bytes,
    /,
    *,
    cls: type[_T] | None = None,
    enum_subsets: Iterable[_LiteralGenericAlias] | None = None,
) -> Any:
    """Deserialize an object."""
    pre_obj = loads(data)
    try:
        obj = _object_hook(pre_obj)
    except _ObjectHookError as error:
        raise DeserializeError(data=error.data, obj=pre_obj) from None
    if cls is None:
        return obj
    if is_namedtuple_class(cls):
        return cls(*obj)
    from dacite import Config, from_dict

    if enum_subsets is None:
        type_hooks = {}
    else:
        type_hooks = {es: _make_type_hook(es) for es in enum_subsets}
    return from_dict(
        cls,
        obj,
        config=Config(
            type_hooks=type_hooks, cast=[Enum], forward_references={"Mapping": Mapping}
        ),
    )


def _object_hook(obj: Any, /) -> Any:
    """Object hook for deserialization."""
    if not isinstance(obj, dict):
        return obj
    if set(obj) != {_SCHEMA_KEY, _SCHEMA_VALUE}:
        return {k: _object_hook(v) for k, v in obj.items()}
    schema: _SchemaDict[Any] = cast(Any, obj)
    value = schema[_SCHEMA_VALUE]
    match schema[_SCHEMA_KEY]:
        # standard library
        case _Key.bytes:
            return _object_hook_bytes(value)
        case _Key.complex:
            return _object_hook_complex(value)
        case _Key.date:
            return _object_hook_date(value)
        case _Key.decimal:
            return _object_hook_decimal(value)
        case _Key.fraction:
            return _object_hook_fraction(value)
        case _Key.ipv4_address:
            return _object_hook_ipv4_address(value)
        case _Key.ipv6_address:
            return _object_hook_ipv6_address(value)
        case _Key.local_datetime:
            return _object_hook_local_datetime(value)
        case _Key.path:
            return _object_hook_path(value)
        case _Key.slice:
            return _object_hook_slice(value)
        case _Key.time:
            return _object_hook_time(value)
        case _Key.timedelta:
            return _object_hook_timedelta(value)
        case _Key.zoned_datetime:
            return _object_hook_zoned_datetime(value)  # skipif-ci-and-windows
        # collections
        case _Key.frozenset:
            return _object_hook_frozenset(value)
        case _Key.set:
            return _object_hook_set(value)
        # fallback
        case _Key.any:
            return _object_hook_fallback(value)
        # never
        case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
            raise _ObjectHookError(data=never)


@dataclass(kw_only=True, slots=True)
class DeserializeError(Exception):
    data: bytes
    obj: Any

    @override
    def __str__(self) -> str:
        return f"Unable to deserialize data {self.data!r}; object hook failed on {self.obj!r}"


def _object_hook_bytes(value: str, /) -> bytes:
    return value.encode()


def _object_hook_complex(value: tuple[int, int], /) -> complex:
    real, imag = value
    return complex(real, imag)


def _object_hook_date(value: str, /) -> dt.date:
    from utilities.whenever import parse_date

    return parse_date(value)


def _object_hook_decimal(value: str, /) -> Decimal:
    return Decimal(value)


def _object_hook_fallback(value: str, /) -> str:
    return value


def _object_hook_fraction(value: tuple[int, int], /) -> Fraction:
    numerator, denominator = value
    return Fraction(numerator=numerator, denominator=denominator)


def _object_hook_frozenset(value: list[_T], /) -> frozenset[_T]:
    return frozenset(value)


def _object_hook_ipv4_address(value: str, /) -> IPv4Address:
    return IPv4Address(value)


def _object_hook_ipv6_address(value: str, /) -> IPv6Address:
    return IPv6Address(value)


def _object_hook_local_datetime(value: str, /) -> dt.date:
    from utilities.whenever import parse_local_datetime

    return parse_local_datetime(value)


def _object_hook_path(value: str, /) -> Path:
    return Path(value)


def _object_hook_set(value: list[_T], /) -> set[_T]:
    return set(value)


def _object_hook_slice(value: tuple[int | None, int | None, int | None], /) -> slice:
    start, stop, step = value
    return slice(start, stop, step)


def _object_hook_time(value: str, /) -> dt.time:
    from utilities.whenever import parse_time

    return parse_time(value)


def _object_hook_timedelta(value: str, /) -> dt.timedelta:
    from utilities.whenever import parse_timedelta

    return parse_timedelta(value)


def _object_hook_zoned_datetime(value: str, /) -> dt.date:
    from utilities.whenever import parse_zoned_datetime  # skipif-ci-and-windows

    return parse_zoned_datetime(value)  # skipif-ci-and-windows


@dataclass(kw_only=True, slots=True)
class _ObjectHookError(Exception):
    data: Any

    @override
    def __str__(self) -> str:
        return f"Unable to cast to object: {self.data!r}"


def _make_type_hook(enum_subset: _LiteralGenericAlias, /) -> Callable[[Any], Enum]:
    def hook(value: Any, /) -> Enum:
        members = cast(tuple[Enum, ...], get_args(enum_subset))
        try:
            return one(e for e in members if e.value == value)
        except OneError:
            raise WrongTypeError(value, enum_subset) from None

    return hook


__all__ = ["DeserializeError", "SerializeError", "deserialize", "serialize"]
