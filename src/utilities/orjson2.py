from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass
from enum import Enum, unique
from functools import partial
from pathlib import Path
from re import Match, Pattern
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    Never,
    assert_never,
    cast,
)

from orjson import (
    OPT_PASSTHROUGH_DATACLASS,
    OPT_PASSTHROUGH_DATETIME,
    OPT_SORT_KEYS,
    dumps,
    loads,
)
from typing_extensions import override

from utilities.dataclasses import Dataclass, asdict_without_defaults
from utilities.functions import get_class_name
from utilities.iterables import OneEmptyError, one
from utilities.math import MAX_INT64, MIN_INT64
from utilities.whenever import (
    parse_date,
    parse_local_datetime,
    parse_time,
    parse_timedelta,
    parse_zoned_datetime,
    serialize_date,
    serialize_local_datetime,
    serialize_time,
    serialize_timedelta,
    serialize_zoned_datetime,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from collections.abc import Set as AbstractSet

    from utilities.types import StrMapping


@unique
class _Prefixes(Enum):
    dataclass = "dc"
    date = "d"
    datetime = "dt"
    frozenset_ = "f"
    list_subclass = "l"
    path = "p"
    set_ = "s"
    timedelta = "td"
    time = "tm"
    tuple_ = "tu"


def serialize2(
    obj: Any,
    /,
    *,
    before: Callable[[Any], Any] | None = None,
    after: Callable[[Any], Any] | None = None,
    dataclass_final_hook: Callable[[type[Dataclass], StrMapping], StrMapping]
    | None = None,
    fallback: bool = False,
) -> bytes:
    """Serialize an object."""
    obj_use = _pre_process(
        obj, before=before, after=after, dataclass_final_hook=dataclass_final_hook
    )
    try:
        return dumps(
            obj_use,
            default=partial(_serialize2_default, fallback=fallback),
            option=OPT_PASSTHROUGH_DATACLASS | OPT_PASSTHROUGH_DATETIME | OPT_SORT_KEYS,
        )
    except TypeError:
        raise _Serialize2TypeError(obj=obj) from None


def _pre_process(
    obj: Any,
    /,
    *,
    before: Callable[[Any], Any] | None = None,
    after: Callable[[Any], Any] | None = None,
    dataclass_final_hook: Callable[[type[Dataclass], StrMapping], StrMapping]
    | None = None,
) -> Any:
    if before is not None:
        obj = before(obj)
    pre = partial(
        _pre_process,
        before=before,
        after=after,
        dataclass_final_hook=dataclass_final_hook,
    )
    match obj:
        case int():
            if not (MIN_INT64 <= obj <= MAX_INT64):
                raise _Serialize2IntegerError(obj=obj)
        case dict():
            return {k: pre(v) for k, v in obj.items()}
        case frozenset():
            return _pre_process_container(
                obj,
                frozenset,
                _Prefixes.frozenset_,
                before=before,
                after=after,
                dataclass_final_hook=dataclass_final_hook,
            )
        case list():
            values = list(map(pre, obj))
            if issubclass(list, type(obj)):
                return values
            key = f"[{_Prefixes.list_subclass.value}|{type(obj).__qualname__}]"
            return {key: values}
        case set():
            return _pre_process_container(
                obj,
                set,
                _Prefixes.set_,
                before=before,
                after=after,
                dataclass_final_hook=dataclass_final_hook,
            )
        case tuple():
            return _pre_process_container(
                obj,
                tuple,
                _Prefixes.tuple_,
                before=before,
                after=after,
                dataclass_final_hook=dataclass_final_hook,
            )
        case Dataclass():
            obj = asdict_without_defaults(
                obj, final=partial(_dataclass_final, hook=dataclass_final_hook)
            )
            return {k: pre(v) for k, v in obj.items()}
        case _:
            pass
    return obj if after is None else after(obj)


def _pre_process_container(
    obj: Any,
    cls: type[Any],
    prefix: _Prefixes,
    /,
    *,
    before: Callable[[Any], Any] | None = None,
    after: Callable[[Any], Any] | None = None,
    dataclass_final_hook: Callable[[type[Dataclass], StrMapping], StrMapping]
    | None = None,
) -> StrMapping:
    if issubclass(cls, type(obj)):
        key = f"[{prefix.value}]"
    else:
        key = f"[{prefix.value}|{type(obj).__qualname__}]"
    values = [
        _pre_process(
            o, before=before, after=after, dataclass_final_hook=dataclass_final_hook
        )
        for o in obj
    ]
    return {key: values}


def _dataclass_final(
    cls: type[Dataclass],
    mapping: StrMapping,
    /,
    *,
    hook: Callable[[type[Dataclass], StrMapping], StrMapping] | None = None,
) -> StrMapping:
    if hook is not None:
        mapping = hook(cls, mapping)
    return {f"[{_Prefixes.dataclass.value}|{cls.__qualname__}]": mapping}


def _serialize2_default(
    obj: Any,
    /,
    *,
    fallback: bool = False,
) -> str:
    if isinstance(obj, dt.datetime):
        if obj.tzinfo is None:
            ser = serialize_local_datetime(obj)
        else:
            ser = serialize_zoned_datetime(obj)
        return f"[{_Prefixes.datetime.value}]{ser}"
    if isinstance(obj, dt.date):  # after datetime
        ser = serialize_date(obj)
        return f"[{_Prefixes.date.value}]{ser}"
    if isinstance(obj, dt.time):
        ser = serialize_time(obj)
        return f"[{_Prefixes.time.value}]{ser}"
    if isinstance(obj, dt.timedelta):
        ser = serialize_timedelta(obj)
        return f"[{_Prefixes.timedelta.value}]{ser}"
    if isinstance(obj, Path):
        ser = str(obj)
        return f"[{_Prefixes.path.value}]{ser}"
    if fallback:
        return str(obj)
    raise TypeError


@dataclass(kw_only=True, slots=True)
class Serialize2Error(Exception):
    obj: Any


@dataclass(kw_only=True, slots=True)
class _Serialize2TypeError(Serialize2Error):
    @override
    def __str__(self) -> str:
        from rich.pretty import pretty_repr

        cls = get_class_name(self.obj)
        return f"Unable to serialize object of type {cls!r}:\n{pretty_repr(self.obj)}"


@dataclass(kw_only=True, slots=True)
class _Serialize2IntegerError(Serialize2Error):
    @override
    def __str__(self) -> str:
        return f"Integer {self.obj} is out of range"


def deserialize2(
    data: bytes, /, *, objects: AbstractSet[type[Any]] | None = None
) -> Any:
    """Deserialize an object."""
    return _object_hook(loads(data), data=data, objects=objects)


_DATACLASS_PATTERN = re.compile(r"^\[" + _Prefixes.dataclass.value + r"\|(.+)\]$")
_LIST_SUBCLASS_PATTERN = re.compile(
    r"^\[" + _Prefixes.list_subclass.value + r"\|(.+)\]$"
)
_LOCAL_DATETIME_PATTERN = re.compile(
    r"^\[" + _Prefixes.datetime.value + r"\](?!(?:.+\+\d{2}:\d{2}\[.+?\]))(.+)$"
)
_ZONED_DATETIME_PATTERN = re.compile(
    r"^\[" + _Prefixes.datetime.value + r"\](.+\+\d{2}:\d{2}\[.+?\])$"
)


def _make_unit_pattern(prefix: _Prefixes, /) -> Pattern[str]:
    return re.compile(r"^\[" + prefix.value + r"\](.+)$")


_DATE_PATTERN, _PATH_PATTERN, _TIME_PATTERN, _TIMEDELTA_PATTERN = map(
    _make_unit_pattern,
    [_Prefixes.date, _Prefixes.path, _Prefixes.time, _Prefixes.timedelta],
)


def _make_container_pattern(prefix: _Prefixes, /) -> Pattern[str]:
    return re.compile(r"^\[" + prefix.value + r"(?:\|(.+))?\]$")


_FROZENSET_PATTERN, _SET_PATTERN, _TUPLE_PATTERN = map(
    _make_container_pattern, [_Prefixes.frozenset_, _Prefixes.set_, _Prefixes.tuple_]
)


def _object_hook(
    obj: bool | float | str | dict[str, Any] | list[Any] | Dataclass | None,  # noqa: FBT001
    /,
    *,
    data: bytes,
    objects: AbstractSet[type[Any]] | None = None,
) -> Any:
    match obj:
        case bool() | int() | float() | Dataclass() | None:
            return obj
        case str():
            # ordered
            if match := _ZONED_DATETIME_PATTERN.search(obj):
                return parse_zoned_datetime(match.group(1))
            if match := _LOCAL_DATETIME_PATTERN.search(obj):
                return parse_local_datetime(match.group(1))
            # unordered
            if match := _DATE_PATTERN.search(obj):
                return parse_date(match.group(1))
            if match := _PATH_PATTERN.search(obj):
                return Path(match.group(1))
            if match := _TIME_PATTERN.search(obj):
                return parse_time(match.group(1))
            if match := _TIMEDELTA_PATTERN.search(obj):
                return parse_timedelta(match.group(1))
            return obj
        case list():
            return [_object_hook(o, data=data, objects=objects) for o in obj]
        case dict():
            if len(obj) == 1:
                key, value = one(obj.items())
                if 0:
                    if (match := _DATACLASS_PATTERN.search(key)) and isinstance(
                        value, dict
                    ):
                        if objects is None:
                            raise _Deserialize2NoObjectsError(data=data, obj=obj)
                        qualname = match.group(1)
                        try:
                            cls = one(o for o in objects if o.__qualname__ == qualname)
                        except OneEmptyError:
                            raise _Deserialize2ObjectEmptyError(
                                data=data, obj=obj, qualname=qualname
                            ) from None
                        return cls(**{
                            k: _object_hook(v, data=data, objects=objects)
                            for k, v in value.items()
                        })
                for cls, pattern in [
                    (frozenset, _FROZENSET_PATTERN),
                    (set, _SET_PATTERN),
                    (tuple, _TUPLE_PATTERN),
                ]:
                    result = _object_hook_container(
                        key, value, cls, pattern, data=data, objects=objects
                    )
                    if result is not None:
                        return result
                if (match := _LIST_SUBCLASS_PATTERN.search(key)) and isinstance(
                    value, list
                ):
                    if objects is None:
                        raise _Deserialize2NoObjectsError(data=data, obj=obj)
                    qualname = match.group(1)
                    try:
                        cls = one(o for o in objects if o.__qualname__ == qualname)
                    except OneEmptyError:
                        raise _Deserialize2ObjectEmptyError(
                            data=data, obj=obj, qualname=qualname
                        ) from None
                    values = (
                        _object_hook(v, data=data, objects=objects) for v in value
                    )
                    return cls(values)
                result = _object_hook_dataclass(key, value, data=data, objects=objects)
                if result is not None:
                    return result
                return {
                    k: _object_hook(v, data=data, objects=objects)
                    for k, v in obj.items()
                }
            return {
                k: _object_hook(v, data=data, objects=objects) for k, v in obj.items()
            }
        case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
            assert_never(cast(Never, never))


def _object_hook_container(
    key: str,
    value: Any,
    cls: type[Any],
    pattern: Pattern[str],
    /,
    *,
    data: bytes,
    objects: AbstractSet[type[Any]] | None = None,
) -> Any:
    if not (match := pattern.search(key)):
        return None
    match value:
        case dict():
            cls = _object_hook_get_container(match, cls, data=data, objects=objects)
            items = {
                k: _object_hook(v, data=data, objects=objects) for k, v in value.items()
            }
            return cls(**items)
        case list():
            cls = _object_hook_get_container(match, cls, data=data, objects=objects)
            return cls(_object_hook(v, data=data, objects=objects) for v in value)
        case _:
            return None


def _object_hook_get_container(
    match: Match[str],
    cls: type[Any],
    /,
    *,
    data: bytes,
    objects: AbstractSet[type[Any]] | None = None,
) -> type[Any]:
    if match.group(1) is None:
        return cls
    return _object_hook_get_object(match, data=data, objects=objects)


def _object_hook_get_object(
    match: Match[str],
    /,
    *,
    data: bytes,
    objects: AbstractSet[type[Any]] | None = None,
) -> type[Any]:
    qualname = match.group(1)
    if objects is None:
        raise _Deserialize2NoObjectsError(data=data, qualname=qualname)
    try:
        return one(o for o in objects if o.__qualname__ == qualname)
    except OneEmptyError:
        raise _Deserialize2ObjectEmptyError(data=data, qualname=qualname) from None


def _object_hook_dataclass(
    key: str,
    value: Any,
    /,
    *,
    data: bytes,
    objects: AbstractSet[type[Any]] | None = None,
) -> Any:
    if not (match := _DATACLASS_PATTERN.search(key)):
        return None
    if not isinstance(value, dict):
        return None
    cls = _object_hook_get_object(match, data=data, objects=objects)
    items = {k: _object_hook(v, data=data, objects=objects) for k, v in value.items()}
    return cls(**items)


@dataclass(kw_only=True, slots=True)
class Deserialize2Error(Exception):
    data: bytes
    qualname: str


@dataclass(kw_only=True, slots=True)
class _Deserialize2NoObjectsError(Deserialize2Error):
    @override
    def __str__(self) -> str:
        return f"Objects required to deserialize {self.qualname!r} from {self.data!r}"


@dataclass(kw_only=True, slots=True)
class _Deserialize2ObjectEmptyError(Deserialize2Error):
    @override
    def __str__(self) -> str:
        return (
            f"Unable to find object to deserialize {self.qualname!r} from {self.data!r}"
        )


__all__ = ["Deserialize2Error", "Serialize2Error", "deserialize2", "serialize2"]
