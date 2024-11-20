from __future__ import annotations

import datetime as dt
import re
from dataclasses import asdict, dataclass
from enum import Enum, unique
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, Never, assert_never, cast

from orjson import (
    OPT_PASSTHROUGH_DATACLASS,
    OPT_PASSTHROUGH_DATETIME,
    OPT_SORT_KEYS,
    dumps,
    loads,
)
from typing_extensions import override

from utilities.dataclasses import (
    Dataclass,
    asdict_without_defaults,
    is_dataclass_instance,
)
from utilities.iterables import OneEmptyError, one
from utilities.math import MAX_INT64, MIN_INT64
from utilities.whenever import (
    parse_date,
    parse_local_datetime,
    parse_timedelta,
    parse_zoned_datetime,
    serialize_date,
    serialize_local_datetime,
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
    path = "p"
    timedelta = "td"


def serialize2(
    obj: Any,
    /,
    *,
    dataclass_hook: Callable[[type[Dataclass], StrMapping], StrMapping] | None = None,
    fallback: bool = False,
) -> bytes:
    """Serialize an object."""
    _pre_process_check(obj)
    asdict_final = partial(_dataclass_hook_final, hook=dataclass_hook)
    if is_dataclass_instance(obj):
        obj_use = asdict_without_defaults(obj, final=asdict_final)
    else:
        obj_use = obj
    return dumps(
        obj_use,
        default=partial(
            _serialize2_default, dataclass_asdict_final=asdict_final, fallback=fallback
        ),
        option=OPT_PASSTHROUGH_DATACLASS | OPT_PASSTHROUGH_DATETIME | OPT_SORT_KEYS,
    )


def _pre_process_check(obj: Any) -> None:
    if isinstance(obj, int) and not (MIN_INT64 <= obj <= MAX_INT64):
        msg = f"Integer value {obj} is out of range"
        raise _Serialize2IntegerError(obj=obj)
    if isinstance(obj, list):
        for o in obj:
            _pre_process_check(o)
        return
    if isinstance(obj, dict):
        for v in obj.values():
            _pre_process_check(v)
        return
    if is_dataclass_instance(obj):
        for v in asdict(obj).values():
            _pre_process_check(v)
        return


def _dataclass_hook_final(
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
    dataclass_asdict_final: Callable[[type[Dataclass], StrMapping], StrMapping],
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
    if isinstance(obj, dt.timedelta):
        ser = serialize_timedelta(obj)
        return f"[{_Prefixes.timedelta.value}]{ser}"
    if isinstance(obj, Path):
        ser = str(obj)
        return f"[{_Prefixes.path.value}]{ser}"
    if is_dataclass_instance(obj):
        mapping = asdict_without_defaults(obj, final=dataclass_asdict_final)
        return serialize2(mapping).decode()
    if fallback:
        return str(obj)
    raise TypeError


@dataclass(kw_only=True, slots=True)
class Serialize2Error(Exception):
    obj: Any


@dataclass(kw_only=True, slots=True)
class _Serialize2IntegerError(Serialize2Error):
    @override
    def __str__(self) -> str:
        return f"Integer {self.obj} is out of range"


def deserialize2(
    data: bytes, /, *, objects: AbstractSet[type[Dataclass]] | None = None
) -> Any:
    """Deserialize an object."""
    return _object_hook(loads(data), data=data, objects=objects)


_DATACLASS_ONE_PATTERN = re.compile(r"^\[" + _Prefixes.dataclass.value + r"\|(.+?)\]$")
_DATACLASS_DICT_PATTERN = re.compile(
    r'^{"\[' + _Prefixes.dataclass.value + r'\|.+?\]":{.*?}}$'
)
_DATE_PATTERN = re.compile(r"^\[" + _Prefixes.date.value + r"\](.+)$")
_PATH_PATTERN = re.compile(r"^\[" + _Prefixes.path.value + r"\](.+)$")
_LOCAL_DATETIME_PATTERN = re.compile(r"^\[" + _Prefixes.datetime.value + r"\](.+)$")
_ZONED_DATETIME_PATTERN = re.compile(
    r"^\[" + _Prefixes.datetime.value + r"\](.+\+\d{2}:\d{2}\[.+?\])$"
)
_TIMEDELTA_PATTERN = re.compile(r"^\[" + _Prefixes.timedelta.value + r"\](.+)$")


def _object_hook(
    obj: bool | float | str | dict[str, Any] | list[Any] | Dataclass | None,  # noqa: FBT001
    /,
    *,
    data: bytes,
    objects: AbstractSet[type[Dataclass]] | None = None,
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
            if match := _TIMEDELTA_PATTERN.search(obj):
                return parse_timedelta(match.group(1))
            if _DATACLASS_DICT_PATTERN.search(obj):
                return deserialize2(obj.encode(), objects=objects)
            return obj
        case list():
            return [_object_hook(o, data=data, objects=objects) for o in obj]
        case dict():
            if len(obj) == 1:
                key, value = one(obj.items())
                if (match := _DATACLASS_ONE_PATTERN.search(key)) and isinstance(
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
                return {
                    k: _object_hook(v, data=data, objects=objects)
                    for k, v in obj.items()
                }
            return {
                k: _object_hook(v, data=data, objects=objects) for k, v in obj.items()
            }
        case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
            assert_never(cast(Never, never))


@dataclass(kw_only=True, slots=True)
class Deserialize2Error(Exception):
    data: bytes
    obj: Any


@dataclass(kw_only=True, slots=True)
class _Deserialize2NoObjectsError(Deserialize2Error):
    @override
    def __str__(self) -> str:
        return f"Objects required to deserialize {self.obj!r} from {self.data!r}"


@dataclass(kw_only=True, slots=True)
class _Deserialize2ObjectEmptyError(Deserialize2Error):
    qualname: str

    @override
    def __str__(self) -> str:
        return f"Unable to find object {self.qualname!r} to deserialize {self.obj!r} (from {self.data!r})"


__all__ = ["Deserialize2Error", "Serialize2Error", "deserialize2", "serialize2"]
