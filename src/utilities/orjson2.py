from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass
from enum import Enum, unique
from functools import partial
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
from utilities.whenever import (
    parse_date,
    parse_zoned_datetime,
    serialize_date,
    serialize_zoned_datetime,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from collections.abc import Set as AbstractSet

    from utilities.types import StrMapping


@unique
class _Prefixes(Enum):
    # keep in order of deserialization
    datetime = "dt"
    date = "d"
    dataclass = "dc"


def serialize2(
    obj: Any,
    /,
    *,
    dataclass_hook: Callable[[type[Dataclass], StrMapping], StrMapping] | None = None,
    fallback: bool = False,
) -> bytes:
    """Serialize an object."""
    return dumps(
        obj,
        default=partial(
            _serialize2_default, dataclass_hook=dataclass_hook, fallback=fallback
        ),
        option=OPT_PASSTHROUGH_DATACLASS | OPT_PASSTHROUGH_DATETIME | OPT_SORT_KEYS,
    )


def _serialize2_default(
    obj: Any,
    /,
    *,
    dataclass_hook: Callable[[type[Dataclass], StrMapping], StrMapping] | None = None,
    fallback: bool = False,
) -> str:
    if isinstance(obj, dt.datetime):
        ser = serialize_zoned_datetime(obj)
        return f"[{_Prefixes.datetime.value}]{ser}"
    if isinstance(obj, dt.date):  # after datetime
        ser = serialize_date(obj)
        return f"[{_Prefixes.date.value}]{ser}"
    if is_dataclass_instance(obj):
        mapping = asdict_without_defaults(
            obj, final=partial(_serialize2_dataclass_final, hook=dataclass_hook)
        )
        return serialize2(mapping).decode()
    if fallback:
        return str(obj)
    raise TypeError


def _serialize2_dataclass_final(
    cls: type[Dataclass],
    mapping: StrMapping,
    /,
    *,
    hook: Callable[[type[Dataclass], StrMapping], StrMapping] | None = None,
) -> StrMapping:
    if hook is not None:
        mapping = hook(cls, mapping)
    return {f"[{_Prefixes.dataclass.value}|{cls.__qualname__}]": mapping}


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
_DATETIME_PATTERN = re.compile(r"^\[" + _Prefixes.datetime.value + r"\](.+)$")


def _object_hook(
    obj: bool | float | str | dict[str, Any] | list[Any] | Dataclass,  # noqa: FBT001
    /,
    *,
    data: bytes,
    objects: AbstractSet[type[Dataclass]] | None = None,
) -> Any:
    match obj:
        case bool() | int() | float() | Dataclass():
            return obj
        case str():
            if match := _DATETIME_PATTERN.search(obj):
                return parse_zoned_datetime(match.group(1))
            if match := _DATE_PATTERN.search(obj):
                return parse_date(match.group(1))
            if _DATACLASS_DICT_PATTERN.search(obj):
                return deserialize2(obj.encode(), objects=objects)
            return obj
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
            return {k: _object_hook(v, data=data) for k, v in obj.items()}
        case list():
            return [_object_hook(o, data=data, objects=objects) for o in obj]
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


__all__ = ["Deserialize2Error", "deserialize2", "serialize2"]
