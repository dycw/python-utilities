from __future__ import annotations

import datetime as dt
import re
from dataclasses import MISSING, fields, is_dataclass
from enum import Enum, StrEnum, unique
from functools import partial
from re import escape, search
from typing import TYPE_CHECKING, AbstractSet, Any

from dacite import from_dict
from orjson import (
    OPT_PASSTHROUGH_DATACLASS,
    OPT_PASSTHROUGH_DATETIME,
    OPT_SORT_KEYS,
    JSONDecodeError,
    dumps,
    loads,
)

from utilities.dataclasses import (
    Dataclass,
    asdict_without_defaults,
    is_dataclass_instance,
)
from utilities.functions import get_class_name
from utilities.iterables import OneEmptyError, OneNonUniqueError, one
from utilities.whenever import (
    parse_date,
    parse_zoned_datetime,
    serialize_date,
    serialize_zoned_datetime,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from utilities.types import StrMapping, ensure_class


@unique
class _Prefixes(Enum):
    # keep in order of deserialization
    datetime = "dt"
    date = "d"
    dataclass = "dc"


def serialize2(obj: Any, /, *, fallback: bool = False) -> bytes:
    """Serialize an object."""
    return dumps(
        obj,
        default=partial(_serialize2_default, fallback=fallback),
        option=OPT_PASSTHROUGH_DATACLASS | OPT_PASSTHROUGH_DATETIME | OPT_SORT_KEYS,
    )


def _serialize2_default(obj: Any, /, *, fallback: bool = False) -> str:
    if isinstance(obj, dt.datetime):
        ser = serialize_zoned_datetime(obj)
        return f"[{_Prefixes.datetime.value}]{ser}"
    if isinstance(obj, dt.date):  # after datetime
        ser = serialize_date(obj)
        return f"[{_Prefixes.date.value}]{ser}"
    if is_dataclass_instance(obj):
        mapping = asdict_without_defaults(obj, final=_serialize2_dataclass_final)
        # breakpoint()

        return serialize2(mapping).decode()
    if fallback:
        return str(obj)
    raise TypeError


def _serialize2_dataclass_final(
    obj: type[Dataclass], mapping: StrMapping, /
) -> dict[str, Any]:
    return {f"[{_Prefixes.dataclass.value}|{obj.__qualname__}]": mapping}


def deserialize2(
    data: bytes,
    /,
    *,
    objects: AbstractSet[type[Dataclass]] | None = None,
) -> Any:
    """Deserialize an object."""
    return _object_hook(loads(data), objects=objects)


_DATACLASS_PATTERN = re.compile(r"^\[" + _Prefixes.dataclass.value + r"\|(.+?)\]$")
_DATE_PATTERN = re.compile(r"^\[" + _Prefixes.date.value + r"\](.+)$")
_DATETIME_PATTERN = re.compile(r"^\[" + _Prefixes.datetime.value + r"\](.+)$")


def _object_hook(
    obj: bool | float | str | dict[str, Any] | list[Any] | Dataclass,  # noqa: FBT001
    /,
    *,
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
            try:
                return deserialize2(obj.encode(), objects=objects)
            except JSONDecodeError:
                return obj
        case dict():
            if len(obj) == 1:
                key, value = one(obj.items())
                if (match := _DATACLASS_PATTERN.search(key)) and isinstance(
                    value, dict
                ):
                    if objects is None:
                        raise _Deserialize2NoObjectMappingsError(obj=obj)
                    qualname = match.group(1)
                    try:
                        cls = one(o for o in objects if o.__qualname__ == qualname)
                    except OneEmptyError:
                        raise _Deserialize2ObjectEmptyError(obj=obj, qualname=qualname)
                    except OneNonUniqueError as error:
                        raise _Deserialize2ObjectNonUniquerror(
                            obj=obj, qualname=qualname
                        ) from error
                    return cls(**{
                        k: _object_hook(v, objects=objects) for k, v in value.items()
                    })
                return {k: _object_hook(v, objects=objects) for k, v in obj.items()}
            return {k: _object_hook(v) for k, v in obj.items()}
        case list():
            return [_object_hook(o, objects=objects) for o in obj]
        case _:
            msg = f"Object {obj} of type {get_class_name(obj)}"
            raise TypeError(msg)
    return None


__all__ = ["deserialize2", "serialize2"]
