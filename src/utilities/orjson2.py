from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass
from functools import partial
from typing import Any

from orjson import OPT_PASSTHROUGH_DATETIME, OPT_SORT_KEYS, dumps, loads
from typing_extensions import override

from utilities.functions import get_class_name
from utilities.whenever import parse_zoned_datetime, serialize_zoned_datetime


def serialize2(obj: Any, /, *, fallback: bool = False) -> bytes:
    """Serialize an object."""
    try:
        return dumps(
            obj,
            default=partial(_serialize_default, fallback=fallback),
            option=OPT_PASSTHROUGH_DATETIME | OPT_SORT_KEYS,
        )
    except TypeError:
        raise Serialize2Error(obj=obj) from None


@dataclass(kw_only=True, slots=True)
class Serialize2Error(Exception):
    obj: Any

    @override
    def __str__(self) -> str:
        cls = get_class_name(self.obj)
        return f"Unable to serialize object of type {cls!r}"


def _serialize_default(obj: Any, /, *, fallback: bool = False) -> bytes:
    if isinstance(obj, dt.datetime):
        ser = serialize_zoned_datetime(obj)
        return f"[dt]{ser}".encode()
    if fallback:
        return str(obj).encode()
    raise Serialize2Error(obj=obj) from None


def _serialize_zoned_datetime(datetime: dt.datetime, /) -> bytes:
    ser = serialize_zoned_datetime(datetime)
    return f"[dt]{ser}".encode()


def deserialize2(data: bytes, /) -> Any:
    """Deserialize an object."""
    return _object_hook(loads(data))


_DATETIME_PATTERN = re.compile(rb"^\[dt\](.+)$")


def _object_hook(obj: Any, /) -> Any:
    if isinstance(obj, list):
        return list(map(_object_hook, obj))
    if isinstance(obj, bytes) and (match := _DATETIME_PATTERN.search(obj)):
        return parse_zoned_datetime(match.group(1).decode())
    return obj


__all__ = ["serialize2"]
