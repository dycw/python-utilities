from __future__ import annotations

import datetime as dt
import re
from functools import partial
from re import escape
from typing import Any

from orjson import OPT_PASSTHROUGH_DATETIME, OPT_SORT_KEYS, dumps, loads

from utilities.whenever import (
    parse_date,
    parse_zoned_datetime,
    serialize_date,
    serialize_zoned_datetime,
)

_DATE_PREFIX = "[d]"
_DATETIME_PREFIX = "[t]"


def serialize2(obj: Any, /, *, fallback: bool = False) -> bytes:
    """Serialize an object."""
    return dumps(
        obj,
        default=partial(_serialize2_default, fallback=fallback),
        option=OPT_PASSTHROUGH_DATETIME | OPT_SORT_KEYS,
    )


def _serialize2_default(obj: Any, /, *, fallback: bool = False) -> str:
    if isinstance(obj, dt.datetime):
        ser = serialize_zoned_datetime(obj)
        return f"{_DATETIME_PREFIX}{ser}"
    if isinstance(obj, dt.date):
        ser = serialize_date(obj)
        return f"{_DATE_PREFIX}{ser}"
    if fallback:
        return str(obj)
    raise TypeError


def deserialize2(data: bytes, /) -> Any:
    """Deserialize an object."""
    return _object_hook(loads(data))


_DATE_PATTERN = re.compile(rf"^{escape(_DATE_PREFIX)}(.+)$")
_DATETIME_PATTERN = re.compile(rf"^{escape(_DATETIME_PREFIX)}(.+)$")


def _object_hook(obj: Any, /) -> Any:
    if isinstance(obj, str):
        if match := _DATE_PATTERN.search(obj):
            return parse_date(match.group(1))
        if match := _DATETIME_PATTERN.search(obj):
            return parse_zoned_datetime(match.group(1))
    if isinstance(obj, dict):
        return {k: _object_hook(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return list(map(_object_hook, obj))
    return obj


__all__ = ["deserialize2", "serialize2"]
