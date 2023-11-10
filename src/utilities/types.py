from __future__ import annotations

import datetime as dt
from typing import Any

Number = int | float
Duration = Number | dt.timedelta
NoneType = type(None)


def ensure_class(x: Any, /) -> type[Any]:
    """Ensure the class of an object is returned, if it is not a class."""
    return x if isinstance(x, type) else type(x)


def issubclass_except_bool_int(x: type[Any], y: type[Any], /) -> bool:
    """Checks for the subclass relation, except bool < int."""
    return issubclass(x, y) and not (issubclass(x, bool) and issubclass(int, y))


__all__ = [
    "Duration",
    "ensure_class",
    "issubclass_except_bool_int",
    "NoneType",
    "Number",
]
