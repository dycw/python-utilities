from __future__ import annotations

import datetime as dt
from collections.abc import Hashable
from typing import Any, TypeGuard

Number = int | float
Duration = Number | dt.timedelta
NoneType = type(None)


def ensure_class(obj: Any, /) -> type[Any]:
    """Ensure the class of an object is returned, if it is not a class."""
    return obj if isinstance(obj, type) else type(obj)


def is_hashable(obj: Any, /) -> TypeGuard[Hashable]:
    """Check if an object is hashable."""
    try:
        _ = hash(obj)
    except TypeError:
        return False
    return True


def issubclass_except_bool_int(x: type[Any], y: type[Any], /) -> bool:
    """Checks for the subclass relation, except bool < int."""
    return issubclass(x, y) and not (issubclass(x, bool) and issubclass(int, y))


__all__ = [
    "Duration",
    "ensure_class",
    "is_hashable",
    "issubclass_except_bool_int",
    "NoneType",
    "Number",
]
