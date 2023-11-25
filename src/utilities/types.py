from __future__ import annotations

import datetime as dt
from collections.abc import Hashable, Mapping, Sized
from collections.abc import Set as AbstractSet
from typing import Any, TypeGuard

Number = int | float
Duration = Number | dt.timedelta
SequenceStrs = list[str] | tuple[str, ...]
IterableStrs = SequenceStrs | AbstractSet[str] | Mapping[str, Any]


def ensure_class(obj: Any, /) -> type[Any]:
    """Ensure the class of an object is returned, if it is not a class."""
    return obj if isinstance(obj, type) else type(obj)


def ensure_hashable(obj: Any, /) -> Hashable:
    """Ensure an object is hashable."""
    if is_hashable(obj):
        return obj
    msg = f"{obj=}"
    raise EnsureHashableError(msg)


class EnsureHashableError(Exception):
    ...


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


def is_sized_not_str(obj: Any, /) -> TypeGuard[Sized]:
    """Check if an object is sized, but not a string."""
    try:
        _ = len(obj)
    except TypeError:
        return False
    return not isinstance(obj, str)


__all__ = [
    "Duration",
    "ensure_class",
    "ensure_hashable",
    "EnsureHashableError",
    "is_hashable",
    "is_sized_not_str",
    "issubclass_except_bool_int",
    "Number",
    "IterableStrs",
    "SequenceStrs",
]
