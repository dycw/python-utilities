from __future__ import annotations

import datetime as dt
from collections.abc import Hashable, Mapping, Sized
from collections.abc import Set as AbstractSet
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeGuard, TypeVar, overload

from typing_extensions import override

Number = int | float
Duration = Number | dt.timedelta
SequenceStrs = list[str] | tuple[str, ...]
IterableStrs = SequenceStrs | AbstractSet[str] | Mapping[str, Any]
PathLike = Path | str


_T = TypeVar("_T")


@overload
def get_class(obj: type[_T], /) -> type[_T]:
    ...


@overload
def get_class(obj: _T, /) -> type[_T]:
    ...


def get_class(obj: _T | type[_T], /) -> type[_T]:
    """Get the class of an object, unless it is already a class."""
    return obj if isinstance(obj, type) else type(obj)


def get_class_name(obj: Any, /) -> str:
    """Get the name of the class of an object, unless it is already a class."""
    return get_class(obj).__name__


def ensure_class(obj: Any, cls: type[_T], /) -> _T:
    """Ensure an object is of the required class."""
    if isinstance(obj, cls):
        return obj
    raise EnsureClassError(obj=obj, cls=cls)


@dataclass(frozen=True, kw_only=True, slots=True)
class EnsureClassError(Exception):
    obj: Any
    cls: type[Any]

    @override
    def __str__(self) -> str:
        return "Object {} must be an instance of {}; got {} instead".format(
            self.obj, self.cls, type(self.obj)
        )


def ensure_hashable(obj: Any, /) -> Hashable:
    """Ensure an object is hashable."""
    if is_hashable(obj):
        return obj
    raise EnsureHashableError(obj=obj)


@dataclass(frozen=True, kw_only=True, slots=True)
class EnsureHashableError(Exception):
    obj: Any

    @override
    def __str__(self) -> str:
        return f"Object {self.obj} must be hashable"


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
    "EnsureClassError",
    "EnsureHashableError",
    "get_class",
    "get_class_name",
    "is_hashable",
    "is_sized_not_str",
    "issubclass_except_bool_int",
    "Number",
    "IterableStrs",
    "PathLike",
    "SequenceStrs",
]
