from __future__ import annotations

import datetime as dt
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, TypeGuard, TypeVar, overload

from typing_extensions import override

from utilities.functions import get_class_name

if TYPE_CHECKING:
    from collections.abc import Callable, Container, Hashable, Sized

Number = int | float
Duration = Number | dt.timedelta
PathLike = Path | str
StrMapping = Mapping[str, Any]
TupleOrStrMapping = tuple[Any, ...] | StrMapping


_T = TypeVar("_T")
_T1 = TypeVar("_T1")
_T2 = TypeVar("_T2")
_T3 = TypeVar("_T3")
_T4 = TypeVar("_T4")
_T5 = TypeVar("_T5")


@overload
def ensure_bool(obj: Any, /, *, nullable: bool) -> bool | None: ...
@overload
def ensure_bool(obj: Any, /, *, nullable: Literal[False] = False) -> bool: ...
def ensure_bool(obj: Any, /, *, nullable: bool = False) -> bool | None:
    """Ensure an object is a boolean."""
    try:
        return ensure_class(obj, bool, nullable=nullable)
    except EnsureClassError as error:
        raise EnsureBoolError(obj=error.obj, nullable=nullable) from None


@dataclass(kw_only=True)
class EnsureBoolError(Exception):
    obj: Any
    nullable: bool

    @override
    def __str__(self) -> str:
        desc = " or None" if self.nullable else ""
        return f"Object {self.obj} must be a boolean{desc}; got {get_class_name(self.obj)} instead"


@overload
def ensure_class(obj: Any, cls: type[_T], /, *, nullable: bool) -> _T | None: ...
@overload
def ensure_class(
    obj: Any, cls: type[_T], /, *, nullable: Literal[False] = False
) -> _T: ...
@overload
def ensure_class(
    obj: Any, cls: tuple[type[_T1], type[_T2]], /, *, nullable: bool
) -> _T1 | _T2 | None: ...
@overload
def ensure_class(
    obj: Any, cls: tuple[type[_T1], type[_T2]], /, *, nullable: Literal[False] = False
) -> _T1 | _T2: ...
@overload
def ensure_class(
    obj: Any, cls: tuple[type[_T1], type[_T2], type[_T3]], /, *, nullable: bool
) -> _T1 | _T2 | _T3 | None: ...
@overload
def ensure_class(
    obj: Any,
    cls: tuple[type[_T1], type[_T2], type[_T3]],
    /,
    *,
    nullable: Literal[False] = False,
) -> _T1 | _T2 | _T3: ...
@overload
def ensure_class(
    obj: Any,
    cls: tuple[type[_T1], type[_T2], type[_T3], type[_T4]],
    /,
    *,
    nullable: bool,
) -> _T1 | _T2 | _T3 | _T4 | None: ...
@overload
def ensure_class(
    obj: Any,
    cls: tuple[type[_T1], type[_T2], type[_T3], type[_T4]],
    /,
    *,
    nullable: Literal[False] = False,
) -> _T1 | _T2 | _T3 | _T4: ...
@overload
def ensure_class(
    obj: Any,
    cls: tuple[type[_T1], type[_T2], type[_T3], type[_T4], type[_T5]],
    /,
    *,
    nullable: bool,
) -> _T1 | _T2 | _T3 | _T4 | _T5 | None: ...
@overload
def ensure_class(
    obj: Any,
    cls: tuple[type[_T1], type[_T2], type[_T3], type[_T4], type[_T5]],
    /,
    *,
    nullable: Literal[False] = False,
) -> _T1 | _T2 | _T3 | _T4 | _T5: ...
def ensure_class(  # pyright: ignore[reportInconsistentOverload]
    obj: Any, cls: type[_T] | tuple[type[_T], ...], /, *, nullable: bool = False
) -> _T:
    """Ensure an object is of the required class."""
    if isinstance(obj, cls) or ((obj is None) and nullable):
        return obj
    raise EnsureClassError(obj=obj, cls=cls, nullable=nullable)


@dataclass(kw_only=True)
class EnsureClassError(Exception):
    obj: Any
    cls: type[Any] | tuple[type[Any], ...]
    nullable: bool

    @override
    def __str__(self) -> str:
        desc = " or None" if self.nullable else ""
        return f"Object {self.obj} must be an instance of {self.cls}{desc}; got {type(self.obj)} instead"


@overload
def ensure_date(obj: Any, /, *, nullable: bool) -> dt.date | None: ...
@overload
def ensure_date(obj: Any, /, *, nullable: Literal[False] = False) -> dt.date: ...
def ensure_date(obj: Any, /, *, nullable: bool = False) -> dt.date | None:
    """Ensure an object is a date."""
    try:
        return ensure_class(obj, dt.date, nullable=nullable)
    except EnsureClassError as error:
        raise EnsureDateError(obj=error.obj, nullable=nullable) from None


@dataclass(kw_only=True)
class EnsureDateError(Exception):
    obj: Any
    nullable: bool

    @override
    def __str__(self) -> str:
        desc = " or None" if self.nullable else ""
        return f"Object {self.obj} must be a date{desc}; got {get_class_name(self.obj)} instead"


@overload
def ensure_datetime(obj: Any, /, *, nullable: bool) -> dt.datetime | None: ...
@overload
def ensure_datetime(
    obj: Any, /, *, nullable: Literal[False] = False
) -> dt.datetime: ...
def ensure_datetime(obj: Any, /, *, nullable: bool = False) -> dt.datetime | None:
    """Ensure an object is a datetime."""
    try:
        return ensure_class(obj, dt.datetime, nullable=nullable)
    except EnsureClassError as error:
        raise EnsureDatetimeError(obj=error.obj, nullable=nullable) from None


@dataclass(kw_only=True)
class EnsureDatetimeError(Exception):
    obj: Any
    nullable: bool

    @override
    def __str__(self) -> str:
        desc = " or None" if self.nullable else ""
        return f"Object {self.obj} must be a datetime{desc}; got {get_class_name(self.obj)} instead"


@overload
def ensure_float(obj: Any, /, *, nullable: bool) -> float | None: ...
@overload
def ensure_float(obj: Any, /, *, nullable: Literal[False] = False) -> float: ...
def ensure_float(obj: Any, /, *, nullable: bool = False) -> float | None:
    """Ensure an object is a float."""
    try:
        return ensure_class(obj, float, nullable=nullable)
    except EnsureClassError as error:
        raise EnsureFloatError(obj=error.obj, nullable=nullable) from None


@dataclass(kw_only=True)
class EnsureFloatError(Exception):
    obj: Any
    nullable: bool

    @override
    def __str__(self) -> str:
        desc = " or None" if self.nullable else ""
        return f"Object {self.obj} must be a float{desc}; got {get_class_name(self.obj)} instead"


def ensure_hashable(obj: Any, /) -> Hashable:
    """Ensure an object is hashable."""
    if is_hashable(obj):
        return obj
    raise EnsureHashableError(obj=obj)


@dataclass(kw_only=True)
class EnsureHashableError(Exception):
    obj: Any

    @override
    def __str__(self) -> str:
        return f"Object {self.obj} must be hashable."


@overload
def ensure_int(obj: Any, /, *, nullable: bool) -> int | None: ...
@overload
def ensure_int(obj: Any, /, *, nullable: Literal[False] = False) -> int: ...
def ensure_int(obj: Any, /, *, nullable: bool = False) -> int | None:
    """Ensure an object is an integer."""
    try:
        return ensure_class(obj, int, nullable=nullable)
    except EnsureClassError as error:
        raise EnsureIntError(obj=error.obj, nullable=nullable) from None


@dataclass(kw_only=True)
class EnsureIntError(Exception):
    obj: Any
    nullable: bool

    @override
    def __str__(self) -> str:
        desc = " or None" if self.nullable else ""
        return f"Object {self.obj} must be an integer{desc}; got {get_class_name(self.obj)} instead"


@overload
def ensure_member(
    obj: Any, container: Container[_T], /, *, nullable: bool
) -> _T | None: ...
@overload
def ensure_member(
    obj: Any, container: Container[_T], /, *, nullable: Literal[False] = False
) -> _T: ...
def ensure_member(
    obj: Any, container: Container[_T], /, *, nullable: bool = False
) -> _T | None:
    """Ensure an object is a member of the container."""
    if (obj in container) or ((obj is None) and nullable):
        return obj
    raise EnsureMemberError(obj=obj, container=container, nullable=nullable)


@dataclass(kw_only=True)
class EnsureMemberError(Exception):
    obj: Any
    container: Container[Any]
    nullable: bool

    @override
    def __str__(self) -> str:
        desc = " or None" if self.nullable else ""
        return f"Object {self.obj} must be a member of {self.container}{desc}"


@overload
def ensure_number(obj: Any, /, *, nullable: bool) -> Number | None: ...
@overload
def ensure_number(obj: Any, /, *, nullable: Literal[False] = False) -> Number: ...
def ensure_number(obj: Any, /, *, nullable: bool = False) -> Number | None:
    """Ensure an object is a number."""
    try:
        return ensure_class(obj, (int, float), nullable=nullable)
    except EnsureClassError as error:
        raise EnsureNumberError(obj=error.obj, nullable=nullable) from None


@dataclass(kw_only=True)
class EnsureNumberError(Exception):
    obj: Any
    nullable: bool

    @override
    def __str__(self) -> str:
        desc = " or None" if self.nullable else ""
        return f"Object {self.obj} must be a number{desc}; got {get_class_name(self.obj)} instead"


def ensure_sized(obj: Any, /) -> Sized:
    """Ensure an object is sized."""
    if is_sized(obj):
        return obj
    raise EnsureSizedError(obj=obj)


@dataclass(kw_only=True)
class EnsureSizedError(Exception):
    obj: Any

    @override
    def __str__(self) -> str:
        return f"Object {self.obj} must be sized"


def ensure_sized_not_str(obj: Any, /) -> Sized:
    """Ensure an object is sized, but not a string."""
    if is_sized_not_str(obj):
        return obj
    raise EnsureSizedNotStrError(obj=obj)


@dataclass(kw_only=True)
class EnsureSizedNotStrError(Exception):
    obj: Any

    @override
    def __str__(self) -> str:
        return f"Object {self.obj} must be sized, but not a string"


@overload
def ensure_time(obj: Any, /, *, nullable: bool) -> dt.time | None: ...
@overload
def ensure_time(obj: Any, /, *, nullable: Literal[False] = False) -> dt.time: ...
def ensure_time(obj: Any, /, *, nullable: bool = False) -> dt.time | None:
    """Ensure an object is a time."""
    try:
        return ensure_class(obj, dt.time, nullable=nullable)
    except EnsureClassError as error:
        raise EnsureTimeError(obj=error.obj, nullable=nullable) from None


@dataclass(kw_only=True)
class EnsureTimeError(Exception):
    obj: Any
    nullable: bool

    @override
    def __str__(self) -> str:
        desc = " or None" if self.nullable else ""
        return f"Object {self.obj} must be a time{desc}; got {get_class_name(self.obj)} instead"


def is_hashable(obj: Any, /) -> TypeGuard[Hashable]:
    """Check if an object is hashable."""
    try:
        _ = hash(obj)
    except TypeError:
        return False
    return True


def issubclass_except_bool_int(x: type[Any], y: type[Any], /) -> bool:
    """Check for the subclass relation, except bool < int."""
    return issubclass(x, y) and not (issubclass(x, bool) and issubclass(int, y))


def is_sized(obj: Any, /) -> TypeGuard[Sized]:
    """Check if an object is sized."""
    try:
        _ = len(obj)
    except TypeError:
        return False
    return True


def is_sized_not_str(obj: Any, /) -> TypeGuard[Sized]:
    """Check if an object is sized, but not a string."""
    return is_sized(obj) and not isinstance(obj, str)


def is_string_mapping(obj: Any, /) -> TypeGuard[StrMapping]:
    """Check if an object is a string mapping."""
    return isinstance(obj, dict) and all(isinstance(key, str) for key in obj)


def is_tuple_or_string_mapping(obj: Any, /) -> TypeGuard[TupleOrStrMapping]:
    """Check if an object is a tuple or string mapping."""
    return isinstance(obj, tuple) or is_string_mapping(obj)


def make_isinstance(cls: type[_T], /) -> Callable[[Any], TypeGuard[_T]]:
    """Check if an object is hashable."""

    def inner(obj: Any, /) -> TypeGuard[_T]:
        return isinstance(obj, cls)

    return inner


__all__ = [
    "Duration",
    "EnsureBoolError",
    "EnsureClassError",
    "EnsureDateError",
    "EnsureDatetimeError",
    "EnsureFloatError",
    "EnsureHashableError",
    "EnsureIntError",
    "EnsureMemberError",
    "EnsureNumberError",
    "EnsureSizedError",
    "EnsureSizedNotStrError",
    "EnsureTimeError",
    "Number",
    "PathLike",
    "ensure_bool",
    "ensure_class",
    "ensure_date",
    "ensure_datetime",
    "ensure_float",
    "ensure_hashable",
    "ensure_int",
    "ensure_member",
    "ensure_number",
    "ensure_sized",
    "ensure_sized_not_str",
    "ensure_time",
    "is_hashable",
    "is_sized",
    "is_sized_not_str",
    "is_string_mapping",
    "is_tuple_or_string_mapping",
    "issubclass_except_bool_int",
    "make_isinstance",
]
