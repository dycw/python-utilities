from __future__ import annotations

from dataclasses import dataclass
from functools import wraps
from inspect import getattr_static
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, overload, override

from rich.pretty import pretty_repr
from whenever import Date, PlainDateTime, Time, TimeDelta, ZonedDateTime

from utilities.core import get_class_name, repr_str

if TYPE_CHECKING:
    from collections.abc import Callable, Container, Iterable, Iterator

    from utilities.types import Number, TypeLike


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


@dataclass(kw_only=True, slots=True)
class EnsureBoolError(Exception):
    obj: Any
    nullable: bool

    @override
    def __str__(self) -> str:
        return _make_error_msg(self.obj, "a boolean", nullable=self.nullable)


##


@overload
def ensure_bytes(obj: Any, /, *, nullable: bool) -> bytes | None: ...
@overload
def ensure_bytes(obj: Any, /, *, nullable: Literal[False] = False) -> bytes: ...
def ensure_bytes(obj: Any, /, *, nullable: bool = False) -> bytes | None:
    """Ensure an object is a bytesean."""
    try:
        return ensure_class(obj, bytes, nullable=nullable)
    except EnsureClassError as error:
        raise EnsureBytesError(obj=error.obj, nullable=nullable) from None


@dataclass(kw_only=True, slots=True)
class EnsureBytesError(Exception):
    obj: Any
    nullable: bool

    @override
    def __str__(self) -> str:
        return _make_error_msg(self.obj, "a byte string", nullable=self.nullable)


##


@overload
def ensure_class[T](obj: Any, cls: type[T], /, *, nullable: bool) -> T | None: ...
@overload
def ensure_class[T](
    obj: Any, cls: type[T], /, *, nullable: Literal[False] = False
) -> T: ...
@overload
def ensure_class[T1, T2](
    obj: Any, cls: tuple[type[T1], type[T2]], /, *, nullable: bool
) -> T1 | T2 | None: ...
@overload
def ensure_class[T1, T2](
    obj: Any, cls: tuple[type[T1], type[T2]], /, *, nullable: Literal[False] = False
) -> T1 | T2: ...
@overload
def ensure_class[T1, T2, T3](
    obj: Any, cls: tuple[type[T1], type[T2], type[T3]], /, *, nullable: bool
) -> T1 | T2 | T3 | None: ...
@overload
def ensure_class[T1, T2, T3](
    obj: Any,
    cls: tuple[type[T1], type[T2], type[T3]],
    /,
    *,
    nullable: Literal[False] = False,
) -> T1 | T2 | T3: ...
@overload
def ensure_class[T1, T2, T3, T4](
    obj: Any, cls: tuple[type[T1], type[T2], type[T3], type[T4]], /, *, nullable: bool
) -> T1 | T2 | T3 | T4 | None: ...
@overload
def ensure_class[T1, T2, T3, T4](
    obj: Any,
    cls: tuple[type[T1], type[T2], type[T3], type[T4]],
    /,
    *,
    nullable: Literal[False] = False,
) -> T1 | T2 | T3 | T4: ...
@overload
def ensure_class[T1, T2, T3, T4, T5](
    obj: Any,
    cls: tuple[type[T1], type[T2], type[T3], type[T4], type[T5]],
    /,
    *,
    nullable: bool,
) -> T1 | T2 | T3 | T4 | T5 | None: ...
@overload
def ensure_class[T1, T2, T3, T4, T5](
    obj: Any,
    cls: tuple[type[T1], type[T2], type[T3], type[T4], type[T5]],
    /,
    *,
    nullable: Literal[False] = False,
) -> T1 | T2 | T3 | T4 | T5: ...
@overload
def ensure_class[T](
    obj: Any, cls: TypeLike[T], /, *, nullable: bool = False
) -> Any: ...
def ensure_class[T](obj: Any, cls: TypeLike[T], /, *, nullable: bool = False) -> Any:
    """Ensure an object is of the required class."""
    if isinstance(obj, cls) or ((obj is None) and nullable):
        return obj
    raise EnsureClassError(obj=obj, cls=cls, nullable=nullable)


@dataclass(kw_only=True, slots=True)
class EnsureClassError(Exception):
    obj: Any
    cls: TypeLike[Any]
    nullable: bool

    @override
    def __str__(self) -> str:
        return _make_error_msg(
            self.obj,
            f"an instance of {get_class_name(self.cls)!r}",
            nullable=self.nullable,
        )


##


@overload
def ensure_date(obj: Any, /, *, nullable: bool) -> Date | None: ...
@overload
def ensure_date(obj: Any, /, *, nullable: Literal[False] = False) -> Date: ...
def ensure_date(obj: Any, /, *, nullable: bool = False) -> Date | None:
    """Ensure an object is a date."""
    try:
        return ensure_class(obj, Date, nullable=nullable)
    except EnsureClassError as error:
        raise EnsureDateError(obj=error.obj, nullable=nullable) from None


@dataclass(kw_only=True, slots=True)
class EnsureDateError(Exception):
    obj: Any
    nullable: bool

    @override
    def __str__(self) -> str:
        return _make_error_msg(self.obj, "a date", nullable=self.nullable)


##


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


@dataclass(kw_only=True, slots=True)
class EnsureFloatError(Exception):
    obj: Any
    nullable: bool

    @override
    def __str__(self) -> str:
        return _make_error_msg(self.obj, "a float", nullable=self.nullable)


##


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


@dataclass(kw_only=True, slots=True)
class EnsureIntError(Exception):
    obj: Any
    nullable: bool

    @override
    def __str__(self) -> str:
        return _make_error_msg(self.obj, "an integer", nullable=self.nullable)


##


@overload
def ensure_member[T](
    obj: Any, container: Container[T], /, *, nullable: bool
) -> T | None: ...
@overload
def ensure_member[T](
    obj: Any, container: Container[T], /, *, nullable: Literal[False] = False
) -> T: ...
def ensure_member[T](
    obj: Any, container: Container[T], /, *, nullable: bool = False
) -> T | None:
    """Ensure an object is a member of the container."""
    if (obj in container) or ((obj is None) and nullable):
        return obj
    raise EnsureMemberError(obj=obj, container=container, nullable=nullable)


@dataclass(kw_only=True, slots=True)
class EnsureMemberError(Exception):
    obj: Any
    container: Container[Any]
    nullable: bool

    @override
    def __str__(self) -> str:
        return _make_error_msg(
            self.obj,
            f"a member of {pretty_repr(self.container)}",
            nullable=self.nullable,
        )


##


def ensure_not_none[T](obj: T | None, /, *, desc: str = "Object") -> T:
    """Ensure an object is not None."""
    if obj is None:
        raise EnsureNotNoneError(desc=desc)
    return obj


@dataclass(kw_only=True, slots=True)
class EnsureNotNoneError(Exception):
    desc: str = "Object"

    @override
    def __str__(self) -> str:
        return f"{self.desc} must not be None"


##


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


@dataclass(kw_only=True, slots=True)
class EnsureNumberError(Exception):
    obj: Any
    nullable: bool

    @override
    def __str__(self) -> str:
        return _make_error_msg(self.obj, "a number", nullable=self.nullable)


##


@overload
def ensure_path(obj: Any, /, *, nullable: bool) -> Path | None: ...
@overload
def ensure_path(obj: Any, /, *, nullable: Literal[False] = False) -> Path: ...
def ensure_path(obj: Any, /, *, nullable: bool = False) -> Path | None:
    """Ensure an object is a Path."""
    try:
        return ensure_class(obj, Path, nullable=nullable)
    except EnsureClassError as error:
        raise EnsurePathError(obj=error.obj, nullable=nullable) from None


@dataclass(kw_only=True, slots=True)
class EnsurePathError(Exception):
    obj: Any
    nullable: bool

    @override
    def __str__(self) -> str:
        return _make_error_msg(self.obj, "a Path", nullable=self.nullable)


##


@overload
def ensure_plain_date_time(obj: Any, /, *, nullable: bool) -> PlainDateTime | None: ...
@overload
def ensure_plain_date_time(
    obj: Any, /, *, nullable: Literal[False] = False
) -> PlainDateTime: ...
def ensure_plain_date_time(
    obj: Any, /, *, nullable: bool = False
) -> PlainDateTime | None:
    """Ensure an object is a plain date-time."""
    try:
        return ensure_class(obj, PlainDateTime, nullable=nullable)
    except EnsureClassError as error:
        raise EnsurePlainDateTimeError(obj=error.obj, nullable=nullable) from None


@dataclass(kw_only=True, slots=True)
class EnsurePlainDateTimeError(Exception):
    obj: Any
    nullable: bool

    @override
    def __str__(self) -> str:
        return _make_error_msg(self.obj, "a plain date-time", nullable=self.nullable)


##


@overload
def ensure_str(obj: Any, /, *, nullable: bool) -> str | None: ...
@overload
def ensure_str(obj: Any, /, *, nullable: Literal[False] = False) -> str: ...
def ensure_str(obj: Any, /, *, nullable: bool = False) -> str | None:
    """Ensure an object is a string."""
    try:
        return ensure_class(obj, str, nullable=nullable)
    except EnsureClassError as error:
        raise EnsureStrError(obj=error.obj, nullable=nullable) from None


@dataclass(kw_only=True, slots=True)
class EnsureStrError(Exception):
    obj: Any
    nullable: bool

    @override
    def __str__(self) -> str:
        return _make_error_msg(self.obj, "a string", nullable=self.nullable)


##


@overload
def ensure_time(obj: Any, /, *, nullable: bool) -> Time | None: ...
@overload
def ensure_time(obj: Any, /, *, nullable: Literal[False] = False) -> Time: ...
def ensure_time(obj: Any, /, *, nullable: bool = False) -> Time | None:
    """Ensure an object is a time."""
    try:
        return ensure_class(obj, Time, nullable=nullable)
    except EnsureClassError as error:
        raise EnsureTimeError(obj=error.obj, nullable=nullable) from None


@dataclass(kw_only=True, slots=True)
class EnsureTimeError(Exception):
    obj: Any
    nullable: bool

    @override
    def __str__(self) -> str:
        return _make_error_msg(self.obj, "a time", nullable=self.nullable)


##


@overload
def ensure_time_delta(obj: Any, /, *, nullable: bool) -> TimeDelta | None: ...
@overload
def ensure_time_delta(
    obj: Any, /, *, nullable: Literal[False] = False
) -> TimeDelta: ...
def ensure_time_delta(obj: Any, /, *, nullable: bool = False) -> TimeDelta | None:
    """Ensure an object is a timedelta."""
    try:
        return ensure_class(obj, TimeDelta, nullable=nullable)
    except EnsureClassError as error:
        raise EnsureTimeDeltaError(obj=error.obj, nullable=nullable) from None


@dataclass(kw_only=True, slots=True)
class EnsureTimeDeltaError(Exception):
    obj: Any
    nullable: bool

    @override
    def __str__(self) -> str:
        return _make_error_msg(self.obj, "a time-delta", nullable=self.nullable)


##


@overload
def ensure_zoned_date_time(obj: Any, /, *, nullable: bool) -> ZonedDateTime | None: ...
@overload
def ensure_zoned_date_time(
    obj: Any, /, *, nullable: Literal[False] = False
) -> ZonedDateTime: ...
def ensure_zoned_date_time(
    obj: Any, /, *, nullable: bool = False
) -> ZonedDateTime | None:
    """Ensure an object is a zoned date-time."""
    try:
        return ensure_class(obj, ZonedDateTime, nullable=nullable)
    except EnsureClassError as error:
        raise EnsureZonedDateTimeError(obj=error.obj, nullable=nullable) from None


@dataclass(kw_only=True, slots=True)
class EnsureZonedDateTimeError(Exception):
    obj: Any
    nullable: bool

    @override
    def __str__(self) -> str:
        return _make_error_msg(self.obj, "a zoned date-time", nullable=self.nullable)


##


def skip_if_optimize[**P](func: Callable[P, None], /) -> Callable[P, None]:
    """Skip a function if we are in the optimized mode."""
    if __debug__:  # pragma: no cover
        return func

    @wraps(func)
    def wrapped(*args: P.args, **kwargs: P.kwargs) -> None:
        _ = (args, kwargs)

    return wrapped


##


def yield_object_attributes(
    obj: Any,
    /,
    *,
    skip: Iterable[str] | None = None,
    static_type: type[Any] | None = None,
) -> Iterator[tuple[str, Any]]:
    """Yield all the object attributes."""
    skip = None if skip is None else set(skip)
    for name in dir(obj):
        if ((skip is None) or (name not in skip)) and (
            (static_type is None) or isinstance(getattr_static(obj, name), static_type)
        ):
            value = getattr(obj, name)
            yield name, value


##


def _make_error_msg(obj: Any, desc: str, /, *, nullable: bool = False) -> str:
    msg = f"Object {repr_str(obj)} of type {get_class_name(obj)!r} must be {desc}"
    if nullable:
        msg += " or None"
    return msg


__all__ = [
    "EnsureBoolError",
    "EnsureBytesError",
    "EnsureClassError",
    "EnsureDateError",
    "EnsureFloatError",
    "EnsureIntError",
    "EnsureMemberError",
    "EnsureNotNoneError",
    "EnsureNumberError",
    "EnsurePathError",
    "EnsurePlainDateTimeError",
    "EnsureStrError",
    "EnsureTimeDeltaError",
    "EnsureTimeError",
    "EnsureZonedDateTimeError",
    "ensure_bool",
    "ensure_bytes",
    "ensure_class",
    "ensure_date",
    "ensure_float",
    "ensure_int",
    "ensure_member",
    "ensure_not_none",
    "ensure_number",
    "ensure_path",
    "ensure_plain_date_time",
    "ensure_str",
    "ensure_time",
    "ensure_time_delta",
    "ensure_zoned_date_time",
    "skip_if_optimize",
    "yield_object_attributes",
]
