from __future__ import annotations

import datetime as dt
from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import asdict, dataclass, is_dataclass
from functools import _lru_cache_wrapper, cached_property, partial, reduce, wraps
from inspect import getattr_static
from pathlib import Path
from re import findall
from types import (
    BuiltinFunctionType,
    FunctionType,
    MethodDescriptorType,
    MethodType,
    MethodWrapperType,
    WrapperDescriptorType,
)
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    TypeGuard,
    TypeVar,
    cast,
    overload,
    override,
)

from typing_extensions import ParamSpec

from utilities.reprlib import get_repr, get_repr_and_class
from utilities.sentinel import Sentinel, sentinel
from utilities.types import (
    Dataclass,
    Number,
    StrMapping,
    TCallable,
    TCallable1,
    TCallable2,
    TSupportsRichComparison,
    TupleOrStrMapping,
    TypeLike,
)

if TYPE_CHECKING:
    from collections.abc import Container, Hashable, Sized


_P = ParamSpec("_P")
_T = TypeVar("_T")
_T1 = TypeVar("_T1")
_T2 = TypeVar("_T2")
_T3 = TypeVar("_T3")
_T4 = TypeVar("_T4")
_T5 = TypeVar("_T5")
_U = TypeVar("_U")


##


def apply_decorators(
    func: TCallable1, /, *decorators: Callable[[TCallable2], TCallable2]
) -> TCallable1:
    """Apply a set of decorators to a function."""
    return reduce(_apply_decorators_one, decorators, func)


def _apply_decorators_one(acc: TCallable, el: Callable[[Any], Any], /) -> TCallable:
    return el(acc)


##


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
@overload
def ensure_class(obj: Any, cls: TypeLike[_T], /, *, nullable: bool = False) -> Any: ...
def ensure_class(obj: Any, cls: TypeLike[_T], /, *, nullable: bool = False) -> Any:
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
def ensure_date(obj: Any, /, *, nullable: bool) -> dt.date | None: ...
@overload
def ensure_date(obj: Any, /, *, nullable: Literal[False] = False) -> dt.date: ...
def ensure_date(obj: Any, /, *, nullable: bool = False) -> dt.date | None:
    """Ensure an object is a date."""
    try:
        return ensure_class(obj, dt.date, nullable=nullable)
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
        raise EnsureDateTimeError(obj=error.obj, nullable=nullable) from None


@dataclass(kw_only=True, slots=True)
class EnsureDateTimeError(Exception):
    obj: Any
    nullable: bool

    @override
    def __str__(self) -> str:
        return _make_error_msg(self.obj, "a datetime", nullable=self.nullable)


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


def ensure_hashable(obj: Any, /) -> Hashable:
    """Ensure an object is hashable."""
    if is_hashable(obj):
        return obj
    raise EnsureHashableError(obj=obj)


@dataclass(kw_only=True, slots=True)
class EnsureHashableError(Exception):
    obj: Any

    @override
    def __str__(self) -> str:
        return _make_error_msg(self.obj, "hashable")


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


@dataclass(kw_only=True, slots=True)
class EnsureMemberError(Exception):
    obj: Any
    container: Container[Any]
    nullable: bool

    @override
    def __str__(self) -> str:
        return _make_error_msg(
            self.obj, f"a member of {get_repr(self.container)}", nullable=self.nullable
        )


##


def ensure_not_none(obj: _T | None, /, *, desc: str = "Object") -> _T:
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


def ensure_sized(obj: Any, /) -> Sized:
    """Ensure an object is sized."""
    if is_sized(obj):
        return obj
    raise EnsureSizedError(obj=obj)


@dataclass(kw_only=True, slots=True)
class EnsureSizedError(Exception):
    obj: Any

    @override
    def __str__(self) -> str:
        return _make_error_msg(self.obj, "sized")


##


def ensure_sized_not_str(obj: Any, /) -> Sized:
    """Ensure an object is sized, but not a string."""
    if is_sized_not_str(obj):
        return obj
    raise EnsureSizedNotStrError(obj=obj)


@dataclass(kw_only=True, slots=True)
class EnsureSizedNotStrError(Exception):
    obj: Any

    @override
    def __str__(self) -> str:
        return _make_error_msg(self.obj, "sized and not a string")


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
def ensure_time(obj: Any, /, *, nullable: bool) -> dt.time | None: ...
@overload
def ensure_time(obj: Any, /, *, nullable: Literal[False] = False) -> dt.time: ...
def ensure_time(obj: Any, /, *, nullable: bool = False) -> dt.time | None:
    """Ensure an object is a time."""
    try:
        return ensure_class(obj, dt.time, nullable=nullable)
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
def ensure_timedelta(obj: Any, /, *, nullable: bool) -> dt.timedelta | None: ...
@overload
def ensure_timedelta(
    obj: Any, /, *, nullable: Literal[False] = False
) -> dt.timedelta: ...
def ensure_timedelta(obj: Any, /, *, nullable: bool = False) -> dt.timedelta | None:
    """Ensure an object is a timedelta."""
    try:
        return ensure_class(obj, dt.timedelta, nullable=nullable)
    except EnsureClassError as error:
        raise EnsureTimeDeltaError(obj=error.obj, nullable=nullable) from None


@dataclass(kw_only=True, slots=True)
class EnsureTimeDeltaError(Exception):
    obj: Any
    nullable: bool

    @override
    def __str__(self) -> str:
        return _make_error_msg(self.obj, "a timedelta", nullable=self.nullable)


##


def first(pair: tuple[_T, Any], /) -> _T:
    """Get the first element in a pair."""
    return pair[0]


##


@overload
def get_class(obj: type[_T], /) -> type[_T]: ...
@overload
def get_class(obj: _T, /) -> type[_T]: ...
def get_class(obj: _T | type[_T], /) -> type[_T]:
    """Get the class of an object, unless it is already a class."""
    return obj if isinstance(obj, type) else type(obj)


##


def get_class_name(obj: Any, /, *, qual: bool = False) -> str:
    """Get the name of the class of an object, unless it is already a class."""
    cls = get_class(obj)
    return f"{cls.__module__}.{cls.__qualname__}" if qual else cls.__name__


##


def get_func_name(obj: Callable[..., Any], /) -> str:
    """Get the name of a callable."""
    if isinstance(obj, BuiltinFunctionType):
        return obj.__name__
    if isinstance(obj, FunctionType):
        name = obj.__name__
        pattern = r"^.+\.([A-Z]\w+\." + name + ")$"
        try:
            (full_name,) = findall(pattern, obj.__qualname__)
        except ValueError:
            return name
        return full_name
    if isinstance(obj, MethodType):
        return f"{get_class_name(obj.__self__)}.{obj.__name__}"
    if isinstance(
        obj,
        MethodType | MethodDescriptorType | MethodWrapperType | WrapperDescriptorType,
    ):
        return obj.__qualname__
    if isinstance(obj, _lru_cache_wrapper):
        return cast("Any", obj).__name__
    if isinstance(obj, partial):
        return get_func_name(obj.func)
    return get_class_name(obj)


##


def get_func_qualname(obj: Callable[..., Any], /) -> str:
    """Get the qualified name of a callable."""
    if isinstance(
        obj, BuiltinFunctionType | FunctionType | MethodType | _lru_cache_wrapper
    ):
        return f"{obj.__module__}.{obj.__qualname__}"
    if isinstance(
        obj, MethodDescriptorType | MethodWrapperType | WrapperDescriptorType
    ):
        return f"{obj.__objclass__.__module__}.{obj.__qualname__}"
    if isinstance(obj, partial):
        return get_func_qualname(obj.func)
    return f"{obj.__module__}.{get_class_name(obj)}"


##


def identity(obj: _T, /) -> _T:
    """Return the object itself."""
    return obj


##


def is_dataclass_class(obj: Any, /) -> TypeGuard[type[Dataclass]]:
    """Check if an object is a dataclass."""
    return isinstance(obj, type) and is_dataclass(obj)


##


def is_dataclass_instance(obj: Any, /) -> TypeGuard[Dataclass]:
    """Check if an object is an instance of a dataclass."""
    return (not isinstance(obj, type)) and is_dataclass(obj)


##


def is_hashable(obj: Any, /) -> TypeGuard[Hashable]:
    """Check if an object is hashable."""
    try:
        _ = hash(obj)
    except TypeError:
        return False
    return True


##


@overload
def is_iterable_of(obj: Any, cls: type[_T], /) -> TypeGuard[Iterable[_T]]: ...
@overload
def is_iterable_of(obj: Any, cls: tuple[type[_T1]], /) -> TypeGuard[Iterable[_T1]]: ...
@overload
def is_iterable_of(
    obj: Any, cls: tuple[type[_T1], type[_T2]], /
) -> TypeGuard[Iterable[_T1 | _T2]]: ...
@overload
def is_iterable_of(
    obj: Any, cls: tuple[type[_T1], type[_T2], type[_T3]], /
) -> TypeGuard[Iterable[_T1 | _T2 | _T3]]: ...
@overload
def is_iterable_of(
    obj: Any, cls: tuple[type[_T1], type[_T2], type[_T3], type[_T4]], /
) -> TypeGuard[Iterable[_T1 | _T2 | _T3 | _T4]]: ...
@overload
def is_iterable_of(
    obj: Any, cls: tuple[type[_T1], type[_T2], type[_T3], type[_T4], type[_T5]], /
) -> TypeGuard[Iterable[_T1 | _T2 | _T3 | _T4 | _T5]]: ...
@overload
def is_iterable_of(obj: Any, cls: TypeLike[_T], /) -> TypeGuard[Iterable[_T]]: ...
def is_iterable_of(obj: Any, cls: TypeLike[_T], /) -> TypeGuard[Iterable[_T]]:
    """Check if an object is a iterable of tuple or string mappings."""
    return isinstance(obj, Iterable) and all(map(make_isinstance(cls), obj))


##


def is_none(obj: Any, /) -> bool:
    """Check if an object is `None`."""
    return obj is None


##


def is_not_none(obj: Any, /) -> bool:
    """Check if an object is not `None`."""
    return obj is not None


##


@overload
def is_sequence_of(obj: Any, cls: type[_T], /) -> TypeGuard[Sequence[_T]]: ...
@overload
def is_sequence_of(obj: Any, cls: tuple[type[_T1]], /) -> TypeGuard[Sequence[_T1]]: ...
@overload
def is_sequence_of(
    obj: Any, cls: tuple[type[_T1], type[_T2]], /
) -> TypeGuard[Sequence[_T1 | _T2]]: ...
@overload
def is_sequence_of(
    obj: Any, cls: tuple[type[_T1], type[_T2], type[_T3]], /
) -> TypeGuard[Sequence[_T1 | _T2 | _T3]]: ...
@overload
def is_sequence_of(
    obj: Any, cls: tuple[type[_T1], type[_T2], type[_T3], type[_T4]], /
) -> TypeGuard[Sequence[_T1 | _T2 | _T3 | _T4]]: ...
@overload
def is_sequence_of(
    obj: Any, cls: tuple[type[_T1], type[_T2], type[_T3], type[_T4], type[_T5]], /
) -> TypeGuard[Sequence[_T1 | _T2 | _T3 | _T4 | _T5]]: ...
@overload
def is_sequence_of(obj: Any, cls: TypeLike[_T], /) -> TypeGuard[Sequence[_T]]: ...
def is_sequence_of(obj: Any, cls: TypeLike[_T], /) -> TypeGuard[Sequence[_T]]:
    """Check if an object is a sequence of tuple or string mappings."""
    return isinstance(obj, Sequence) and is_iterable_of(obj, cls)


##


def is_sequence_of_tuple_or_str_mapping(
    obj: Any, /
) -> TypeGuard[Sequence[TupleOrStrMapping]]:
    """Check if an object is a sequence of tuple or string mappings."""
    return isinstance(obj, Sequence) and all(map(is_tuple_or_str_mapping, obj))


##


def is_sized(obj: Any, /) -> TypeGuard[Sized]:
    """Check if an object is sized."""
    try:
        _ = len(obj)
    except TypeError:
        return False
    return True


##


def is_sized_not_str(obj: Any, /) -> TypeGuard[Sized]:
    """Check if an object is sized, but not a string."""
    return is_sized(obj) and not isinstance(obj, str)


##


def is_string_mapping(obj: Any, /) -> TypeGuard[StrMapping]:
    """Check if an object is a string mapping."""
    return isinstance(obj, dict) and is_iterable_of(obj, str)


##


def is_tuple(obj: Any, /) -> TypeGuard[tuple[Any, ...]]:
    """Check if an object is a tuple or string mapping."""
    return make_isinstance(tuple)(obj)


##


def is_tuple_or_str_mapping(obj: Any, /) -> TypeGuard[TupleOrStrMapping]:
    """Check if an object is a tuple or string mapping."""
    return is_tuple(obj) or is_string_mapping(obj)


##


@overload
def make_isinstance(cls: type[_T], /) -> Callable[[Any], TypeGuard[_T]]: ...
@overload
def make_isinstance(cls: tuple[type[_T1]], /) -> Callable[[Any], TypeGuard[_T1]]: ...
@overload
def make_isinstance(
    cls: tuple[type[_T1], type[_T2]], /
) -> Callable[[Any], TypeGuard[_T1 | _T2]]: ...
@overload
def make_isinstance(
    cls: tuple[type[_T1], type[_T2], type[_T3]], /
) -> Callable[[Any], TypeGuard[_T1 | _T2 | _T3]]: ...
@overload
def make_isinstance(
    cls: tuple[type[_T1], type[_T2], type[_T3], type[_T4]], /
) -> Callable[[Any], TypeGuard[_T1 | _T2 | _T3 | _T4]]: ...
@overload
def make_isinstance(
    cls: tuple[type[_T1], type[_T2], type[_T3], type[_T4], type[_T5]], /
) -> Callable[[Any], TypeGuard[_T1 | _T2 | _T3 | _T4 | _T5]]: ...
@overload
def make_isinstance(cls: TypeLike[_T], /) -> Callable[[Any], TypeGuard[_T]]: ...
def make_isinstance(cls: TypeLike[_T], /) -> Callable[[Any], TypeGuard[_T]]:
    """Make a curried `isinstance` function."""
    return partial(_make_instance_core, cls=cls)


def _make_instance_core(obj: Any, /, *, cls: TypeLike[_T]) -> TypeGuard[_T]:
    return isinstance(obj, cls)


##


def map_object(
    func: Callable[[Any], Any],
    obj: _T,
    /,
    *,
    before: Callable[[Any], Any] | None = None,
) -> _T:
    """Map a function over an object, across a variety of structures."""
    if before is not None:
        obj = before(obj)
    match obj:
        case dict():
            return type(obj)({
                k: map_object(func, v, before=before) for k, v in obj.items()
            })
        case frozenset() | list() | set() | tuple():
            return type(obj)(map_object(func, i, before=before) for i in obj)
        case Dataclass():
            return map_object(func, asdict(obj), before=before)
        case _:
            return func(obj)


##


@overload
def min_nullable(
    iterable: Iterable[TSupportsRichComparison | None], /, *, default: Sentinel = ...
) -> TSupportsRichComparison: ...
@overload
def min_nullable(
    iterable: Iterable[TSupportsRichComparison | None], /, *, default: _U = ...
) -> TSupportsRichComparison | _U: ...
def min_nullable(
    iterable: Iterable[TSupportsRichComparison | None],
    /,
    *,
    default: _U | Sentinel = sentinel,
) -> TSupportsRichComparison | _U:
    """Compute the minimum of a set of values; ignoring nulls."""
    values = (i for i in iterable if i is not None)
    if isinstance(default, Sentinel):
        try:
            return min(values)
        except ValueError:
            raise MinNullableError(values=values) from None
    return min(values, default=default)


@dataclass(kw_only=True, slots=True)
class MinNullableError(Exception, Generic[TSupportsRichComparison]):
    values: Iterable[TSupportsRichComparison]

    @override
    def __str__(self) -> str:
        return "Minimum of an all-None iterable is undefined"


@overload
def max_nullable(
    iterable: Iterable[TSupportsRichComparison | None], /, *, default: Sentinel = ...
) -> TSupportsRichComparison: ...
@overload
def max_nullable(
    iterable: Iterable[TSupportsRichComparison | None], /, *, default: _U = ...
) -> TSupportsRichComparison | _U: ...
def max_nullable(
    iterable: Iterable[TSupportsRichComparison | None],
    /,
    *,
    default: _U | Sentinel = sentinel,
) -> TSupportsRichComparison | _U:
    """Compute the maximum of a set of values; ignoring nulls."""
    values = (i for i in iterable if i is not None)
    if isinstance(default, Sentinel):
        try:
            return max(values)
        except ValueError:
            raise MaxNullableError(values=values) from None
    return max(values, default=default)


@dataclass(kw_only=True, slots=True)
class MaxNullableError(Exception, Generic[TSupportsRichComparison]):
    values: Iterable[TSupportsRichComparison]

    @override
    def __str__(self) -> str:
        return "Maximum of an all-None iterable is undefined"


##


def not_func(func: Callable[_P, bool], /) -> Callable[_P, bool]:
    """Lift a boolean-valued function to return its conjugation."""

    @wraps(func)
    def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> bool:
        return not func(*args, **kwargs)

    return wrapped


##


def second(pair: tuple[Any, _U], /) -> _U:
    """Get the second element in a pair."""
    return pair[1]


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


def yield_object_properties(
    obj: Any, /, *, skip: Iterable[str] | None = None
) -> Iterator[tuple[str, Any]]:
    """Yield all the object properties."""
    yield from yield_object_attributes(obj, skip=skip, static_type=property)


def yield_object_cached_properties(
    obj: Any, /, *, skip: Iterable[str] | None = None
) -> Iterator[tuple[str, Any]]:
    """Yield all the object cached properties."""
    yield from yield_object_attributes(obj, skip=skip, static_type=cached_property)


##


def _make_error_msg(obj: Any, desc: str, /, *, nullable: bool = False) -> str:
    msg = f"{get_repr_and_class(obj)} must be {desc}"
    if nullable:
        msg += " or None"
    return msg


__all__ = [
    "EnsureBoolError",
    "EnsureBytesError",
    "EnsureClassError",
    "EnsureDateError",
    "EnsureDateTimeError",
    "EnsureFloatError",
    "EnsureHashableError",
    "EnsureIntError",
    "EnsureMemberError",
    "EnsureNotNoneError",
    "EnsureNumberError",
    "EnsurePathError",
    "EnsureSizedError",
    "EnsureSizedNotStrError",
    "EnsureStrError",
    "EnsureTimeDeltaError",
    "EnsureTimeError",
    "MaxNullableError",
    "MinNullableError",
    "apply_decorators",
    "ensure_bool",
    "ensure_bytes",
    "ensure_class",
    "ensure_date",
    "ensure_datetime",
    "ensure_float",
    "ensure_hashable",
    "ensure_int",
    "ensure_member",
    "ensure_not_none",
    "ensure_number",
    "ensure_path",
    "ensure_sized",
    "ensure_sized_not_str",
    "ensure_str",
    "ensure_time",
    "ensure_timedelta",
    "first",
    "get_class",
    "get_class_name",
    "get_func_name",
    "get_func_qualname",
    "identity",
    "is_dataclass_class",
    "is_dataclass_instance",
    "is_hashable",
    "is_iterable_of",
    "is_none",
    "is_not_none",
    "is_sequence_of_tuple_or_str_mapping",
    "is_sized",
    "is_sized_not_str",
    "is_string_mapping",
    "is_tuple",
    "is_tuple_or_str_mapping",
    "make_isinstance",
    "map_object",
    "max_nullable",
    "min_nullable",
    "not_func",
    "second",
    "yield_object_attributes",
    "yield_object_cached_properties",
    "yield_object_properties",
]
