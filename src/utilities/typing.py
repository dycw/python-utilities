from __future__ import annotations

import datetime as dt
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from itertools import chain
from pathlib import Path
from types import NoneType, UnionType
from typing import (
    Any,
    Literal,
    NamedTuple,
    Optional,  # pyright: ignore[reportDeprecated]
    TypeAliasType,
    TypeGuard,
    Union,  # pyright: ignore[reportDeprecated]
    get_origin,
    overload,
    override,
)
from typing import get_args as _get_args
from typing import get_type_hints as _get_type_hints
from uuid import UUID
from warnings import warn

from whenever import (
    Date,
    DateDelta,
    DateTimeDelta,
    PlainDateTime,
    Time,
    TimeDelta,
    ZonedDateTime,
)

from utilities.iterables import unique_everseen
from utilities.sentinel import Sentinel
from utilities.types import StrMapping


def get_args(obj: Any, /, *, optional_drop_none: bool = False) -> tuple[Any, ...]:
    """Get the arguments of an annotation."""
    if isinstance(obj, TypeAliasType):
        return get_args(obj.__value__)
    args = _get_args(obj)
    if is_optional_type(obj) and optional_drop_none:
        args = tuple(a for a in args if a is not NoneType)
    return args


##


def get_literal_elements(obj: Any, /) -> list[Any]:
    """Get the elements of a literal annotation."""
    return _get_literal_elements_inner(obj)


def _get_literal_elements_inner(obj: Any, /) -> list[Any]:
    if isinstance(obj, str | int):
        return [obj]
    args = get_args(obj)
    parts = chain.from_iterable(map(_get_literal_elements_inner, args))
    return list(unique_everseen(parts))


##


def get_type_classes(obj: Any, /) -> tuple[type[Any], ...]:
    """Get the type classes from a type/tuple/Union type."""
    types: Sequence[type[Any]] = []
    if isinstance(obj, type):
        types.append(obj)
    elif isinstance(obj, tuple):
        for arg in obj:
            if isinstance(arg, type):
                types.append(arg)
            elif isinstance(arg, tuple):
                types.extend(get_type_classes(arg))
            elif is_union_type(arg):
                types.extend(get_union_type_classes(arg))
            else:
                raise _GetTypeClassesTupleError(obj=obj, inner=arg)
    elif is_union_type(obj):
        types.extend(get_union_type_classes(obj))
    else:
        raise _GetTypeClassesTypeError(obj=obj)
    return tuple(types)


@dataclass(kw_only=True, slots=True)
class GetTypeClassesError(Exception):
    obj: Any


@dataclass(kw_only=True, slots=True)
class _GetTypeClassesTypeError(GetTypeClassesError):
    @override
    def __str__(self) -> str:
        return f"Object must be a type, tuple or Union type; got {self.obj!r} of type {type(self.obj)!r}"


@dataclass(kw_only=True, slots=True)
class _GetTypeClassesTupleError(GetTypeClassesError):
    inner: Any

    @override
    def __str__(self) -> str:
        return f"Tuple must contain types, tuples or Union types only; got {self.inner} of type {type(self.inner)!r}"


##


def get_type_hints(
    obj: Any,
    /,
    *,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
    warn_name_errors: bool = False,
) -> dict[str, Any]:
    """Get the type hints of an object."""
    result: dict[str, Any] = obj.__annotations__
    _ = {
        Date,
        DateDelta,
        DateTimeDelta,
        Literal,
        Path,
        PlainDateTime,
        Sentinel,
        StrMapping,
        Time,
        TimeDelta,
        UUID,
        ZonedDateTime,
        dt,
    }
    globalns_use = globals() | ({} if globalns is None else dict(globalns))
    localns_use = {} if localns is None else dict(localns)
    try:
        hints = _get_type_hints(obj, globalns=globalns_use, localns=localns_use)
    except NameError as error:
        if warn_name_errors:
            warn(f"Error getting type hints for {obj!r}; {error}", stacklevel=2)
    else:
        result.update({
            key: value
            for key, value in hints.items()
            if (key not in result) or isinstance(result[key], str)
        })
    return result


##


def get_union_type_classes(obj: Any, /) -> tuple[type[Any], ...]:
    """Get the type classes from a Union type."""
    if not is_union_type(obj):
        raise _GetUnionTypeClassesUnionTypeError(obj=obj)
    types_: Sequence[type[Any]] = []
    for arg in get_args(obj):
        if not isinstance(arg, type):
            raise _GetUnionTypeClassesInternalTypeError(obj=obj, inner=arg)
        types_.append(arg)
    return tuple(types_)


@dataclass(kw_only=True, slots=True)
class GetUnionTypeClassesError(Exception):
    obj: Any


@dataclass(kw_only=True, slots=True)
class _GetUnionTypeClassesUnionTypeError(GetUnionTypeClassesError):
    @override
    def __str__(self) -> str:
        return (
            f"Object must be a Union type; got {self.obj!r} of type {type(self.obj)!r}"
        )


@dataclass(kw_only=True, slots=True)
class _GetUnionTypeClassesInternalTypeError(GetUnionTypeClassesError):
    inner: Any

    @override
    def __str__(self) -> str:
        return f"Union type must contain types only; got {self.inner} of type {type(self.inner)!r}"


##


def is_dict_type(obj: Any, /) -> bool:
    """Check if an object is a dict type annotation."""
    return _is_annotation_of_type(obj, dict)


##


def is_frozenset_type(obj: Any, /) -> bool:
    """Check if an object is a frozenset type annotation."""
    return _is_annotation_of_type(obj, frozenset)


##


@overload
def is_instance_gen[T](obj: Any, type_: type[T], /) -> TypeGuard[T]: ...
@overload
def is_instance_gen[T1](obj: Any, type_: tuple[T1], /) -> TypeGuard[T1]: ...
@overload
def is_instance_gen[T1, T2](
    obj: Any, type_: tuple[T1, T2], /
) -> TypeGuard[T1 | T2]: ...
@overload
def is_instance_gen[T1, T2, T3](
    obj: Any, type_: tuple[T1, T2, T3], /
) -> TypeGuard[T1 | T2 | T3]: ...
@overload
def is_instance_gen[T1, T2, T3, T4](
    obj: Any, type_: tuple[T1, T2, T3, T4], /
) -> TypeGuard[T1 | T2 | T3 | T4]: ...
@overload
def is_instance_gen[T1, T2, T3, T4, T5](
    obj: Any, type_: tuple[T1, T2, T3, T4, T5], /
) -> TypeGuard[T1 | T2 | T3 | T4 | T5]: ...
@overload
def is_instance_gen(obj: Any, type_: Any, /) -> bool: ...
def is_instance_gen(obj: Any, type_: Any, /) -> bool:
    """Check if an instance relationship holds, except bool<int."""
    # parent
    if isinstance(type_, tuple):
        return any(is_instance_gen(obj, t) for t in type_)  # skipif-ci-and-not-windows
    if is_literal_type(type_):
        return obj in get_args(type_)
    if is_union_type(type_):
        return any(is_instance_gen(obj, t) for t in get_args(type_))
    # tuple vs tuple
    if isinstance(obj, tuple) and is_tuple_type(type_):
        type_args = get_args(type_)
        return (len(obj) == len(type_args)) and all(
            is_instance_gen(o, t) for o, t in zip(obj, type_args, strict=True)
        )
    if isinstance(obj, tuple) is not is_tuple_type(type_):
        return False
    # basic
    if isinstance(type_, type):
        return any(_is_instance_gen_type(obj, t) for t in get_type_classes(type_))
    raise IsInstanceGenError(obj=obj, type_=type_)


def _is_instance_gen_type[T](obj: Any, type_: type[T], /) -> TypeGuard[T]:
    return (
        isinstance(obj, type_)
        and not (
            isinstance(obj, bool)
            and issubclass(type_, int)
            and not issubclass(type_, bool)
        )
        and not (
            isinstance(obj, dt.datetime)
            and issubclass(type_, dt.date)
            and not issubclass(type_, dt.datetime)
        )
    )


@dataclass(kw_only=True, slots=True)
class IsInstanceGenError(Exception):
    obj: Any
    type_: Any

    @override
    def __str__(self) -> str:
        return f"Invalid arguments; got {self.obj!r} of type {type(self.obj)!r} and {self.type_!r} of type {type(self.type_)!r}"


##


def is_list_type(obj: Any, /) -> bool:
    """Check if an object is a list type annotation."""
    return _is_annotation_of_type(obj, list)


##


def is_literal_type(obj: Any, /) -> bool:
    """Check if an object is a literal type annotation."""
    return _is_annotation_of_type(obj, Literal)


##


def is_mapping_type(obj: Any, /) -> bool:
    """Check if an object is a mapping type annotation."""
    return _is_annotation_of_type(obj, Mapping)


##


def is_namedtuple_class(obj: Any, /) -> TypeGuard[type[Any]]:
    """Check if an object is a namedtuple."""
    return isinstance(obj, type) and _is_namedtuple_core(obj)


def is_namedtuple_instance(obj: Any, /) -> bool:
    """Check if an object is an instance of a dataclass."""
    return (not isinstance(obj, type)) and _is_namedtuple_core(obj)


def _is_namedtuple_core(obj: Any, /) -> bool:
    """Check if an object is an instance of a dataclass."""
    try:
        (base,) = obj.__orig_bases__
    except (AttributeError, ValueError):
        return False
    return base is NamedTuple


##


def is_optional_type(obj: Any, /) -> bool:
    """Check if an object is an optional type annotation."""
    is_optional = _is_annotation_of_type(obj, Optional)  # pyright: ignore[reportDeprecated]
    return is_optional or (
        is_union_type(obj) and any(a is NoneType for a in _get_args(obj))
    )


##


def is_sequence_type(obj: Any, /) -> bool:
    """Check if an object is a sequence type annotation."""
    return _is_annotation_of_type(obj, Sequence)


##


def is_set_type(obj: Any, /) -> bool:
    """Check if an object is a set type annotation."""
    return _is_annotation_of_type(obj, set)


##


@overload
def is_subclass_gen[T](cls: type[Any], parent: type[T], /) -> TypeGuard[type[T]]: ...
@overload
def is_subclass_gen[T1](
    cls: type[Any], parent: tuple[type[T1]], /
) -> TypeGuard[type[T1]]: ...
@overload
def is_subclass_gen[T1, T2](
    cls: type[Any], parent: tuple[type[T1], type[T2]], /
) -> TypeGuard[type[T1 | T2]]: ...
@overload
def is_subclass_gen[T1, T2, T3](
    cls: type[Any], parent: tuple[type[T1], type[T2], type[T3]], /
) -> TypeGuard[type[T1 | T2 | T3]]: ...
@overload
def is_subclass_gen[T1, T2, T3, T4](
    cls: type[Any], parent: tuple[type[T1], type[T2], type[T3], type[T4]], /
) -> TypeGuard[type[T1 | T2 | T3 | T4]]: ...
@overload
def is_subclass_gen[T1, T2, T3, T4, T5](
    cls: type[Any], parent: tuple[type[T1], type[T2], type[T3], type[T4], type[T5]], /
) -> TypeGuard[type[T1 | T2 | T3 | T4 | T5]]: ...
@overload
def is_subclass_gen(cls: Any, parent: Any, /) -> bool: ...
def is_subclass_gen(cls: Any, parent: Any, /) -> bool:
    """Generalized `issubclass`."""
    # child
    if isinstance(cls, tuple):
        return all(is_subclass_gen(c, parent) for c in cls)
    if is_literal_type(cls):
        types = tuple(map(type, get_args(cls)))
        return (
            is_literal_type(parent) and set(get_args(cls)).issubset(get_args(parent))
        ) or is_subclass_gen(types, parent)
    if is_union_type(cls):
        return all(is_subclass_gen(c, parent) for c in get_args(cls))
    # parent
    if isinstance(parent, tuple):
        return any(is_subclass_gen(cls, p) for p in parent)
    if is_literal_type(parent):
        return is_literal_type(cls) and set(get_args(cls)).issubset(get_args(parent))
    if is_union_type(parent):
        return any(is_subclass_gen(cls, p) for p in get_args(parent))
    # tuple vs tuple
    if is_tuple_type(cls) and is_tuple_type(parent):
        cls_args, parent_args = get_args(cls), get_args(parent)
        return (len(cls_args) == len(parent_args)) and all(
            is_subclass_gen(c, p) for c, p in zip(cls_args, parent_args, strict=True)
        )
    if is_tuple_type(cls) is not is_tuple_type(parent):
        return False
    # basic
    if isinstance(cls, type):
        return any(_is_subclass_gen_type(cls, p) for p in get_type_classes(parent))
    raise IsSubclassGenError(cls=cls)


def _is_subclass_gen_type[T](cls: type[Any], parent: type[T], /) -> TypeGuard[type[T]]:
    return (
        issubclass(cls, parent)
        and not (
            issubclass(cls, bool)
            and issubclass(parent, int)
            and not issubclass(parent, bool)
        )
        and not (
            issubclass(cls, dt.datetime)
            and issubclass(parent, dt.date)
            and not issubclass(parent, dt.datetime)
        )
    )


@dataclass(kw_only=True, slots=True)
class IsSubclassGenError(Exception):
    cls: Any

    @override
    def __str__(self) -> str:
        return f"Argument must be a class; got {self.cls!r} of type {type(self.cls)!r}"


##


def is_tuple_type(obj: Any, /) -> bool:
    """Check if an object is a tuple type annotation."""
    return _is_annotation_of_type(obj, tuple)


##


def is_union_type(obj: Any, /) -> bool:
    """Check if an object is a union type annotation."""
    is_old_union = _is_annotation_of_type(obj, Union)  # pyright: ignore[reportDeprecated]
    return is_old_union or _is_annotation_of_type(obj, UnionType)


##


def _is_annotation_of_type(obj: Any, origin: Any, /) -> bool:
    """Check if an object is an annotation with a given origin."""
    return (get_origin(obj) is origin) or (
        isinstance(obj, TypeAliasType) and _is_annotation_of_type(obj.__value__, origin)
    )


__all__ = [
    "GetTypeClassesError",
    "GetUnionTypeClassesError",
    "IsInstanceGenError",
    "IsSubclassGenError",
    "get_literal_elements",
    "get_type_classes",
    "get_type_hints",
    "get_union_type_classes",
    "is_dict_type",
    "is_frozenset_type",
    "is_instance_gen",
    "is_list_type",
    "is_literal_type",
    "is_mapping_type",
    "is_namedtuple_class",
    "is_namedtuple_instance",
    "is_optional_type",
    "is_sequence_type",
    "is_set_type",
    "is_subclass_gen",
    "is_tuple_type",
    "is_union_type",
]
