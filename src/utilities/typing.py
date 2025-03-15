from __future__ import annotations

import datetime as dt
from collections.abc import Mapping, Sequence
from itertools import chain
from pathlib import Path
from types import NoneType, UnionType
from typing import (
    Any,
    Literal,
    NamedTuple,
    Optional,  # pyright: ignore[reportDeprecated]
    Self,
    TypeAliasType,
    TypeGuard,
    Union,  # pyright: ignore[reportDeprecated]
    get_origin,
)
from typing import get_args as _get_args
from typing import get_type_hints as _get_type_hints
from uuid import UUID

from utilities.iterables import unique_everseen
from utilities.sentinel import Sentinel
from utilities.types import StrMapping


def contains_self(obj: Any, /) -> bool:
    """Check if an annotation contains `Self`."""
    return (obj is Self) or any(map(contains_self, get_args(obj)))


##


def get_args(obj: Any, /) -> tuple[Any, ...]:
    """Get the arguments of an annotation."""
    if isinstance(obj, TypeAliasType):
        return get_args(obj.__value__)
    if is_optional_type(obj):
        args = _get_args(obj)
        return tuple(a for a in args if a is not NoneType)
    return _get_args(obj)


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


def get_type_hints(
    cls: Any,
    /,
    *,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
) -> dict[str, Any]:
    """Get the type hints of an object."""
    result: dict[str, Any] = cls.__annotations__
    _ = {Literal, Path, Sentinel, StrMapping, UUID, dt}
    globalns = globals() | ({} if globalns is None else dict(globalns))
    localns = {} if localns is None else dict(localns)
    try:
        hints = _get_type_hints(cls, globalns=globalns, localns=localns)
    except NameError:
        pass
    else:
        result.update({
            key: value
            for key, value in hints.items()
            if (key not in result) or isinstance(result[key], str)
        })
    return result


##


def is_dict_type(obj: Any, /) -> bool:
    """Check if an object is a dict type annotation."""
    return _is_annotation_of_type(obj, dict)


##


def is_frozenset_type(obj: Any, /) -> bool:
    """Check if an object is a frozenset type annotation."""
    return _is_annotation_of_type(obj, frozenset)


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
    "contains_self",
    "get_literal_elements",
    "get_type_hints",
    "is_dict_type",
    "is_frozenset_type",
    "is_list_type",
    "is_literal_type",
    "is_mapping_type",
    "is_namedtuple_class",
    "is_namedtuple_instance",
    "is_optional_type",
    "is_sequence_type",
    "is_set_type",
    "is_union_type",
]
