from __future__ import annotations

from collections.abc import Mapping, Sequence
from types import NoneType, UnionType
from typing import Any, Literal, get_origin
from typing import get_args as _get_args

try:  # pragma: version-ge-312
    from typing import TypeAliasType  # pyright: ignore[reportAttributeAccessIssue]
except ImportError:  # pragma: no cover
    TypeAliasType = None


def get_args(obj: Any, /) -> tuple[Any, ...]:
    """Get the arguments of an annotation."""
    if (TypeAliasType is not None) and isinstance(  # pragma: version-ge-312
        obj, TypeAliasType
    ):
        return get_args(obj.__value__)  # pragma: no cover
    if is_optional_type(obj):
        args = _get_args(obj)
        return tuple(a for a in args if a is not NoneType)
    return _get_args(obj)


def is_dict_type(obj: Any, /) -> bool:
    """Check if an object is a dict type annotation."""
    return _is_annotation_of_type(obj, dict)


def is_frozenset_type(obj: Any, /) -> bool:
    """Check if an object is a frozenset type annotation."""
    return _is_annotation_of_type(obj, frozenset)


def is_list_type(obj: Any, /) -> bool:
    """Check if an object is a list type annotation."""
    return _is_annotation_of_type(obj, list)


def is_literal_type(obj: Any, /) -> bool:
    """Check if an object is a literal type annotation."""
    return _is_annotation_of_type(obj, Literal)


def is_mapping_type(obj: Any, /) -> bool:
    """Check if an object is a mapping type annotation."""
    return _is_annotation_of_type(obj, Mapping)


def is_optional_type(obj: Any, /) -> bool:
    """Check if an object is an optional type annotation."""
    return is_union_type(obj) and any(a is NoneType for a in _get_args(obj))


def is_sequence_type(obj: Any, /) -> bool:
    """Check if an object is a sequence type annotation."""
    return _is_annotation_of_type(obj, Sequence)


def is_set_type(obj: Any, /) -> bool:
    """Check if an object is a set type annotation."""
    return _is_annotation_of_type(obj, set)


def is_union_type(obj: Any, /) -> bool:
    """Check if an object is a union type annotation."""
    return _is_annotation_of_type(obj, UnionType)


def _is_annotation_of_type(obj: Any, origin: Any, /) -> bool:
    """Check if an object is an annotation with a given origin."""
    return (get_origin(obj) is origin) or (
        (TypeAliasType is not None)
        and isinstance(obj, TypeAliasType)
        and _is_annotation_of_type(obj.__value__, origin)
    )


__all__ = [
    "is_dict_type",
    "is_frozenset_type",
    "is_list_type",
    "is_literal_type",
    "is_mapping_type",
    "is_optional_type",
    "is_sequence_type",
    "is_set_type",
    "is_union_type",
]
