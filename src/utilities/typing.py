from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import NoneType, UnionType
from typing import Any, Literal, get_origin
from typing import get_args as _get_args

from typing_extensions import override

try:  # pragma: version-ge-312
    from typing import TypeAliasType  # type: ignore[reportAttributeAccessIssue]
except ImportError:  # pragma: no cover
    TypeAliasType = None


def get_args(obj: Any, /) -> tuple[Any, ...]:
    """Get the arguments of an annotation."""
    if (TypeAliasType is not None) and isinstance(obj, TypeAliasType):
        return get_args(obj.__value__)
    return _get_args(obj)


def get_literal_args(obj: Any, /) -> tuple[Any, ...]:
    """Get the arguments of a Literal."""
    if not is_literal_type(obj):
        raise GetLiteralArgsError(obj=obj)
    return get_args(obj)


@dataclass(kw_only=True)
class GetLiteralArgsError(Exception):
    obj: Any

    @override
    def __str__(self) -> str:
        return f"Object must be a Literal annotation; got {self.obj} instead"


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
    return is_union_type(obj) and any(a is NoneType for a in get_args(obj))


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
    "GetLiteralArgsError",
    "get_literal_args",
    "is_dict_type",
    "is_frozenset_type",
    "is_list_type",
    "is_literal_type",
    "is_mapping_type",
    "is_optional_type",
    "is_set_type",
    "is_union_type",
]
