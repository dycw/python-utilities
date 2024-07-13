from __future__ import annotations

from dataclasses import dataclass
from types import UnionType
from typing import Any, Literal, get_args, get_origin

from typing_extensions import override

try:  # pragma: version-ge-312
    from typing import TypeAliasType  # type: ignore[reportAttributeAccessIssue]
except ImportError:  # pragma: no cover
    TypeAliasType = None


def get_literal_args(obj: Any, /) -> tuple[Any, ...]:
    """Get the arguments of a Literal."""
    if not is_literal_type(obj):
        raise GetLiteralArgsError(obj=obj)
    if (TypeAliasType is not None) and isinstance(obj, TypeAliasType):
        return get_literal_args(obj.__value__)
    return get_args(obj)


@dataclass(kw_only=True)
class GetLiteralArgsError(Exception):
    obj: Any

    @override
    def __str__(self) -> str:
        return f"Object must be a Literal; got {self.obj} instead"


def is_frozenset_type(obj: Any, /) -> bool:
    """Check if an object is a frozenset annotation."""
    return _is_annotation_with_origin(obj, frozenset)


def is_list_type(obj: Any, /) -> bool:
    """Check if an object is a list annotation."""
    return _is_annotation_with_origin(obj, list)


def is_literal_type(obj: Any, /) -> bool:
    """Check if an object is a Literal annotation."""
    return _is_annotation_with_origin(obj, Literal)


def is_set_type(obj: Any, /) -> bool:
    """Check if an object is a set annotation."""
    return _is_annotation_with_origin(obj, set)


def is_union_type(obj: Any, /) -> bool:
    """Check if an object is a union type."""
    return _is_annotation_with_origin(obj, UnionType)


def _is_annotation_with_origin(obj: Any, origin: Any, /) -> bool:
    """Check if an object is an annotation with a given origin."""
    return (get_origin(obj) is origin) or (
        (TypeAliasType is not None)
        and isinstance(obj, TypeAliasType)
        and _is_annotation_with_origin(obj.__value__, origin)
    )


__all__ = [
    "GetLiteralArgsError",
    "get_literal_args",
    "is_frozenset_type",
    "is_literal_type",
]
