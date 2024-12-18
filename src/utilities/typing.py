from __future__ import annotations

from collections.abc import Mapping, Sequence
from types import NoneType, UnionType
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    NamedTuple,
    Optional,  # pyright: ignore[reportDeprecated]
    Self,
    TypeGuard,
    Union,  # pyright: ignore[reportDeprecated]
    _eval_type,  # pyright: ignore[reportAttributeAccessIssue]
    _TypedDictMeta,  # pyright: ignore[reportAttributeAccessIssue]
    get_origin,
)
from typing import get_args as _get_args
from typing import get_type_hints as _get_type_hints

from utilities.errors import ImpossibleCaseError
from utilities.iterables import check_sets_equal

if TYPE_CHECKING:
    from utilities.types import StrMapping


try:  # skipif-version-ge-312
    from typing import TypeAliasType  # pyright: ignore[reportAttributeAccessIssue]
except ImportError:  # pragma: no cover
    TypeAliasType = None


def contains_self(obj: Any, /) -> bool:
    """Check if an annotation contains `Self`."""
    return (obj is Self) or any(map(contains_self, get_args(obj)))


def eval_typed_dict(
    cls: Any,
    /,
    *,
    globals_: StrMapping | None = None,
    locals_: StrMapping | None = None,
) -> Mapping[str, Any]:
    """Evaluate a typed dict."""
    return {
        k: _eval_typed_dict_one(v, globalsns=globals_, localns=locals_)
        for k, v in cls.__annotations__.items()
    }


def _eval_typed_dict_one(
    cls: Any,
    /,
    *,
    globalsns: StrMapping | None = None,
    localns: StrMapping | None = None,
) -> Any:
    """Evaluate the field of a typed dict."""
    globals_use = globals() if globalsns is None else globalsns
    locals_use = locals() if localns is None else localns
    result = _eval_type(cls, globals_use, locals_use)
    return eval_typed_dict(result) if isinstance(result, _TypedDictMeta) else result


def get_args(obj: Any, /) -> tuple[Any, ...]:
    """Get the arguments of an annotation."""
    if (TypeAliasType is not None) and isinstance(  # skipif-version-ge-312
        obj, TypeAliasType
    ):
        return get_args(obj.__value__)  # pragma: no cover
    if is_optional_type(obj):
        args = _get_args(obj)
        return tuple(a for a in args if a is not NoneType)
    return _get_args(obj)


def get_type_hints(
    cls: Any,
    /,
    *,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
) -> dict[str, Any]:
    """Get the type hints of an object."""
    try:
        first = _get_type_hints(cls)
    except NameError:
        first = None
    try:
        second = _get_type_hints(
            cls,
            globalns=globals() if globalns is None else dict(globalns),
            localns=locals() if localns is None else dict(localns),
        )
    except NameError:
        second = None
    if (first is None) and (second is None):
        return cls.__annotations__
    if (first is not None) and (second is None):
        return first
    if (first is None) and (second is not None):
        return second
    if (first is not None) and (second is not None):
        check_sets_equal(first, second)
        return {k: second[k] if isinstance(first[k], str) else second[k] for k in first}
    raise ImpossibleCaseError(case=[f"{first=}", f"{second=}"])  # pragma: no cover


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


def is_optional_type(obj: Any, /) -> bool:
    """Check if an object is an optional type annotation."""
    is_optional = _is_annotation_of_type(obj, Optional)  # pyright: ignore[reportDeprecated]
    return is_optional or (
        is_union_type(obj) and any(a is NoneType for a in _get_args(obj))
    )


def is_sequence_type(obj: Any, /) -> bool:
    """Check if an object is a sequence type annotation."""
    return _is_annotation_of_type(obj, Sequence)


def is_set_type(obj: Any, /) -> bool:
    """Check if an object is a set type annotation."""
    return _is_annotation_of_type(obj, set)


def is_union_type(obj: Any, /) -> bool:
    """Check if an object is a union type annotation."""
    is_old_union = _is_annotation_of_type(obj, Union)  # pyright: ignore[reportDeprecated]
    return is_old_union or _is_annotation_of_type(obj, UnionType)


def _is_annotation_of_type(obj: Any, origin: Any, /) -> bool:
    """Check if an object is an annotation with a given origin."""
    return (get_origin(obj) is origin) or (
        (TypeAliasType is not None)
        and isinstance(obj, TypeAliasType)
        and _is_annotation_of_type(obj.__value__, origin)
    )


__all__ = [
    "contains_self",
    "eval_typed_dict",
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
