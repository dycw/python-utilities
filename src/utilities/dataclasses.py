from __future__ import annotations

from dataclasses import fields, is_dataclass, replace
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    TypeGuard,
    TypeVar,
    get_type_hints,
    runtime_checkable,
)

from typing_extensions import Protocol

from utilities.iterables import always_iterable
from utilities.sentinel import Sentinel

if TYPE_CHECKING:
    from collections.abc import Iterator

    from utilities.types import StrMapping


@runtime_checkable
class Dataclass(Protocol):
    """Protocol for `dataclass` classes."""

    __dataclass_fields__: ClassVar[dict[str, Any]]


def extend_non_sentinel(obj: _T, **kwargs: Any) -> _T:
    """Extend attributes on a dataclass, filtering out sentinel values."""
    return replace_non_sentinel(
        obj,
        **{
            k: list(chain(getattr(obj, k), always_iterable(v)))
            for k, v in kwargs.items()
            if not isinstance(v, Sentinel)
        },
    )


def get_dataclass_class(obj: Dataclass | type[Dataclass], /) -> type[Dataclass]:
    """Get the underlying dataclass, if possible."""
    if is_dataclass_class(obj):
        return obj
    if is_dataclass_instance(obj):
        return type(obj)
    msg = f"{obj=}"
    raise GetDataClassClassError(msg)


class GetDataClassClassError(Exception): ...


def get_dataclass_fields(
    cls: type[Dataclass],
    /,
    *,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
) -> StrMapping:
    """Get the fields of a dataclass."""
    return get_type_hints(
        cls,
        globalns=globals() if globalns is None else dict(globalns),
        localns=locals() if localns is None else dict(localns),
    )


def is_dataclass_class(obj: Any, /) -> TypeGuard[type[Dataclass]]:
    """Check if an object is a dataclass."""
    return isinstance(obj, type) and is_dataclass(obj)


def is_dataclass_instance(obj: Any, /) -> TypeGuard[Dataclass]:
    """Check if an object is an instance of a dataclass."""
    return (not isinstance(obj, type)) and is_dataclass(obj)


_T = TypeVar("_T", bound=Dataclass)


def replace_non_sentinel(obj: _T, **kwargs: Any) -> _T:
    """Replace attributes on a dataclass, filtering out sentinel values."""
    return replace(
        obj, **{k: v for k, v in kwargs.items() if not isinstance(v, Sentinel)}
    )


def yield_field_names(obj: Dataclass | type[Dataclass], /) -> Iterator[str]:
    """Yield the field names of a dataclass."""
    for field in fields(obj):
        yield field.name


__all__ = [
    "Dataclass",
    "GetDataClassClassError",
    "extend_non_sentinel",
    "get_dataclass_class",
    "get_dataclass_fields",
    "is_dataclass_class",
    "is_dataclass_instance",
    "replace_non_sentinel",
    "yield_field_names",
]
