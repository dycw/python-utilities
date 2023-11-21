from __future__ import annotations

from collections.abc import Iterator
from dataclasses import fields, is_dataclass, replace
from typing import Any, ClassVar, TypeGuard, TypeVar

from typing_extensions import Protocol

from utilities.sentinel import Sentinel


class Dataclass(Protocol):
    """Protocol for `dataclass` classes."""

    __dataclass_fields__: ClassVar[dict[str, Any]]


_TDC = TypeVar("_TDC", bound=Dataclass)


def get_dataclass_class(obj: Dataclass | type[Dataclass], /) -> type[Dataclass]:
    """Get the underlying dataclass, if possible."""

    if is_dataclass_class(obj):
        return obj
    if is_dataclass_instance(obj):
        return type(obj)
    msg = f"{obj=}"
    raise NotADataClassNorADataClassInstanceError(msg)


class NotADataClassNorADataClassInstanceError(Exception):
    """Raised when an object is neither a dataclass nor an instance of one."""


def is_dataclass_class(obj: Any, /) -> TypeGuard[type[Dataclass]]:
    """Check if an object is a dataclass."""

    return isinstance(obj, type) and is_dataclass(obj)


def is_dataclass_instance(obj: Any, /) -> TypeGuard[Dataclass]:
    """Check if an object is an instance of a dataclass."""

    return (not isinstance(obj, type)) and is_dataclass(obj)


def replace_non_sentinel(obj: _TDC, **kwargs: Any) -> _TDC:
    return replace(
        obj, **{k: v for k, v in kwargs.items() if not isinstance(v, Sentinel)}
    )


def yield_field_names(obj: Dataclass | type[Dataclass], /) -> Iterator[str]:
    """Yield the field names of a dataclass."""

    for field in fields(obj):
        yield field.name


__all__ = [
    "Dataclass",
    "get_dataclass_class",
    "is_dataclass_class",
    "is_dataclass_instance",
    "NotADataClassNorADataClassInstanceError",
    "replace_non_sentinel",
    "yield_field_names",
]
