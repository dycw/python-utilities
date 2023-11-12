from __future__ import annotations

from collections.abc import Iterator
from dataclasses import fields, is_dataclass
from typing import Any


def get_dataclass_class(obj: Any, /) -> bool:
    """Get the underlying dataclass, if possible."""

    if is_dataclass_class(obj):
        return obj
    if is_dataclass_instance(obj):
        return type(obj)
    msg = f"{obj=}"
    raise NotADataClassNorADataClassInstanceError(msg)


class NotADataClassNorADataClassInstanceError(TypeError):
    """Raised when an object is neither a dataclass nor an instance of one."""


def is_dataclass_class(obj: Any, /) -> bool:
    """Check if an object is a dataclass."""

    return isinstance(obj, type) and is_dataclass(obj)


def is_dataclass_instance(obj: Any, /) -> bool:
    """Check if an object is an instance of a dataclass."""

    return (not isinstance(obj, type)) and is_dataclass(obj)


def yield_field_names(obj: Any, /) -> Iterator[str]:
    """Yield the field names of a dataclass."""

    for field in fields(obj):
        yield field.name


__all__ = [
    "get_dataclass_class",
    "is_dataclass_class",
    "is_dataclass_instance",
    "NotADataClassNorADataClassInstanceError",
    "yield_field_names",
]
