from __future__ import annotations

from dataclasses import is_dataclass
from typing import Any


def is_dataclass_instance(obj: Any, /) -> bool:
    """Check if an object is an instance of a dataclass."""

    return (not isinstance(obj, type)) and is_dataclass(obj)


__all__ = [
    "is_dataclass_instance",
]
