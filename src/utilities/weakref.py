from __future__ import annotations

from typing import TYPE_CHECKING, Any
from weakref import ReferenceType, ref

if TYPE_CHECKING:
    from collections.abc import Callable


def add_finalizer(obj: Any, callback: Callable[[], None], /) -> ReferenceType:
    """Add a finalizer for an object."""
    return ref(obj, _add_finalizer_modify(callback))


def _add_finalizer_modify(func: Callable[[], None], /) -> Callable[[Any], None]:
    """Modify a callback to work with `ref`."""

    def wrapped(_: Any, /) -> None:
        func()

    return wrapped


__all__ = ["add_finalizer"]
