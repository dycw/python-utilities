from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar
from weakref import ReferenceType, ref

if TYPE_CHECKING:
    from collections.abc import Callable

_T = TypeVar("_T")


def add_finalizer(obj: _T, callback: Callable[[], None], /) -> ReferenceType[_T]:
    """Add a finalizer for an object."""
    return ref(obj, _add_finalizer_modify(callback))


def _add_finalizer_modify(func: Callable[[], None], /) -> Callable[[Any], None]:
    """Modify a callback to work with `ref`."""

    def wrapped(_: Any, /) -> None:
        func()

    return wrapped


__all__ = ["add_finalizer"]
