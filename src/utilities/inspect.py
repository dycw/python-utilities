from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterator


def yield_object_attributes(obj: Any, /) -> Iterator[tuple[str, Any]]:
    """Yield all the object attributes."""
    for name in dir(obj):
        value = getattr(obj, name)
        yield name, value


__all__ = ["yield_object_attributes"]
