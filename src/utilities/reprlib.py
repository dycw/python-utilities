from __future__ import annotations

import reprlib
from typing import Any


def get_repr_and_class(obj: Any, /) -> str:
    """Get the `reprlib`-representation & class of an object."""
    return f"Object {reprlib.repr(obj)!r} of type {type(obj).__name__!r}"


__all__ = ["get_repr_and_class"]
