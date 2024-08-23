from __future__ import annotations

from typing import Any, TypeVar

_T = TypeVar("_T")
_U = TypeVar("_U")


def first(pair: tuple[_T, Any], /) -> _T:
    """Get the first element in a pair."""
    return pair[0]


def identity(obj: _T, /) -> _T:
    """Return the object itself."""
    return obj


def second(pair: tuple[Any, _U], /) -> _U:
    """Get the second element in a pair."""
    return pair[1]


__all__ = ["first", "identity", "second"]
