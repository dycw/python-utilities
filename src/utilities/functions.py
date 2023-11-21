from __future__ import annotations

from typing import TypeVar

_T = TypeVar("_T")


def identity(obj: _T, /) -> _T:
    """The identity function."""
    return obj
