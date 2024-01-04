from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from typing import ParamSpec, TypeVar

from beartype import beartype

from utilities.pytest import is_pytest

_P = ParamSpec("_P")
_R = TypeVar("_R")


def beartype_if_pytest(func: Callable[_P, _R], /) -> Callable[_P, _R]:
    """Apply `beartype` to a function only if `pytest` is running."""

    safe = beartype(func)

    @wraps(func)
    def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        func_use = safe if is_pytest() else func
        return func_use(*args, **kwargs)

    return wrapped


__all__ = ["beartype_if_pytest"]
