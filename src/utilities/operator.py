from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from typing_extensions import override

import utilities.math


def is_equal(
    x: Any, y: Any, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if two objects are equal."""
    if type(x) is not type(y):
        return False
    if isinstance(x, dict):
        y = cast(dict[Any, Any], y)
        if not is_equal(set(x), set(y), rel_tol=rel_tol, abs_tol=abs_tol):
            return False
        return all(is_equal(x[i], y[i], rel_tol=rel_tol, abs_tol=abs_tol) for i in x)
    if isinstance(x, frozenset | set):
        y = cast(frozenset[Any] | set[Any], y)
        try:
            x_sorted = sorted(x)
            y_sorted = sorted(y)
        except TypeError:
            raise _IsEqualUnsortableCollectionsError(x=x, y=y) from None
        return all(
            is_equal(i, j, rel_tol=rel_tol, abs_tol=abs_tol)
            for i, j in zip(x_sorted, y_sorted, strict=True)
        )
    if isinstance(x, int | float):
        y = cast(int | float, y)
        return utilities.math.is_equal(x, y, rel_tol=rel_tol, abs_tol=abs_tol)
    return x == y


@dataclass(kw_only=True, slots=True)
class IsEqualError(Exception):
    x: Any
    y: Any


@dataclass(kw_only=True, slots=True)
class _IsEqualUnsortableCollectionsError(IsEqualError):
    @override
    def __str__(self) -> str:
        return f"Unsortable collection(s): {self.x}, {self.y}"


__all__ = ["IsEqualError", "is_equal"]
