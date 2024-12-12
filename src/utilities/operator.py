from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from collections.abc import Set as AbstractSet
from dataclasses import asdict, dataclass
from typing import Any, TypeVar, cast

from typing_extensions import override

import utilities.math
from utilities.dataclasses import Dataclass, is_dataclass_instance
from utilities.math import sort_floats

_T = TypeVar("_T")


def is_equal(
    x: Any, y: Any, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if two objects are equal."""
    if type(x) is not type(y):
        return False

    # singletons
    if isinstance(x, int | float):
        y = cast(int | float, y)
        return utilities.math.is_equal(x, y, rel_tol=rel_tol, abs_tol=abs_tol)
    if isinstance(x, str):  # else Sequence
        y = cast(str, y)
        return x == y
    if is_dataclass_instance(x):
        y = cast(Dataclass, y)
        x_values = asdict(x)
        y_values = asdict(y)
        return is_equal(x_values, y_values)
    # collections
    if isinstance(x, Mapping):
        y = cast(Mapping[Any, Any], y)
        x_keys = set(x)
        y_keys = set(y)
        if not is_equal(x_keys, y_keys, rel_tol=rel_tol, abs_tol=abs_tol):
            return False
        x_values = [x[i] for i in x]
        y_values = [y[i] for i in x]
        return is_equal(x_values, y_values, rel_tol=rel_tol, abs_tol=abs_tol)
    if isinstance(x, AbstractSet):
        y = cast(AbstractSet[Any], y)
        if all(isinstance(x_i, float) for x_i in x):
            x_sorted = sort_floats(x)
        try:
            x_sorted = sorted(x)
            y_sorted = sorted(y)
        except TypeError:
            raise _IsEqualUnsortableSetError(x=x, y=y) from None
        return is_equal(x_sorted, y_sorted, rel_tol=rel_tol, abs_tol=abs_tol)
    if isinstance(x, Sequence):
        y = cast(Sequence[Any], y)
        if len(x) != len(y):
            return False
        return all(
            is_equal(x_i, y_i, rel_tol=rel_tol, abs_tol=abs_tol)
            for x_i, y_i in zip(x, y, strict=True)
        )
    return x == y


def _try_sort(x: Iterable[_T], /) -> list[_T]:
    floats: list[float] = []
    non_floats: list[Any] = []
    for x_i in x:
        if isinstance(x_i, float):
            floats.append(x_i)
        else:
            non_floats.append(x_i)
    try:
        non_floats_sorted = sorted(non_floats)
    except TypeError:
        raise _IsEqualUnsortableSetError(x=non_floats) from None


@dataclass(kw_only=True, slots=True)
class IsEqualError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _IsEqualUnsortableSetError(IsEqualError):
    x: Iterable[Any]

    @override
    def __str__(self) -> str:
        return f"Unsortable collection(s): {self.x}, {self.y}"


__all__ = ["IsEqualError", "is_equal"]
