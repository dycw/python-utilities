from __future__ import annotations

import reprlib
from collections.abc import Iterable, Mapping, Sequence
from collections.abc import Set as AbstractSet
from dataclasses import asdict, dataclass
from typing import Any, Generic, TypeVar, cast

from typing_extensions import override

import utilities.math
from utilities.dataclasses import Dataclass, is_dataclass_instance
from utilities.iterables import SortIterableError, sort_iterable

_T = TypeVar("_T")


def is_equal(
    x: Any, y: Any, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if two objects are equal."""
    if type(x) is not type(y):
        return False

    breakpoint()

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
        try:
            x_sorted = sort_iterable(x)
            y_sorted = sort_iterable(y)
        except SortIterableError as error:
            raise _IsEqualUnsortableSetError(iterable=error.iterable) from None
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


@dataclass(kw_only=True, slots=True)
class IsEqualError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _IsEqualUnsortableSetError(IsEqualError, Generic[_T]):
    iterable: Iterable[_T]

    @override
    def __str__(self) -> str:
        return f"Iterable {reprlib.repr(self.iterable)} must be sortable"


__all__ = ["IsEqualError", "is_equal"]
