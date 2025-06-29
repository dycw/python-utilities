from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from collections.abc import Set as AbstractSet
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any, cast, override

import utilities.math
from utilities.functions import is_dataclass_instance
from utilities.iterables import SortIterableError, sort_iterable
from utilities.reprlib import get_repr

if TYPE_CHECKING:
    from utilities.types import Dataclass, Number


def is_equal[T](
    x: Any,
    y: Any,
    /,
    *,
    rel_tol: float | None = None,
    abs_tol: float | None = None,
    extra: Mapping[type[T], Callable[[T, T], bool]] | None = None,
) -> bool:
    """Check if two objects are equal."""
    if type(x) is type(y):
        # extra
        if extra is not None:
            try:
                cmp = next(cmp for cls, cmp in extra.items() if isinstance(x, cls))
            except StopIteration:
                pass
            else:
                x = cast("T", x)
                y = cast("T", y)
                return cmp(x, y)

        # singletons
        if isinstance(x, int | float):
            y = cast("Number", y)
            return utilities.math.is_equal(x, y, rel_tol=rel_tol, abs_tol=abs_tol)
        if isinstance(x, str):  # else Sequence
            y = cast("str", y)
            return x == y
        if is_dataclass_instance(x):
            y = cast("Dataclass", y)
            x_values = asdict(x)
            y_values = asdict(y)
            return is_equal(x_values, y_values, rel_tol=rel_tol, abs_tol=abs_tol)
        if isinstance(x, Exception):
            return is_equal(x.args, y.args)

        # collections
        if isinstance(x, AbstractSet):
            y = cast("AbstractSet[Any]", y)
            try:
                x_sorted = sort_iterable(x)
                y_sorted = sort_iterable(y)
            except SortIterableError:
                return _is_in(x, y) and _is_in(y, x)
            return is_equal(x_sorted, y_sorted, rel_tol=rel_tol, abs_tol=abs_tol)
        if isinstance(x, Mapping):
            y = cast("Mapping[Any, Any]", y)
            x_keys = set(x)
            y_keys = set(y)
            if not is_equal(x_keys, y_keys, rel_tol=rel_tol, abs_tol=abs_tol):
                return False
            x_values = [x[i] for i in x]
            y_values = [y[i] for i in x]
            return is_equal(x_values, y_values, rel_tol=rel_tol, abs_tol=abs_tol)
        if isinstance(x, Sequence):
            y = cast("Sequence[Any]", y)
            if len(x) != len(y):
                return False
            return all(
                is_equal(x_i, y_i, rel_tol=rel_tol, abs_tol=abs_tol)
                for x_i, y_i in zip(x, y, strict=True)
            )

    if isinstance(x, int | float) and isinstance(y, int | float):
        return utilities.math.is_equal(x, y, rel_tol=rel_tol, abs_tol=abs_tol)

    return (type(x) is type(y)) and (x == y)


def _is_in[T](
    x: AbstractSet[Any],
    y: AbstractSet[Any],
    /,
    *,
    rel_tol: float | None = None,
    abs_tol: float | None = None,
    extra: Mapping[type[T], Callable[[T, T], bool]] | None = None,
) -> bool:
    return all(
        any(
            is_equal(x_i, y_i, rel_tol=rel_tol, abs_tol=abs_tol, extra=extra)
            for y_i in y
        )
        for x_i in x
    )


@dataclass(kw_only=True, slots=True)
class IsEqualError(Exception):
    x: Any
    y: Any

    @override
    def __str__(self) -> str:
        return f"Unable to sort {get_repr(self.x)} and {get_repr(self.y)}"  # pragma: no cover


__all__ = ["IsEqualError", "is_equal"]
