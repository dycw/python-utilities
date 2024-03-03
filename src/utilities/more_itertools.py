from collections.abc import Iterable, Iterator
from typing import Any, TypeVar, cast

from more_itertools import always_iterable as _always_iterable
from more_itertools import windowed_complete as _windowed_complete

_T = TypeVar("_T")


def always_iterable(
    obj: _T | Iterable[_T],
    /,
    *,
    base_type: type[Any] | tuple[type[Any], ...] | None = (str, bytes),
) -> Iterator[_T]:
    """Typed version of `always_iterable`."""
    return _always_iterable(obj, base_type=base_type)


def windowed_complete(
    iterable: Iterable[_T], n: int, /
) -> Iterator[tuple[tuple[_T, ...], tuple[_T, ...], tuple[_T, ...]]]:
    """Typed version of `windowed_complete`."""
    return cast(
        Iterator[tuple[tuple[_T, ...], tuple[_T, ...], tuple[_T, ...]]],
        _windowed_complete(iterable, n),
    )


__all__ = ["always_iterable", "windowed_complete"]
