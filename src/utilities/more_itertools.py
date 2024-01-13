from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any, TypeVar

from more_itertools import always_iterable as _always_iterable
from typing_extensions import override

_T = TypeVar("_T")


def always_iterable(
    obj: _T | Iterable[_T],
    /,
    *,
    base_type: type[Any] | tuple[type[Any], ...] | None = (str, bytes),
) -> Iterator[_T]:
    """Typed version of `always_iterable`."""
    return _always_iterable(obj, base_type=base_type)


def one(iterable: Iterable[_T], /) -> _T:
    """Custom version of `one` with separate exceptions."""
    it = iter(iterable)
    try:
        first = next(it)
    except StopIteration:
        raise OneEmptyError(iterable=iterable) from None
    try:
        second = next(it)
    except StopIteration:
        return first
    raise OneNonUniqueError(iterable=iterable, first=first, second=second)


@dataclass(frozen=True, kw_only=True, slots=True)
class OneError(Exception):
    iterable: Iterable[Any]


@dataclass(frozen=True, kw_only=True, slots=True)
class OneEmptyError(OneError):
    @override
    def __str__(self) -> str:
        return "Iterable {} must contain exactly one item; it was empty".format(
            self.iterable
        )


@dataclass(frozen=True, kw_only=True, slots=True)
class OneNonUniqueError(OneError):
    first: Any
    second: Any

    @override
    def __str__(self) -> str:
        return "Iterable {} must contain exactly one item; got {}, {} and perhaps more".format(
            self.iterable, self.first, self.second
        )


__all__ = ["OneEmptyError", "OneError", "OneNonUniqueError", "always_iterable", "one"]
