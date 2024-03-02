from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any, Generic, TypeVar, cast, overload

from more_itertools import always_iterable as _always_iterable
from more_itertools import transpose as _transpose
from more_itertools import windowed_complete as _windowed_complete
from typing_extensions import override

_T = TypeVar("_T")
_T1 = TypeVar("_T1")
_T2 = TypeVar("_T2")
_T3 = TypeVar("_T3")
_T4 = TypeVar("_T4")
_T5 = TypeVar("_T5")


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
class OneError(Exception, Generic[_T]):
    iterable: Iterable[_T]


@dataclass(frozen=True, kw_only=True, slots=True)
class OneEmptyError(OneError[_T]):
    @override
    def __str__(self) -> str:
        return f"Iterable {self.iterable} must not be empty."


@dataclass(frozen=True, kw_only=True, slots=True)
class OneNonUniqueError(OneError[_T]):
    first: _T
    second: _T

    @override
    def __str__(self) -> str:
        return "Iterable {} must contain exactly one item; got {}, {} and perhaps more.".format(
            self.iterable, self.first, self.second
        )


@overload
def transpose(iterable: Iterable[tuple[_T1]], /) -> tuple[tuple[_T1, ...]]: ...


@overload
def transpose(
    iterable: Iterable[tuple[_T1, _T2]], /
) -> tuple[tuple[_T1, ...], tuple[_T2, ...]]: ...


@overload
def transpose(
    iterable: Iterable[tuple[_T1, _T2, _T3]], /
) -> tuple[tuple[_T1, ...], tuple[_T2, ...], tuple[_T3, ...]]: ...


@overload
def transpose(
    iterable: Iterable[tuple[_T1, _T2, _T3, _T4]], /
) -> tuple[tuple[_T1, ...], tuple[_T2, ...], tuple[_T3, ...], tuple[_T4, ...]]: ...


@overload
def transpose(
    iterable: Iterable[tuple[_T1, _T2, _T3, _T4, _T5]], /
) -> tuple[
    tuple[_T1, ...], tuple[_T2, ...], tuple[_T3, ...], tuple[_T4, ...], tuple[_T5, ...]
]: ...


def transpose(iterable: Iterable[tuple[Any, ...]]) -> tuple[tuple[Any, ...], ...]:
    """Typed verison of `transpose`."""
    return tuple(_transpose(iterable))


def windowed_complete(
    iterable: Iterable[_T], n: int, /
) -> Iterator[tuple[tuple[_T, ...], tuple[_T, ...], tuple[_T, ...]]]:
    """Typed version of `windowed_complete`."""
    return cast(
        Iterator[tuple[tuple[_T, ...], tuple[_T, ...], tuple[_T, ...]]],
        _windowed_complete(iterable, n),
    )


__all__ = [
    "OneEmptyError",
    "OneError",
    "OneNonUniqueError",
    "always_iterable",
    "one",
    "transpose",
    "windowed_complete",
]
