from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from typing import Any, Generic, TypeVar, cast, overload

from more_itertools import always_iterable as _always_iterable
from more_itertools import peekable as _peekable
from more_itertools import windowed_complete as _windowed_complete
from typing_extensions import override

from utilities.sentinel import Sentinel, sentinel

_T = TypeVar("_T")
_U = TypeVar("_U")


def always_iterable(
    obj: _T | Iterable[_T],
    /,
    *,
    base_type: type[Any] | tuple[type[Any], ...] | None = (str, bytes),
) -> Iterator[_T]:
    """Typed version of `always_iterable`."""
    return _always_iterable(obj, base_type=base_type)


class peekable(_peekable, Generic[_T]):  # noqa: N801
    """Peekable which supports dropwhile/takewhile methods."""

    def __init__(self, iterable: Iterable[_T], /) -> None:
        super().__init__(iterable)

    @override
    def __next__(self) -> _T:
        return super().__next__()

    def dropwhile(self, predicate: Callable[[_T], bool], /) -> None:
        while bool(self) and predicate(self.peek()):
            _ = next(self)

    @overload
    def peek(self, *, default: Sentinel = sentinel) -> _T: ...
    @overload
    def peek(self, *, default: _U) -> _T | _U: ...
    @override
    def peek(self, *, default: Any = sentinel) -> Any:  # type: ignore[]
        if isinstance(default, Sentinel):
            return super().peek()
        return super().peek(default=default)

    def takewhile(self, predicate: Callable[[_T], bool], /) -> Iterator[_T]:
        while bool(self) and predicate(self.peek()):
            yield next(self)


def windowed_complete(
    iterable: Iterable[_T], n: int, /
) -> Iterator[tuple[tuple[_T, ...], tuple[_T, ...], tuple[_T, ...]]]:
    """Typed version of `windowed_complete`."""
    return cast(
        Iterator[tuple[tuple[_T, ...], tuple[_T, ...], tuple[_T, ...]]],
        _windowed_complete(iterable, n),
    )


__all__ = ["always_iterable", "peekable", "windowed_complete"]
