from __future__ import annotations

import reprlib
from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass
from itertools import islice
from textwrap import indent
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    TypeGuard,
    TypeVar,
    assert_never,
    cast,
    overload,
)

from more_itertools import partition, split_into
from more_itertools import peekable as _peekable
from typing_extensions import override

from utilities.functions import get_class_name
from utilities.iterables import always_iterable
from utilities.sentinel import Sentinel, sentinel

if TYPE_CHECKING:
    from utilities.iterables import MaybeIterable

_T = TypeVar("_T")
_U = TypeVar("_U")


@overload
def filter_include_and_exclude(
    iterable: Iterable[_T],
    /,
    *,
    include: MaybeIterable[_U] | None = None,
    exclude: MaybeIterable[_U] | None = None,
    key: Callable[[_T], _U],
) -> Iterable[_T]: ...
@overload
def filter_include_and_exclude(
    iterable: Iterable[_T],
    /,
    *,
    include: MaybeIterable[_T] | None = None,
    exclude: MaybeIterable[_T] | None = None,
    key: Callable[[_T], Any] | None = None,
) -> Iterable[_T]: ...
def filter_include_and_exclude(
    iterable: Iterable[_T],
    /,
    *,
    include: MaybeIterable[_U] | None = None,
    exclude: MaybeIterable[_U] | None = None,
    key: Callable[[_T], _U] | None = None,
) -> Iterable[_T]:
    """Filter an iterable based on an inclusion/exclusion pair."""
    include, exclude = resolve_include_and_exclude(include=include, exclude=exclude)
    if include is not None:
        if key is None:
            iterable = (x for x in iterable if x in include)
        else:
            iterable = (x for x in iterable if key(x) in include)
    if exclude is not None:
        if key is None:
            iterable = (x for x in iterable if x not in exclude)
        else:
            iterable = (x for x in iterable if key(x) not in exclude)
    return iterable


def partition_typeguard(
    pred: Callable[[_T], TypeGuard[_U]], iterable: Iterable[_T], /
) -> tuple[Iterator[_T], Iterator[_U]]:
    """Partition with a typeguarded function."""
    false, true = partition(pred, iterable)
    true = cast(Iterator[_U], true)
    return false, true


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
    def peek(self, *, default: Any = sentinel) -> Any:  # pyright: ignore[reportIncompatibleMethodOverride]
        if isinstance(default, Sentinel):
            return super().peek()
        return super().peek(default=default)

    def takewhile(self, predicate: Callable[[_T], bool], /) -> Iterator[_T]:
        while bool(self) and predicate(self.peek()):
            yield next(self)


def resolve_include_and_exclude(
    *,
    include: _T | Iterable[_T] | None = None,
    exclude: _T | Iterable[_T] | None = None,
) -> tuple[set[_T] | None, set[_T] | None]:
    """Resolve an inclusion/exclusion pair."""
    include_use = include if include is None else set(always_iterable(include))
    exclude_use = exclude if exclude is None else set(always_iterable(exclude))
    if (
        (include_use is not None)
        and (exclude_use is not None)
        and (len(include_use & exclude_use) >= 1)
    ):
        raise ResolveIncludeAndExcludeError(include=include_use, exclude=exclude_use)
    return include_use, exclude_use


@dataclass(kw_only=True)
class ResolveIncludeAndExcludeError(Exception, Generic[_T]):
    include: Iterable[_T]
    exclude: Iterable[_T]

    @override
    def __str__(self) -> str:
        include = list(self.include)
        exclude = list(self.exclude)
        overlap = set(include) & set(exclude)
        return f"Iterables {reprlib.repr(include)} and {reprlib.repr(exclude)} must not overlap; got {reprlib.repr(overlap)}"


@dataclass(frozen=True)
class Split(Generic[_T]):
    """An iterable split into head/tail."""

    head: _T
    tail: _T

    @override
    def __repr__(self) -> str:
        cls = get_class_name(self)
        spaces = 4 * " "
        head_first = indent("head=", spaces)
        head_rest = indent(repr(self.head), 2 * spaces)
        tail_first = indent("tail=", spaces)
        tail_rest = indent(repr(self.tail), 2 * spaces)
        joined = f"{head_first}\n{head_rest}\n{tail_first}\n{tail_rest}"
        return f"{cls}(\n{joined}\n)"


def yield_splits(
    iterable: Iterable[_T],
    head: int,
    tail: int,
    /,
    *,
    min_frac: float | None = None,
    freq: int | None = None,
) -> Iterator[Split[Sequence[_T]]]:
    """Yield the splits of an iterable."""
    it1 = _yield_splits1(iterable, head + tail)
    it2 = _yield_splits2(it1, head, tail, min_frac=min_frac)
    it3 = _yield_splits3(it2)
    freq_use = tail if freq is None else freq
    return islice(it3, 0, None, freq_use)


def _yield_splits1(
    iterable: Iterable[_T], total: int, /
) -> Iterator[tuple[Literal["head", "body"], Sequence[_T]]]:
    peek = peekable(iterable)
    for i in range(1, total + 1):
        if len(result := peek[:i]) < i:
            return
        yield "head", result
    while True:
        _ = next(peek)
        if len(result := peek[:total]) >= 1:
            yield "body", result
        else:
            break


def _yield_splits2(
    iterable: Iterable[tuple[Literal["head", "body"], Sequence[_T]],],
    head: int,
    tail: int,
    /,
    *,
    min_frac: float | None = None,
) -> Iterator[tuple[Iterable[_T], int, int]]:
    min_length = head if min_frac is None else min_frac * head
    for kind, window in iterable:
        len_win = len(window)
        match kind:
            case "head":
                len_head = max(len_win - tail, 0)
                if len_head >= min_length:
                    yield window, len_head, tail
            case "body":
                len_tail = max(len_win - head, 0)
                if len_tail >= 1:
                    yield window, head, len_tail
            case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
                assert_never(never)


def _yield_splits3(
    iterable: Iterable[tuple[Iterable[_T], int, int]], /
) -> Iterator[Split[Sequence[_T]]]:
    for window, len_head, len_tail in iterable:
        head_win, tail_win = split_into(window, [len_head, len_tail])
        yield cast(Split[Sequence[_T]], Split(head=list(head_win), tail=list(tail_win)))


__all__ = [
    "ResolveIncludeAndExcludeError",
    "Split",
    "filter_include_and_exclude",
    "partition_typeguard",
    "peekable",
    "resolve_include_and_exclude",
    "yield_splits",
]
