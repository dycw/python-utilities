from __future__ import annotations

import builtins
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
    override,
)

from more_itertools import bucket, partition, split_into
from more_itertools import peekable as _peekable

from utilities.functions import get_class_name
from utilities.sentinel import Sentinel, sentinel

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence

    from utilities.types import THashable

_T = TypeVar("_T")
_U = TypeVar("_U")


@overload
def bucket_mapping(
    iterable: Iterable[_T], func: Callable[[_T], THashable], /, *, list: Literal[True]
) -> Mapping[THashable, Sequence[_T]]: ...
@overload
def bucket_mapping(
    iterable: Iterable[_T], func: Callable[[_T], THashable], /, *, list: bool = False
) -> Mapping[THashable, Iterator[_T]]: ...
def bucket_mapping(
    iterable: Iterable[_T],
    func: Callable[[_T], THashable],
    /,
    *,
    list: bool = False,  # noqa: A002
) -> Mapping[THashable, Iterator[_T]] | Mapping[THashable, Sequence[_T]]:
    """Bucket the values of iterable into a mapping."""
    b = bucket(iterable, func)
    mapping = {key: b[key] for key in b}
    if list:
        return {key: builtins.list(value) for key, value in mapping.items()}
    return mapping


def partition_typeguard(
    pred: Callable[[_T], TypeGuard[_U]], iterable: Iterable[_T], /
) -> tuple[Iterator[_T], Iterator[_U]]:
    """Partition with a typeguarded function."""
    false, true = partition(pred, iterable)
    true = cast("Iterator[_U]", true)
    return false, true


class peekable(_peekable, Generic[_T]):  # noqa: N801
    """Peekable which supports dropwhile/takewhile methods."""

    def __init__(self, iterable: Iterable[_T], /) -> None:
        super().__init__(iterable)

    @override
    def __iter__(self) -> Iterator[_T]:  # pyright: ignore[reportIncompatibleMethodOverride]
        while bool(self):
            yield next(self)

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


@dataclass(kw_only=True, slots=True)
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
            case _ as never:
                assert_never(never)


def _yield_splits3(
    iterable: Iterable[tuple[Iterable[_T], int, int]], /
) -> Iterator[Split[Sequence[_T]]]:
    for window, len_head, len_tail in iterable:
        head_win, tail_win = split_into(window, [len_head, len_tail])
        yield cast(
            "Split[Sequence[_T]]", Split(head=list(head_win), tail=list(tail_win))
        )


__all__ = ["Split", "bucket_mapping", "partition_typeguard", "peekable", "yield_splits"]
