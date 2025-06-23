from __future__ import annotations

from collections.abc import Callable, Hashable
from dataclasses import dataclass
from itertools import islice
from textwrap import indent
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    TypeGuard,
    assert_never,
    cast,
    overload,
    override,
)

from more_itertools import bucket, partition, split_into
from more_itertools import peekable as _peekable

from utilities.functions import get_class_name
from utilities.iterables import OneNonUniqueError, one
from utilities.reprlib import get_repr
from utilities.sentinel import Sentinel, sentinel

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping, Sequence


@overload
def bucket_mapping[T, UH: Hashable](
    iterable: Iterable[T],
    func: Callable[[T], UH],
    /,
    *,
    pre: None = None,
    post: None = None,
) -> Mapping[UH, Iterator[T]]: ...
@overload
def bucket_mapping[T, UH: Hashable](
    iterable: Iterable[T],
    func: Callable[[T], UH],
    /,
    *,
    pre: None = None,
    post: Literal["list"],
) -> Mapping[UH, list[T]]: ...
@overload
def bucket_mapping[T, UH: Hashable](
    iterable: Iterable[T],
    func: Callable[[T], UH],
    /,
    *,
    pre: None = None,
    post: Literal["tuple"],
) -> Mapping[UH, tuple[T, ...]]: ...
@overload
def bucket_mapping[T, UH: Hashable](
    iterable: Iterable[T],
    func: Callable[[T], UH],
    /,
    *,
    pre: None = None,
    post: Literal["set"],
) -> Mapping[UH, set[T]]: ...
@overload
def bucket_mapping[T, UH: Hashable](
    iterable: Iterable[T],
    func: Callable[[T], UH],
    /,
    *,
    pre: None = None,
    post: Literal["frozenset"],
) -> Mapping[UH, frozenset[T]]: ...
@overload
def bucket_mapping[T, UH: Hashable](
    iterable: Iterable[T],
    func: Callable[[T], UH],
    /,
    *,
    pre: None = None,
    post: Literal["unique"],
) -> Mapping[UH, T]: ...
@overload
def bucket_mapping[T, U, UH: Hashable](
    iterable: Iterable[T],
    func: Callable[[T], UH],
    /,
    *,
    pre: Callable[[T], U] | None = None,
    post: None = None,
) -> Mapping[UH, Iterator[U]]: ...
@overload
def bucket_mapping[T, U, UH: Hashable](
    iterable: Iterable[T],
    func: Callable[[T], UH],
    /,
    *,
    pre: Callable[[T], U],
    post: Literal["list"],
) -> Mapping[UH, list[U]]: ...
@overload
def bucket_mapping[T, U, UH: Hashable](
    iterable: Iterable[T],
    func: Callable[[T], UH],
    /,
    *,
    pre: Callable[[T], U],
    post: Literal["tuple"],
) -> Mapping[UH, tuple[U, ...]]: ...
@overload
def bucket_mapping[T, U, UH: Hashable](
    iterable: Iterable[T],
    func: Callable[[T], UH],
    /,
    *,
    pre: Callable[[T], U],
    post: Literal["set"],
) -> Mapping[UH, set[U]]: ...
@overload
def bucket_mapping[T, U, UH: Hashable](
    iterable: Iterable[T],
    func: Callable[[T], UH],
    /,
    *,
    pre: Callable[[T], U],
    post: Literal["frozenset"],
) -> Mapping[UH, frozenset[U]]: ...
@overload
def bucket_mapping[T, U, UH: Hashable](
    iterable: Iterable[T],
    func: Callable[[T], UH],
    /,
    *,
    pre: Callable[[T], U] | None = None,
    post: Literal["unique"],
) -> Mapping[UH, U]: ...
@overload
def bucket_mapping[T, U, UH: Hashable](
    iterable: Iterable[T],
    func: Callable[[T], UH],
    /,
    *,
    pre: Callable[[T], U] | None = None,
    post: Literal["list", "tuple", "set", "frozenset", "unique"] | None = None,
) -> (
    Mapping[UH, Iterator[T]]
    | Mapping[UH, list[T]]
    | Mapping[UH, tuple[T, ...]]
    | Mapping[UH, set[T]]
    | Mapping[UH, frozenset[T]]
    | Mapping[UH, T]
    | Mapping[UH, Iterator[U]]
    | Mapping[UH, list[U]]
    | Mapping[UH, tuple[U, ...]]
    | Mapping[UH, set[U]]
    | Mapping[UH, frozenset[U]]
    | Mapping[UH, U]
): ...
def bucket_mapping[T, U, UH: Hashable](
    iterable: Iterable[T],
    func: Callable[[T], UH],
    /,
    *,
    pre: Callable[[T], U] | None = None,
    post: Literal["list", "tuple", "set", "frozenset", "unique"] | None = None,
) -> (
    Mapping[UH, Iterator[T]]
    | Mapping[UH, list[T]]
    | Mapping[UH, tuple[T, ...]]
    | Mapping[UH, set[T]]
    | Mapping[UH, frozenset[T]]
    | Mapping[UH, T]
    | Mapping[UH, Iterator[U]]
    | Mapping[UH, list[U]]
    | Mapping[UH, tuple[U, ...]]
    | Mapping[UH, set[U]]
    | Mapping[UH, frozenset[U]]
    | Mapping[UH, U]
):
    """Bucket the values of iterable into a mapping."""
    bckt = bucket(iterable, func)
    mapping = {key: bckt[key] for key in bckt}
    match pre, post:
        case None, None:
            return mapping
        case None, "list":
            return {k: list(v) for k, v in mapping.items()}
        case None, "tuple":
            return {k: tuple(v) for k, v in mapping.items()}
        case None, "set":
            return {k: set(v) for k, v in mapping.items()}
        case None, "frozenset":
            return {k: frozenset(v) for k, v in mapping.items()}
        case None, "unique":
            return _bucket_mapping_unique(mapping)
        case Callable(), None:
            return {k: map(pre, v) for k, v in mapping.items()}
        case Callable(), "list":
            return {k: list(map(pre, v)) for k, v in mapping.items()}
        case Callable(), "tuple":
            return {k: tuple(map(pre, v)) for k, v in mapping.items()}
        case Callable(), "set":
            return {k: set(map(pre, v)) for k, v in mapping.items()}
        case Callable(), "frozenset":
            return {k: frozenset(map(pre, v)) for k, v in mapping.items()}
        case Callable(), "unique":
            return _bucket_mapping_unique({k: map(pre, v) for k, v in mapping.items()})
        case _ as never:
            assert_never(never)


def _bucket_mapping_unique[K: Hashable, V](
    mapping: Mapping[K, Iterable[V]], /
) -> Mapping[K, V]:
    results: dict[K, V] = {}
    errors: dict[K, tuple[V, V]] = {}
    for key, value in mapping.items():
        try:
            results[key] = one(value)
        except OneNonUniqueError as error:
            errors[key] = (error.first, error.second)
    if len(errors) >= 1:
        raise BucketMappingError(errors=errors)
    return results


@dataclass(kw_only=True, slots=True)
class BucketMappingError[K: Hashable, V](Exception):
    errors: Mapping[K, tuple[V, V]]

    @override
    def __str__(self) -> str:
        parts = [
            f"{get_repr(key)} (#1: {get_repr(first)}, #2: {get_repr(second)})"
            for key, (first, second) in self.errors.items()
        ]
        desc = ", ".join(parts)
        return f"Buckets must contain exactly one item each; got {desc}"


##


def partition_list[T](
    pred: Callable[[T], bool], iterable: Iterable[T], /
) -> tuple[list[T], list[T]]:
    """Partition with lists."""
    false, true = partition(pred, iterable)
    return list(false), list(true)


##


def partition_typeguard[T, U](
    pred: Callable[[T], TypeGuard[U]], iterable: Iterable[T], /
) -> tuple[Iterator[T], Iterator[U]]:
    """Partition with a typeguarded function."""
    false, true = partition(pred, iterable)
    true = cast("Iterator[U]", true)
    return false, true


##


class peekable[T](_peekable):  # noqa: N801
    """Peekable which supports dropwhile/takewhile methods."""

    def __init__(self, iterable: Iterable[T], /) -> None:
        super().__init__(iterable)

    @override
    def __iter__(self) -> Iterator[T]:  # pyright: ignore[reportIncompatibleMethodOverride]
        while bool(self):
            yield next(self)

    @override
    def __next__(self) -> T:
        return super().__next__()

    def dropwhile(self, predicate: Callable[[T], bool], /) -> None:
        while bool(self) and predicate(self.peek()):
            _ = next(self)

    @overload
    def peek(self, *, default: Sentinel = sentinel) -> T: ...
    @overload
    def peek[U](self, *, default: U) -> T | U: ...
    @override
    def peek(self, *, default: Any = sentinel) -> Any:  # pyright: ignore[reportIncompatibleMethodOverride]
        if isinstance(default, Sentinel):
            return super().peek()
        return super().peek(default=default)

    def takewhile(self, predicate: Callable[[T], bool], /) -> Iterator[T]:
        while bool(self) and predicate(self.peek()):
            yield next(self)


##


@dataclass(kw_only=True, slots=True)
class Split[T]:
    """An iterable split into head/tail."""

    head: T
    tail: T

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


def yield_splits[T](
    iterable: Iterable[T],
    head: int,
    tail: int,
    /,
    *,
    min_frac: float | None = None,
    freq: int | None = None,
) -> Iterator[Split[Sequence[T]]]:
    """Yield the splits of an iterable."""
    it1 = _yield_splits1(iterable, head + tail)
    it2 = _yield_splits2(it1, head, tail, min_frac=min_frac)
    it3 = _yield_splits3(it2)
    freq_use = tail if freq is None else freq
    return islice(it3, 0, None, freq_use)


def _yield_splits1[T](
    iterable: Iterable[T], total: int, /
) -> Iterator[tuple[Literal["head", "body"], Sequence[T]]]:
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


def _yield_splits2[T](
    iterable: Iterable[tuple[Literal["head", "body"], Sequence[T]],],
    head: int,
    tail: int,
    /,
    *,
    min_frac: float | None = None,
) -> Iterator[tuple[Iterable[T], int, int]]:
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


def _yield_splits3[T](
    iterable: Iterable[tuple[Iterable[T], int, int]], /
) -> Iterator[Split[Sequence[T]]]:
    for window, len_head, len_tail in iterable:
        head_win, tail_win = split_into(window, [len_head, len_tail])
        yield cast(
            "Split[Sequence[T]]", Split(head=list(head_win), tail=list(tail_win))
        )


__all__ = [
    "BucketMappingError",
    "Split",
    "bucket_mapping",
    "partition_list",
    "partition_typeguard",
    "peekable",
    "yield_splits",
]
