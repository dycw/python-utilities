from __future__ import annotations

from collections.abc import Callable, Hashable, Iterable, Iterator, MutableSet
from math import inf
from time import monotonic
from typing import TYPE_CHECKING, Any, override

import cachetools
from cachetools.func import ttl_cache

if TYPE_CHECKING:
    from whenever import TimeDelta


class TTLCache[K: Hashable, V](cachetools.TTLCache[K, V]):
    """A TTL-cache."""

    def __init__(
        self,
        *,
        max_size: int | None = None,
        max_duration: TimeDelta | None = None,
        timer: Callable[[], float] = monotonic,
        get_size_of: Callable[[Any], int] | None = None,
    ) -> None:
        super().__init__(
            maxsize=inf if max_size is None else max_size,
            ttl=inf if max_duration is None else max_duration.in_seconds(),
            timer=timer,
            getsizeof=get_size_of,
        )


##


class TTLSet[T: Hashable](MutableSet[T]):
    """A TTL-set."""

    _cache: TTLCache[T, None]

    @override
    def __init__(
        self,
        iterable: Iterable[T] | None = None,
        /,
        *,
        max_size: int | None = None,
        max_duration: TimeDelta | None = None,
        timer: Callable[[], float] = monotonic,
        get_size_of: Callable[[Any], int] | None = None,
    ) -> None:
        super().__init__()
        self._cache = TTLCache(
            max_size=max_size,
            max_duration=max_duration,
            timer=timer,
            get_size_of=get_size_of,
        )
        if iterable is not None:
            self._cache.update((i, None) for i in iterable)

    @override
    def __contains__(self, x: object) -> bool:
        return self._cache.__contains__(x)

    @override
    def __iter__(self) -> Iterator[T]:
        return self._cache.__iter__()

    @override
    def __len__(self) -> int:
        return self._cache.__len__()

    @override
    def __repr__(self) -> str:
        return set(self._cache).__repr__()

    @override
    def __str__(self) -> str:
        return set(self._cache).__str__()

    @override
    def add(self, value: T) -> None:
        self._cache[value] = None

    @override
    def discard(self, value: T) -> None:
        del self._cache[value]


##


def cache[F: Callable](
    *,
    max_size: int | None = None,
    max_duration: TimeDelta | None = None,
    timer: Callable[[], float] = monotonic,
    typed_: bool = False,
) -> Callable[[F], F]:
    """Decorate a function with `max_size` and/or `ttl` settings."""
    return ttl_cache(
        maxsize=inf if max_size is None else max_size,
        ttl=inf if max_duration is None else max_duration.in_seconds(),
        timer=timer,
        typed=typed_,
    )


__all__ = ["TTLCache", "TTLSet", "cache"]
