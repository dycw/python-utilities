from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, MutableSet
from math import inf
from typing import TYPE_CHECKING, Any, TypeVar, cast

from cachetools import TTLCache
from cachetools.func import ttl_cache
from typing_extensions import override

from utilities.datetime import datetime_duration_to_float
from utilities.functions import identity
from utilities.functools import lru_cache

if TYPE_CHECKING:
    from utilities.types import Duration

_F = TypeVar("_F", bound=Callable[..., Any])
_T = TypeVar("_T")


class TTLSet(MutableSet[_T]):
    """A set."""

    _cache: TTLCache[_T, None]

    @override
    def __init__(
        self,
        iterable: Iterable[_T] | None = None,
        /,
        *,
        max_size: int | None = None,
        max_duration: Duration | None = None,
    ) -> None:
        super().__init__()
        self._cache = TTLCache(
            maxsize=inf if max_size is None else max_size,
            ttl=inf
            if max_duration is None
            else datetime_duration_to_float(max_duration),
        )
        if iterable is not None:
            self._cache.update((i, None) for i in iterable)

    @override
    def __contains__(self, x: object) -> bool:
        return self._cache.__contains__(x)

    @override
    def __iter__(self) -> Iterator[_T]:
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
    def add(self, value: _T) -> None:
        self._cache[value] = None

    @override
    def discard(self, value: _T) -> None:
        del self._cache[value]


def cache(
    *, max_size: int | None = None, max_duration: Duration | None = None
) -> Callable[[_F], _F]:
    """Decorate a function with `max_size` and/or `ttl` settings."""
    if max_duration is not None:
        return ttl_cache(maxsize=max_size, ttl=datetime_duration_to_float(max_duration))
    if max_size is not None:
        return cast(Any, lru_cache(max_size=max_size))
    return identity


__all__ = ["TTLSet", "cache"]
