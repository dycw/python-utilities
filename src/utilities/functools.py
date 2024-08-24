from __future__ import annotations

from collections.abc import Callable
from functools import cache as _cache
from functools import lru_cache as _lru_cache
from functools import partial as _partial
from typing import Any, ParamSpec, TypeVar, cast, overload

from typing_extensions import override

_T = TypeVar("_T")
_P = ParamSpec("_P")
_R = TypeVar("_R")


def cache(func: Callable[_P, _R], /) -> Callable[_P, _R]:
    """Typed version of `cache`."""
    typed_cache = cast(Callable[[Callable[_P, _R]], Callable[_P, _R]], _cache)
    return typed_cache(func)


@overload
def lru_cache(
    func: Callable[_P, _R], /, *, max_size: int = ..., typed: bool = ...
) -> Callable[_P, _R]: ...
@overload
def lru_cache(
    func: None = None, /, *, max_size: int = ..., typed: bool = ...
) -> Callable[[Callable[_P, _R]], Callable[_P, _R]]: ...
def lru_cache(
    func: Callable[_P, _R] | None = None, /, *, max_size: int = 128, typed: bool = False
) -> Callable[_P, _R] | Callable[[Callable[_P, _R]], Callable[_P, _R]]:
    """Typed version of `lru_cache`."""
    if func is None:
        result = partial(lru_cache, max_size=max_size, typed=typed)
        return cast(Any, result)
    wrapped = _lru_cache(maxsize=max_size, typed=typed)(func)
    return cast(Any, wrapped)


class partial(_partial[_T]):  # noqa: N801
    """Partial which accepts Ellipsis for positional arguments."""

    @override
    def __call__(self, *args: Any, **kwargs: Any) -> _T:
        iter_args = iter(args)
        head = (next(iter_args) if arg is ... else arg for arg in self.args)
        return self.func(*head, *iter_args, **{**self.keywords, **kwargs})


__all__ = ["cache", "lru_cache", "partial"]
