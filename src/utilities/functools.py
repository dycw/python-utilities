from __future__ import annotations

from functools import cache as _cache
from functools import lru_cache as _lru_cache
from functools import partial as _partial
from typing import TYPE_CHECKING, Any, cast, overload, override

if TYPE_CHECKING:
    from collections.abc import Callable


def cache[F: Callable](func: F, /) -> F:
    """Typed version of `cache`."""
    typed_cache = cast("Callable[[F], F]", _cache)
    return typed_cache(func)


##


_MAX_SIZE = 128


@overload
def lru_cache[F: Callable](
    func: F, /, *, max_size: int = _MAX_SIZE, typed: bool = False
) -> F: ...
@overload
def lru_cache[F: Callable](
    func: None = None, /, *, max_size: int = _MAX_SIZE, typed: bool = False
) -> Callable[[F], F]: ...
def lru_cache[F: Callable](
    func: F | None = None, /, *, max_size: int = _MAX_SIZE, typed: bool = False
) -> F | Callable[[F], F]:
    """Typed version of `lru_cache`."""
    if func is None:
        result = partial(lru_cache, max_size=max_size, typed=typed)
        return cast("Callable[[F], F]", result)
    wrapped = _lru_cache(maxsize=max_size, typed=typed)(func)
    return cast("Any", wrapped)


##


class partial[T](_partial[T]):  # noqa: N801
    """Partial which accepts Ellipsis for positional arguments."""

    @override
    def __call__(self, *args: Any, **kwargs: Any) -> T:
        iter_args = iter(args)
        head = (next(iter_args) if arg is ... else arg for arg in self.args)
        return self.func(*head, *iter_args, **{**self.keywords, **kwargs})


__all__ = ["cache", "lru_cache", "partial"]
