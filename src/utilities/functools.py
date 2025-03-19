from __future__ import annotations

from collections.abc import Callable, Iterator
from functools import cache as _cache
from functools import cached_property
from functools import lru_cache as _lru_cache
from functools import partial as _partial
from typing import Any, TypeGuard, TypeVar, cast, overload, override

from utilities.functions import yield_object_attributes

_F = TypeVar("_F", bound=Callable[..., Any])
_T = TypeVar("_T")


def cache(func: _F, /) -> _F:
    """Typed version of `cache`."""
    typed_cache = cast("Callable[[_F], _F]", _cache)
    return typed_cache(func)


##


@overload
def lru_cache(func: _F, /, *, max_size: int = ..., typed: bool = ...) -> _F: ...
@overload
def lru_cache(
    func: None = None, /, *, max_size: int = ..., typed: bool = ...
) -> Callable[[_F], _F]: ...
def lru_cache(
    func: _F | None = None, /, *, max_size: int = 128, typed: bool = False
) -> _F | Callable[[_F], _F]:
    """Typed version of `lru_cache`."""
    if func is None:
        result = partial(lru_cache, max_size=max_size, typed=typed)
        return cast("Callable[[_F], _F]", result)
    wrapped = _lru_cache(maxsize=max_size, typed=typed)(func)
    return cast("Any", wrapped)


##


class partial(_partial[_T]):  # noqa: N801
    """Partial which accepts Ellipsis for positional arguments."""

    @override
    def __call__(self, *args: Any, **kwargs: Any) -> _T:
        iter_args = iter(args)
        head = (next(iter_args) if arg is ... else arg for arg in self.args)
        return self.func(*head, *iter_args, **{**self.keywords, **kwargs})


##


def yield_cached_properties(obj: Any) -> Iterator[tuple[str, Any]]:
    """Yield the cached properties of an object."""
    yield from yield_object_attributes(obj, static=_is_cached_property)


def _is_cached_property(obj: Any, /) -> TypeGuard[cached_property]:
    return isinstance(obj, cached_property)


__all__ = ["cache", "lru_cache", "partial", "yield_cached_properties"]
