from __future__ import annotations

from asyncio import iscoroutinefunction
from collections.abc import Callable
from functools import partial, wraps
from typing import Any, TypeVar, cast, overload

_F = TypeVar("_F", bound=Callable[..., Any])


@overload
def beartype(func: _F, /, *, enable: Callable[[], bool] | None = ...) -> _F: ...
@overload
def beartype(
    func: None = None, /, *, enable: Callable[[], bool] | None = ...
) -> Callable[[_F], _F]: ...
def beartype(
    func: _F | None = None, /, *, enable: Callable[[], bool] | None = None
) -> _F | Callable[[_F], _F]:
    """Apply `beartype` in the development environment."""
    if func is None:
        result = partial(beartype)
        return cast(Callable[[_F], _F], result)

    decorated = beartype(func)
    if enable is None:
        return decorated

    if not iscoroutinefunction(func):

        @wraps(func)
        def beartype_sync(*args: Any, **kwargs: Any) -> Any:
            if enable():
                return decorated(*args, **kwargs)
            return func(*args, **kwargs)

        return cast(_F, beartype_sync)

    @wraps(func)
    async def beartype_async(*args: Any, **kwargs: Any) -> Any:
        if enable():
            return decorated(*args, **kwargs)
        return func(*args, **kwargs)

    return cast(_F, beartype_async)
