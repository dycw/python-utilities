from __future__ import annotations

from asyncio import iscoroutinefunction
from collections.abc import Callable
from functools import partial, wraps
from typing import Any, TypeVar, cast, overload

from beartype import beartype

from utilities.typing import contains_self, get_type_hints

_F = TypeVar("_F", bound=Callable[..., Any])


@overload
def beartype_cond(
    func: _F,
    /,
    *,
    setup: Callable[[], bool] | None = ...,
    runtime: Callable[[], bool] | None = ...,
) -> _F: ...
@overload
def beartype_cond(
    func: None = None,
    /,
    *,
    setup: Callable[[], bool] | None = ...,
    runtime: Callable[[], bool] | None = ...,
) -> Callable[[_F], _F]: ...
def beartype_cond(
    func: _F | None = None,
    /,
    *,
    setup: Callable[[], bool] | None = None,
    runtime: Callable[[], bool] | None = None,
) -> _F | Callable[[_F], _F]:
    """Apply `beartype` conditionally."""
    if func is None:
        result = partial(beartype_cond, setup=setup, runtime=runtime)
        return cast(Callable[[_F], _F], result)

    if (setup is not None) and not setup():
        return func

    if any(map(contains_self, get_type_hints(func).values())):
        decorated = func
    else:
        decorated = beartype(func)

    if runtime is None:
        return decorated

    if not iscoroutinefunction(func):

        @wraps(func)
        def beartype_sync(*args: Any, **kwargs: Any) -> Any:
            if runtime():
                return decorated(*args, **kwargs)
            return func(*args, **kwargs)

        return cast(_F, beartype_sync)

    @wraps(func)
    async def beartype_async(*args: Any, **kwargs: Any) -> Any:
        if runtime():
            return await decorated(*args, **kwargs)
        return await func(*args, **kwargs)

    return cast(_F, beartype_async)


__all__ = ["beartype_cond"]
