from __future__ import annotations

from collections.abc import Callable
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast, overload

import atools

from utilities.core import duration_to_seconds
from utilities.types import Coro, Duration, PathLike

if TYPE_CHECKING:
    from atools._memoize_decorator import Keygen, Pickler


type _Key[**P, T] = tuple[Callable[P, Coro[T]], Duration]
_MEMOIZED_FUNCS: dict[_Key, Callable[..., Coro[Any]]] = {}


async def call_memoized[**P, T](
    func: Callable[P, Coro[T]],
    refresh: Duration | None = None,
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> T:
    """Call an asynchronous function, with possible memoization."""
    if refresh is None:
        return await func(*args, **kwargs)
    key: _Key = (func, refresh)
    memoized_func: Callable[P, Coro[T]]
    try:
        memoized_func = _MEMOIZED_FUNCS[key]
    except KeyError:
        memoized_func = _MEMOIZED_FUNCS[(key)] = memoize(
            duration=duration_to_seconds(refresh)
        )(func)
    return await memoized_func(*args, **kwargs)


##


@overload
def memoize[F: Callable[..., Coro[Any]]](
    func: F,
    /,
    *,
    db_path: PathLike | None = None,
    duration: Duration | None = None,
    keygen: Keygen | None = None,
    pickler: Pickler | None = None,
    size: int | None = None,
) -> F: ...
@overload
def memoize[F: Callable[..., Coro[Any]]](
    func: None = None,
    /,
    *,
    db_path: PathLike | None = None,
    duration: Duration | None = None,
    keygen: Keygen | None = None,
    pickler: Pickler | None = None,
    size: int | None = None,
) -> Callable[[F], F]: ...
def memoize[F: Callable[..., Coro[Any]]](
    func: F | None = None,
    /,
    *,
    db_path: PathLike | None = None,
    duration: Duration | None = None,
    keygen: Keygen | None = None,
    pickler: Pickler | None = None,
    size: int | None = None,
) -> F | Callable[[F], F]:
    """Memoize a function."""
    if func is None:
        result = partial(
            memoize,
            db_path=db_path,
            duration=duration,
            keygen=keygen,
            pickler=pickler,
            size=size,
        )
        return cast("Callable[[F], F]", result)
    return atools.memoize(
        db_path=None if db_path is None else Path(db_path),
        duration=None if duration is None else duration_to_seconds(duration),
        keygen=keygen,
        pickler=pickler,
        size=size,
    )(func)


__all__ = ["call_memoized"]
