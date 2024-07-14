from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar, cast, overload

from atools import memoize as _memoize
from atools._memoize_decorator import Keygen, Pickler, _AsyncMemoize
from typing_extensions import override

from utilities.types import ensure_class

if TYPE_CHECKING:
    import datetime as dt
    from pathlib import Path

_P = ParamSpec("_P")
_R = TypeVar("_R")
_AsyncFunc = Callable[_P, Awaitable[_R]]


@overload
def memoize(
    func: _AsyncFunc[_P, _R],
    /,
    *,
    db_path: Path | None = ...,
    duration: float | dt.timedelta | None = ...,
    keygen: Keygen | None = ...,
    pickler: Pickler | None = ...,
    size: int | None = ...,
) -> _AsyncFunc[_P, _R]: ...
@overload
def memoize(
    func: None = ...,
    /,
    *,
    db_path: Path | None = ...,
    duration: float | dt.timedelta | None = ...,
    keygen: Keygen | None = ...,
    pickler: Pickler | None = ...,
    size: int | None = ...,
) -> Callable[[_AsyncFunc[_P, _R]], _AsyncFunc[_P, _R]]: ...
def memoize(
    func: _AsyncFunc[_P, _R] | None = None,
    /,
    *,
    db_path: Path | None = None,
    duration: float | dt.timedelta | None = None,
    keygen: Keygen | None = None,
    pickler: Pickler | None = None,
    size: int | None = None,
) -> _AsyncFunc[_P, _R] | Callable[[_AsyncFunc[_P, _R]], _AsyncFunc[_P, _R]]:
    """Memoize an asynchronous function."""
    return cast(
        Any,
        _memoize(
            func,
            db_path=db_path,
            duration=duration,
            keygen=keygen,
            pickler=pickler,
            size=size,
        ),
    )


async def refresh_memoized(
    func: _AsyncFunc[_P, _R], /, *args: _P.args, **kwargs: _P.kwargs
) -> _R:
    """Refresh a memoized, asynchronous function."""
    func_any = cast(Any, func)
    try:
        memoize = func_any.memoize
    except AttributeError:
        raise RefreshMemoizedError(func=func) from None
    memoize = ensure_class(memoize, _AsyncMemoize)
    await memoize.remove(*args, **kwargs)
    return await func(*args, **kwargs)


@dataclass(kw_only=True)
class RefreshMemoizedError(Exception):
    func: _AsyncFunc[..., Any]

    @override
    def __str__(self) -> str:
        return f"Asynchronous function {self.func} must be memoized"


__all__ = ["RefreshMemoizedError", "memoize", "refresh_memoized"]
