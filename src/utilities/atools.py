from __future__ import annotations

from collections.abc import Callable, Hashable, Iterator
from dataclasses import dataclass
from inspect import signature
from re import search
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    ParamSpec,
    TypeVar,
    cast,
    get_origin,
    overload,
)

from atools import memoize as _memoize
from atools._memoize_decorator import Pickler, _AsyncMemoize
from typing_extensions import override

from utilities.asyncio import Coroutine1
from utilities.types import ensure_class
from utilities.typing import get_args

if TYPE_CHECKING:
    import datetime as dt
    from pathlib import Path

_P = ParamSpec("_P")
_R = TypeVar("_R")
_AsyncFunc = Callable[_P, Coroutine1[_R]]


class _NoMemoize:
    """Base class for the no-memoize sentinel object."""


no_memoize = _NoMemoize()


@overload
def memoize(
    func: _AsyncFunc[_P, _R],
    /,
    *,
    db_path: Path | None = ...,
    duration: float | dt.timedelta | None = ...,
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
    pickler: Pickler | None = ...,
    size: int | None = ...,
) -> Callable[[_AsyncFunc[_P, _R]], _AsyncFunc[_P, _R]]: ...
def memoize(
    func: _AsyncFunc[_P, _R] | None = None,
    /,
    *,
    db_path: Path | None = None,
    duration: float | dt.timedelta | None = None,
    pickler: Pickler | None = None,
    size: int | None = None,
) -> _AsyncFunc[_P, _R] | Callable[[_AsyncFunc[_P, _R]], _AsyncFunc[_P, _R]]:
    """Memoize an asynchronous function."""
    if func is None:
        return _memoize(
            func, db_path=db_path, duration=duration, pickler=pickler, size=size
        )
    return _memoize(
        func,
        db_path=db_path,
        duration=duration,
        keygen=_memoize_auto_keygen(func),
        pickler=pickler,
        size=size,
    )


def _memoize_auto_keygen(
    func: _AsyncFunc[_P, _R], /
) -> Callable[..., tuple[Hashable, ...]]:
    """Automatic `keygen` for `memoize`."""
    params = list(_memoize_auto_keygen_yield_params(func))

    def keygen(**kwargs: Any) -> tuple[Hashable, ...]:
        return tuple(v for k, v in kwargs.items() if k in params)

    return keygen


def _memoize_auto_keygen_yield_params(func: _AsyncFunc[_P, _R], /) -> Iterator[str]:
    """Yield the parameters to be respected."""
    sig = signature(func)
    for k, v in sig.parameters.items():
        if _memoize_auto_keygen_is_param(v.annotation):
            yield k


def _memoize_auto_keygen_is_param(ann: Any, /) -> bool:
    """Check if a parameter is to be memoized."""
    if isinstance(ann, str):
        return not search("no_memoize", ann)
    if get_origin(ann) is Annotated:  # pragma: no cover
        args = get_args(ann)
        return all(arg is not no_memoize for arg in args)
    return True


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


@dataclass(kw_only=True, slots=True)
class RefreshMemoizedError(Exception):
    func: _AsyncFunc[..., Any]

    @override
    def __str__(self) -> str:
        return f"Asynchronous function {self.func} must be memoized"


__all__ = ["RefreshMemoizedError", "memoize", "no_memoize", "refresh_memoized"]
