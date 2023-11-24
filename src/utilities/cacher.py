from __future__ import annotations

import datetime as dt
from collections.abc import Callable, Hashable, Iterator
from functools import partial, wraps
from inspect import signature
from pathlib import Path
from typing import Any, ParamSpec, TypeVar

from utilities.datetime import UTC, duration_to_timedelta, get_now
from utilities.git import get_repo_root_or_cwd_sub_path
from utilities.hashlib import md5_hash
from utilities.pathlib import PathLike
from utilities.pickle import read_pickle, write_pickle
from utilities.types import Duration, is_hashable

_P = ParamSpec("_P")
_R = TypeVar("_R")


def _caches(path: Path, /) -> Path:
    return path.joinpath(".caches")


_ROOT = get_repo_root_or_cwd_sub_path(_caches, if_missing=_caches)


def cache_to_disk(
    *, root: PathLike = _ROOT, ttl: Duration | None = None
) -> Callable[[Callable[_P, _R]], Callable[_P, _R]]:
    """Factory for decorators which caches locally using pickles."""

    return partial(_cache_to_disk, root=root, ttl=ttl)


def _cache_to_disk(
    func: Callable[_P, _R], /, *, root: PathLike = _ROOT, ttl: Duration | None = None
) -> Callable[_P, _R]:
    """Decorator which caches locally using pickles."""

    root = Path(root, func.__name__)
    sig = signature(func)
    ttl_use = None if ttl is None else duration_to_timedelta(ttl)

    @wraps(func)
    def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        """The decorated function."""
        ba = sig.bind(*args, **kwargs)
        hash_args = tuple(_yield_hashable_args(*ba.args))
        hash_kwargs = tuple(_yield_hashable_kwargs(**ba.kwargs))
        path = root.joinpath(md5_hash((hash_args, hash_kwargs)))
        if _needs_run(path, ttl=ttl_use):
            value = func(*args, **kwargs)
            write_pickle(value, path, overwrite=True)
            return value
        return read_pickle(path)

    return wrapped


def _yield_hashable_args(*args: Any) -> Iterator[Hashable]:
    for i, arg in enumerate(args):
        if is_hashable(arg):
            yield arg
        else:
            msg = f"Positional argument {arg} (index {i}) is non-hashable"
            raise NonHashableArgumentError(msg)


def _yield_hashable_kwargs(**kwargs: Any) -> Iterator[tuple[str, Hashable]]:
    for key, arg in kwargs.items():
        if is_hashable(arg):
            yield key, arg
        else:
            msg = f"Keyword argument {key} = {arg} is non-hashable"
            raise NonHashableArgumentError(msg)


class NonHashableArgumentError(Exception):
    """Raised when an argument is non-hashable."""


def _needs_run(path: Path, /, *, ttl: dt.timedelta | None = None) -> bool:
    if not path.exists():
        return True
    if ttl is None:
        return False
    now = get_now()
    modified = dt.datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
    age = now - modified
    return age >= ttl
