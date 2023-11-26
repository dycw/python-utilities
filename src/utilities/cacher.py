from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from functools import partial, wraps
from inspect import signature
from pathlib import Path
from typing import ParamSpec, TypeVar

from utilities.datetime import UTC, duration_to_timedelta, get_now
from utilities.git import get_repo_root_or_cwd_sub_path
from utilities.hashlib import md5_hash
from utilities.iterables import ensure_hashables
from utilities.pathlib import PathLike
from utilities.pickle import read_pickle, write_pickle
from utilities.types import Duration

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
        hash_args, hash_kwargs = ensure_hashables(*ba.args, **ba.kwargs)
        hash_pair = tuple(hash_args), tuple(hash_kwargs.items())
        path = root.joinpath(md5_hash(hash_pair))
        if _needs_run(path, ttl=ttl_use):
            value = func(*args, **kwargs)
            write_pickle(value, path, overwrite=True)
            return value
        return read_pickle(path)

    return wrapped


def _needs_run(path: Path, /, *, ttl: dt.timedelta | None = None) -> bool:
    if not path.exists():
        return True
    if ttl is None:
        return False
    now = get_now()
    modified = dt.datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
    age = now - modified
    return age >= ttl


__all__ = ["cache_to_disk"]
