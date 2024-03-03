from __future__ import annotations

import datetime as dt
from functools import partial, wraps
from inspect import signature
from operator import itemgetter
from pathlib import Path
from typing import TYPE_CHECKING, ParamSpec, TypeVar

from utilities.cachetools import cache
from utilities.datetime import duration_to_timedelta, get_now
from utilities.git import get_repo_root_or_cwd_sub_path
from utilities.hashlib import md5_hash
from utilities.pathlib import ensure_path, get_modified_time
from utilities.pickle import read_pickle, write_pickle
from utilities.types import Duration, PathLike, ensure_class

if TYPE_CHECKING:
    from collections.abc import Callable

_P = ParamSpec("_P")
_R = TypeVar("_R")


def _caches(path: Path, /) -> Path:
    return Path(path, ".caches")


_ROOT = get_repo_root_or_cwd_sub_path(_caches, if_missing=_caches)
_MD5_HASH_MAX_SIZE = 128
_MD5_HASH_MAX_DURATION = dt.timedelta(minutes=10)
_GET_MODIFIED_MAX_SIZE = 128
_GET_MODIFIED_MAX_DURATION = dt.timedelta(minutes=10)


def cache_to_disk(
    *,
    root: PathLike = _ROOT,
    md5_hash_max_size: int | None = _MD5_HASH_MAX_SIZE,
    md5_hash_max_duration: Duration | None = _MD5_HASH_MAX_DURATION,
    get_modified_time_max_size: int | None = _GET_MODIFIED_MAX_SIZE,
    get_modified_time_max_duration: Duration | None = _GET_MODIFIED_MAX_DURATION,
    skip: bool = False,
    validate_path: bool = False,
    max_size: int | None = None,
    max_duration: Duration | None = None,
) -> Callable[[Callable[_P, _R]], Callable[_P, _R]]:
    """Factory for decorators which caches locally using pickles."""

    return partial(
        _cache_to_disk,
        root=root,
        md5_hash_max_size=md5_hash_max_size,
        md5_hash_max_duration=md5_hash_max_duration,
        get_modified_time_max_size=get_modified_time_max_size,
        get_modified_time_max_duration=get_modified_time_max_duration,
        skip=skip,
        validate_path=validate_path,
        max_size=max_size,
        max_duration=max_duration,
    )


def _cache_to_disk(
    func: Callable[_P, _R],
    /,
    *,
    root: PathLike = _ROOT,
    md5_hash_max_size: int | None = _MD5_HASH_MAX_SIZE,
    md5_hash_max_duration: Duration | None = _MD5_HASH_MAX_DURATION,
    get_modified_time_max_size: int | None = _GET_MODIFIED_MAX_SIZE,
    get_modified_time_max_duration: Duration | None = _GET_MODIFIED_MAX_DURATION,
    skip: bool = False,
    validate_path: bool = False,
    max_size: int | None = None,
    max_duration: Duration | None = None,
) -> Callable[_P, _R]:
    """Decorator which caches locally using pickles."""

    root_use = Path(root, func.__module__, func.__name__)
    sig = signature(func)
    md5_hash_use = cache(
        max_size=md5_hash_max_size, max_duration=md5_hash_max_duration
    )(md5_hash)
    get_modified_time_use = cache(
        max_size=get_modified_time_max_size, max_duration=get_modified_time_max_duration
    )(get_modified_time)
    max_duration_use = (
        None if max_duration is None else duration_to_timedelta(max_duration)
    )

    @wraps(func)
    def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        """The decorated function."""
        if skip:
            return func(*args, **kwargs)
        rerun = ensure_class(kwargs.pop("rerun", False), bool)
        ba = sig.bind(*args, **kwargs)
        stem = md5_hash_use((ba.args, tuple(ba.kwargs.items())))
        path = ensure_path(root_use, stem, validate=validate_path)
        if _needs_run(
            path, get_modified_time_use, rerun=rerun, max_duration=max_duration_use
        ):
            value = func(*args, **kwargs)
            write_pickle(value, path, overwrite=True)
            _maybe_clean(root_use, max_size=max_size)
            return value
        return read_pickle(path)

    return wrapped


def _needs_run(
    path: Path,
    get_modified_time: Callable[[PathLike], dt.datetime],
    /,
    *,
    rerun: bool = False,
    max_duration: dt.timedelta | None = None,
) -> bool:
    if (not path.exists()) or rerun:
        return True
    if max_duration is None:
        return False
    modified = get_modified_time(path)
    return get_now() - modified >= max_duration


def _maybe_clean(root: Path, /, *, max_size: int | None = None) -> None:
    if max_size is None:
        return
    paths = list(root.iterdir())
    if len(paths) <= max_size:
        return
    mapping = {p: p.stat().st_mtime for p in paths}
    as_list = sorted(mapping.items(), key=itemgetter(1), reverse=True)
    for path, _ in as_list[max_size:]:
        path.unlink(missing_ok=True)


__all__ = ["cache_to_disk"]
