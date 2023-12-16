from __future__ import annotations

import datetime as dt
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from os import chdir
from os import walk as _walk
from pathlib import Path

from utilities.datetime import UTC
from utilities.pathvalidate import valid_path, valid_path_cwd
from utilities.re import extract_group
from utilities.types import PathLike


def ensure_suffix(path: PathLike, suffix: str, /) -> Path:
    """Ensure a path has the required suffix."""
    as_path = valid_path(path)
    parts = as_path.name.split(".")
    clean_suffix = extract_group(r"^\.(\w+)$", suffix)
    if parts[-1] != clean_suffix:
        parts.append(clean_suffix)
    return as_path.with_name(".".join(parts))


def get_modified_time(path: PathLike, /) -> dt.datetime:
    """Get the modified time of a file."""
    return dt.datetime.fromtimestamp(Path(path).stat().st_mtime, tz=UTC)


@contextmanager
def temp_cwd(path: PathLike, /) -> Iterator[None]:
    """Context manager with temporary current working directory set."""
    prev = valid_path_cwd()
    chdir(path)
    try:
        yield
    finally:
        chdir(prev)


def walk(
    top: PathLike,
    /,
    *,
    topdown: bool = True,
    onerror: Callable[[OSError], None] | None = None,
    followlinks: bool = False,
) -> Iterator[tuple[Path, list[Path], list[Path]]]:
    """Iterate through a directory recursively."""
    for dirpath, dirnames, filenames in _walk(
        top, topdown=topdown, onerror=onerror, followlinks=followlinks
    ):
        yield (
            valid_path(dirpath),
            list(map(valid_path, dirnames)),
            list(map(valid_path, filenames)),
        )


__all__ = ["ensure_suffix", "get_modified_time", "temp_cwd", "walk"]
