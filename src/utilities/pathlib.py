from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from os import chdir
from os import walk as _walk
from pathlib import Path

from utilities.re import extract_group

PathLike = Path | str


def ensure_suffix(path: PathLike, suffix: str, /) -> Path:
    """Ensure a path has the required suffix."""
    as_path = Path(path)
    parts = as_path.name.split(".")
    clean_suffix = extract_group(r"^\.(\w+)$", suffix)
    if parts[-1] != clean_suffix:
        parts.append(clean_suffix)
    return as_path.with_name(".".join(parts))


@contextmanager
def temp_cwd(path: PathLike, /) -> Iterator[None]:
    """Context manager with temporary current working directory set."""
    prev = Path.cwd()
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
        yield Path(dirpath), list(map(Path, dirnames)), list(map(Path, filenames))


__all__ = ["PathLike", "ensure_suffix", "temp_cwd", "walk"]
