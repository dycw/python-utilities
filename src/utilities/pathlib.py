from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from os import chdir
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
