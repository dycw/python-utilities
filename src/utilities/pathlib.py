from __future__ import annotations

from contextlib import contextmanager
from os import chdir
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    from utilities.types import PathLike

PWD = Path.cwd()


def ensure_path(
    *parts: PathLike, validate: bool = False, sanitize: bool = False
) -> Path:
    """Ensure a path-like object is a path."""
    if validate or sanitize:
        from utilities.pathvalidate import valid_path

        return valid_path(*parts, sanitize=sanitize)
    return Path(*parts)


def list_dir(path: PathLike, /) -> Sequence[Path]:
    """List the contents of a directory."""
    return sorted(Path(path).iterdir())


@contextmanager
def temp_cwd(path: PathLike, /) -> Iterator[None]:
    """Context manager with temporary current working directory set."""
    prev = Path.cwd()
    chdir(path)
    try:
        yield
    finally:
        chdir(prev)


__all__ = ["ensure_path", "list_dir", "temp_cwd"]
