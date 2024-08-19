from __future__ import annotations

from contextlib import contextmanager
from os import chdir
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    from utilities.types import PathLike

PWD = Path.cwd()


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


__all__ = ["list_dir", "temp_cwd"]
