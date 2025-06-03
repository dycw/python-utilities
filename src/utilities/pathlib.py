from __future__ import annotations

from collections.abc import Callable
from contextlib import contextmanager
from itertools import chain
from os import chdir
from pathlib import Path
from typing import TYPE_CHECKING, assert_never, overload

from utilities.sentinel import Sentinel, sentinel

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    from utilities.types import MaybeCallablePath, PathLike, PathLikeOrCallable

PWD = Path.cwd()


def ensure_suffix(path: PathLike, suffix: str, /) -> Path:
    """Ensure a path has a given suffix."""
    path = Path(path)
    parts = path.name.split(".")
    parts = list(chain([parts[0]], (f".{p}" for p in parts[1:])))
    if (len(parts) == 0) or (parts[-1] != suffix):
        parts.append(suffix)
    name = "".join(parts)
    return path.with_name(name)


##


@overload
def get_path(*, path: MaybeCallablePath) -> Path: ...
@overload
def get_path(*, path: None) -> None: ...
@overload
def get_path(*, path: Sentinel) -> Sentinel: ...
@overload
def get_path(*, path: MaybeCallablePath | Sentinel) -> Path | Sentinel: ...
@overload
def get_path(
    *, path: MaybeCallablePath | None | Sentinel = sentinel
) -> Path | None | Sentinel: ...
def get_path(
    *, path: MaybeCallablePath | None | Sentinel = sentinel
) -> Path | None | Sentinel:
    """Get the path."""
    match path:
        case Path() | None | Sentinel():
            return path
        case str():
            return Path(path)
        case Callable() as func:
            return get_path(path=func())
        case _ as never:
            assert_never(never)


##


def get_root(path: PathLike, /) -> Path:
    """Get the root of a path."""


##


def list_dir(path: PathLike, /) -> Sequence[Path]:
    """List the contents of a directory."""
    return sorted(Path(path).iterdir())


##


def resolve_path(*, path: PathLikeOrCallable | None = None) -> Path:
    """Resolve for a path."""
    match path:
        case None:
            return Path.cwd()
        case Path() | str():
            return Path(path)
        case _:
            return Path(path())


##


@contextmanager
def temp_cwd(path: PathLike, /) -> Iterator[None]:
    """Context manager with temporary current working directory set."""
    prev = Path.cwd()
    chdir(path)
    try:
        yield
    finally:
        chdir(prev)


__all__ = ["ensure_suffix", "get_path", "list_dir", "resolve_path", "temp_cwd"]
