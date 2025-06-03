from __future__ import annotations

from collections.abc import Callable
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from itertools import chain
from os import chdir
from pathlib import Path
from re import IGNORECASE, search
from subprocess import PIPE, CalledProcessError, check_output
from typing import TYPE_CHECKING, assert_never, overload, override

from utilities.sentinel import Sentinel, sentinel

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    from utilities.types import MaybeCallablePathLike, PathLike

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
def get_path(*, path: MaybeCallablePathLike | None) -> Path: ...
@overload
def get_path(*, path: Sentinel) -> Sentinel: ...
def get_path(
    *, path: MaybeCallablePathLike | None | Sentinel = sentinel
) -> Path | None | Sentinel:
    """Get the path."""
    match path:
        case Path() | Sentinel():
            return path
        case str():
            return Path(path)
        case None:
            return Path.cwd()
        case Callable() as func:
            return get_path(path=func())
        case _ as never:
            assert_never(never)


##


def get_root(*, path: MaybeCallablePathLike | None = None) -> Path:
    """Get the root of a path."""
    path = get_path(path=path)
    try:
        output = check_output(
            ["git", "rev-parse", "--show-toplevel"], stderr=PIPE, cwd=path, text=True
        )
    except CalledProcessError as error:
        # newer versions of git report "Not a git repository", whilst older
        # versions report "not a git repository"
        if not search("fatal: not a git repository", error.stderr, flags=IGNORECASE):
            raise  # pragma: no cover
    else:
        return Path(output.strip("\n"))
    all_paths = list(chain([path], path.parents))
    with suppress(StopIteration):
        return next(
            p for p in all_paths if any(p_i.name == ".envrc" for p_i in p.iterdir())
        )
    raise GetRootError(path=path)


@dataclass(kw_only=True, slots=True)
class GetRootError(Exception):
    path: PathLike

    @override
    def __str__(self) -> str:
        return f"Unable to determine root from {str(self.path)!r}"


##


def list_dir(path: PathLike, /) -> Sequence[Path]:
    """List the contents of a directory."""
    return sorted(Path(path).iterdir())


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


__all__ = ["PWD", "ensure_suffix", "get_path", "list_dir", "temp_cwd"]
