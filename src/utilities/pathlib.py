from __future__ import annotations

from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass
from itertools import chain
from os import chdir
from os.path import expandvars
from pathlib import Path
from re import IGNORECASE, search
from subprocess import PIPE, CalledProcessError, check_output
from typing import TYPE_CHECKING, Literal, assert_never, overload, override

from utilities.errors import ImpossibleCaseError
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


def expand_path(path: PathLike, /) -> Path:
    """Expand a path."""
    path = str(path)
    path = expandvars(path)
    return Path(path).expanduser()


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
    path_dir = path.parent if path.is_file() else path
    try:
        output = check_output(
            ["git", "rev-parse", "--show-toplevel"],
            stderr=PIPE,
            cwd=path_dir,
            text=True,
        )
    except CalledProcessError as error:
        # newer versions of git report "Not a git repository", whilst older
        # versions report "not a git repository"
        if not search("fatal: not a git repository", error.stderr, flags=IGNORECASE):
            raise  # pragma: no cover
        root_git = None
    else:
        root_git = Path(output.strip("\n")).resolve()
    all_paths = list(chain([path_dir], path_dir.parents))
    try:
        root_envrc = next(
            p.resolve()
            for p in all_paths
            if any(p_i.name == ".envrc" for p_i in p.iterdir())
        )
    except StopIteration:
        root_envrc = None
    match root_git, root_envrc:
        case None, None:
            raise GetRootError(path=path)
        case Path(), None:
            return root_git
        case None, Path():
            return root_envrc
        case Path(), Path():
            if root_git == root_envrc:
                return root_git
            if is_sub_path(root_git, root_envrc, strict=True):
                return root_git
            if is_sub_path(root_envrc, root_git, strict=True):
                return root_envrc
            raise ImpossibleCaseError(  # pragma: no cover
                case=[f"{root_git=}", f"{root_envrc=}"]
            )
        case _ as never:
            assert_never(never)


@dataclass(kw_only=True, slots=True)
class GetRootError(Exception):
    path: PathLike

    @override
    def __str__(self) -> str:
        return f"Unable to determine root from {str(self.path)!r}"


##


type _GetTailDisambiguate = Literal["raise", "earlier", "later"]


def get_tail(
    path: PathLike, root: PathLike, /, *, disambiguate: _GetTailDisambiguate = "raise"
) -> Path:
    """Get the tail of a path following a root match."""
    path_parts, root_parts = [Path(p).parts for p in [path, root]]
    len_path, len_root = map(len, [path_parts, root_parts])
    if len_root > len_path:
        raise _GetTailLengthError(path=path, root=root, len_root=len_root)
    candidates = {
        i + len_root: path_parts[i : i + len_root]
        for i in range(len_path + 1 - len_root)
    }
    matches = {k: v for k, v in candidates.items() if v == root_parts}
    match len(matches), disambiguate:
        case 0, _:
            raise _GetTailEmptyError(path=path, root=root)
        case 1, _:
            return _get_tail_core(path, next(iter(matches)))
        case _, "raise":
            first, second, *_ = matches
            raise _GetTailNonUniqueError(
                path=path,
                root=root,
                first=_get_tail_core(path, first),
                second=_get_tail_core(path, second),
            )
        case _, "earlier":
            return _get_tail_core(path, next(iter(matches)))
        case _, "later":
            return _get_tail_core(path, next(iter(reversed(matches))))
        case _ as never:
            assert_never(never)


def _get_tail_core(path: PathLike, i: int, /) -> Path:
    parts = Path(path).parts
    return Path(*parts[i:])


@dataclass(kw_only=True, slots=True)
class GetTailError(Exception):
    path: PathLike
    root: PathLike


@dataclass(kw_only=True, slots=True)
class _GetTailLengthError(GetTailError):
    len_root: int

    @override
    def __str__(self) -> str:
        return f"Unable to get the tail of {str(self.path)!r} with root of length {self.len_root}"


@dataclass(kw_only=True, slots=True)
class _GetTailEmptyError(GetTailError):
    @override
    def __str__(self) -> str:
        return (
            f"Unable to get the tail of {str(self.path)!r} with root {str(self.root)!r}"
        )


@dataclass(kw_only=True, slots=True)
class _GetTailNonUniqueError(GetTailError):
    first: Path
    second: Path

    @override
    def __str__(self) -> str:
        return f"Path {str(self.path)!r} must contain exactly one tail with root {str(self.root)!r}; got {str(self.first)!r}, {str(self.second)!r} and perhaps more"


##


def is_sub_path(x: PathLike, y: PathLike, /, *, strict: bool = False) -> bool:
    """Check if a path is a sub path of another."""
    x, y = [Path(i).resolve() for i in [x, y]]
    return x.is_relative_to(y) and not (strict and y.is_relative_to(x))


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


__all__ = [
    "PWD",
    "GetTailError",
    "ensure_suffix",
    "expand_path",
    "get_path",
    "get_tail",
    "is_sub_path",
    "list_dir",
    "temp_cwd",
]
