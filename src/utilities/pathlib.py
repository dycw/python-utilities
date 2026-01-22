from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from itertools import chain
from pathlib import Path
from re import IGNORECASE, search
from subprocess import PIPE, CalledProcessError, check_output
from typing import TYPE_CHECKING, Literal, assert_never, overload, override

from utilities.constants import Sentinel

if TYPE_CHECKING:
    from collections.abc import Sequence

    from utilities.types import MaybeCallablePathLike, PathLike


def ensure_suffix(path: PathLike, suffix: str, /) -> Path:
    """Ensure a path has a given suffix."""
    path = Path(path)
    parts = path.name.split(".")
    suffixes = suffix.strip(".").split(".")
    max_len = max(len(parts), len(suffixes))
    try:
        i = next(i for i in range(max_len, 0, -1) if parts[-i:] == suffixes[:i])
    except StopIteration:
        add = suffixes
    else:
        add = suffixes[i:]
    name = ".".join(chain(parts, add))
    return path.with_name(name)


##


def get_repo_root(path: MaybeCallablePathLike = Path.cwd, /) -> Path:
    """Get the repo root."""
    path = to_path(path)
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
        if search("fatal: not a git repository", error.stderr, flags=IGNORECASE):
            raise _GetRepoRootNotARepoError(path=path) from None
        raise  # pragma: no cover
    except FileNotFoundError as error:  # pragma: no cover
        if search("No such file or directory: 'git'", str(error), flags=IGNORECASE):
            raise _GetRepoRootGitNotFoundError from None
        raise
    else:
        return Path(output.strip("\n"))


@dataclass(kw_only=True, slots=True)
class GetRepoRootError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _GetRepoRootGitNotFoundError(GetRepoRootError):
    @override
    def __str__(self) -> str:
        return "'git' not found"  # pragma: no cover


@dataclass(kw_only=True, slots=True)
class _GetRepoRootNotARepoError(GetRepoRootError):
    path: Path

    @override
    def __str__(self) -> str:
        return f"Path is not part of a `git` repository: {self.path}"


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
        case never:
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


def module_path(
    path: PathLike,
    /,
    *,
    root: PathLike | None = None,
    disambiguate: _GetTailDisambiguate = "raise",
) -> str:
    """Return a module path."""
    path = Path(path)
    if root is not None:
        path = get_tail(path, root, disambiguate=disambiguate)
    parts = path.with_suffix("").parts
    return ".".join(parts)


##


def list_dir(path: PathLike, /) -> Sequence[Path]:
    """List the contents of a directory."""
    return sorted(Path(path).iterdir())


##


@overload
def to_path(path: Sentinel, /) -> Sentinel: ...
@overload
def to_path(path: MaybeCallablePathLike | None = Path.cwd, /) -> Path: ...
def to_path(
    path: MaybeCallablePathLike | None | Sentinel = Path.cwd, /
) -> Path | Sentinel:
    """Get the path."""
    match path:
        case Path() | Sentinel():
            return path
        case None:
            return Path.cwd()
        case str():
            return Path(path)
        case Callable() as func:
            return to_path(func())
        case never:
            assert_never(never)


__all__ = [
    "GetRepoRootError",
    "GetTailError",
    "ensure_suffix",
    "get_repo_root",
    "get_tail",
    "list_dir",
    "module_path",
    "to_path",
]
