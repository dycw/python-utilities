from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from re import IGNORECASE, search
from subprocess import PIPE, CalledProcessError, check_output
from typing import TYPE_CHECKING, TypeVar, overload

from typing_extensions import override

from utilities.pathlib import PWD

if TYPE_CHECKING:
    from collections.abc import Callable

    from utilities.types import PathLike

_GET_BRANCH_NAME = ["git", "rev-parse", "--abbrev-ref", "HEAD"]
_T = TypeVar("_T")
_U = TypeVar("_U")


def get_branch_name(*, cwd: PathLike = PWD) -> str:
    """Get the current branch name."""
    root = get_repo_root(cwd=cwd)
    output = check_output(_GET_BRANCH_NAME, stderr=PIPE, cwd=root, text=True)
    return output.strip("\n")


def get_repo_name(*, cwd: PathLike = PWD) -> str:
    """Get the repo name."""
    root = get_repo_root(cwd=cwd)
    output = check_output(
        ["git", "remote", "get-url", "origin"], stderr=PIPE, cwd=root, text=True
    )
    return Path(output.strip("\n")).stem  # not valid_path


def get_repo_root(*, cwd: PathLike = PWD) -> Path:
    """Get the repo root."""
    try:
        output = check_output(
            ["git", "rev-parse", "--show-toplevel"], stderr=PIPE, cwd=cwd, text=True
        )
    except CalledProcessError as error:
        # newer versions of git report "Not a git repository", whilst older
        # versions report "not a git repository"
        if search("fatal: not a git repository", error.stderr, flags=IGNORECASE):
            raise GetRepoRootError(cwd=cwd) from error
        raise  # pragma: no cover
    else:
        return Path(output.strip("\n"))


@dataclass(kw_only=True)
class GetRepoRootError(Exception):
    cwd: PathLike

    @override
    def __str__(self) -> str:
        return f"Path is not part of a `git` repository: {self.cwd}"


@overload
def get_repo_root_or_cwd_sub_path(
    if_exists: Callable[[Path], _T], /, *, cwd: PathLike = ..., if_missing: None = ...
) -> _T | None: ...
@overload
def get_repo_root_or_cwd_sub_path(
    if_exists: Callable[[Path], _T],
    /,
    *,
    cwd: PathLike = ...,
    if_missing: Callable[[Path], _U] = ...,
) -> _T | _U: ...
def get_repo_root_or_cwd_sub_path(
    if_exists: Callable[[Path], _T],
    /,
    *,
    cwd: PathLike = PWD,
    if_missing: Callable[[Path], _U] | None = None,
) -> _T | _U | None:
    """Get a path under the repo root, if it exists, else under the CWD."""
    try:
        root = get_repo_root(cwd=cwd)
    except (FileNotFoundError, GetRepoRootError):
        if if_missing is None:
            return None
        return if_missing(Path(cwd))
    return if_exists(root)


__all__ = [
    "GetRepoRootError",
    "get_branch_name",
    "get_repo_name",
    "get_repo_root",
    "get_repo_root_or_cwd_sub_path",
]
