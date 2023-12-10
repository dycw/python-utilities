from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from re import IGNORECASE, search
from subprocess import PIPE, CalledProcessError, check_output
from typing import TypeVar, overload

from utilities.pathlib import PathLike
from utilities.pathvalidate import valid_path, valid_path_cwd

_GET_BRANCH_NAME = ["git", "rev-parse", "--abbrev-ref", "HEAD"]
_T = TypeVar("_T")
_U = TypeVar("_U")


def get_branch_name(*, cwd: PathLike = valid_path_cwd()) -> str:
    """Get the current branch name."""
    root = get_repo_root(cwd=cwd)
    output = check_output(
        _GET_BRANCH_NAME,  # noqa: S603
        stderr=PIPE,
        cwd=root,
        text=True,
    )
    return output.strip("\n")


def get_repo_name(*, cwd: PathLike = valid_path_cwd()) -> str:
    """Get the repo name."""
    root = get_repo_root(cwd=cwd)
    output = check_output(
        ["git", "remote", "get-url", "origin"],  # noqa: S603, S607
        stderr=PIPE,
        cwd=root,
        text=True,
    )
    return Path(output.strip("\n")).stem  # not valid_path


def get_repo_root(*, cwd: PathLike = valid_path_cwd()) -> Path:
    """Get the repo root."""
    try:
        output = check_output(
            ["git", "rev-parse", "--show-toplevel"],  # noqa: S603, S607
            stderr=PIPE,
            cwd=cwd,
            text=True,
        )
    except CalledProcessError as error:
        # newer versions of git report "Not a git repository", whilst older
        # versions report "not a git repository"
        if search("fatal: not a git repository", error.stderr, flags=IGNORECASE):
            raise GetRepoRootError(cwd) from error
        raise  # pragma: no cover
    else:
        return valid_path(output.strip("\n"))


class GetRepoRootError(Exception):
    ...


@overload
def get_repo_root_or_cwd_sub_path(
    if_exists: Callable[[Path], _T], /, *, cwd: PathLike = ..., if_missing: None = ...
) -> _T | None:
    ...


@overload
def get_repo_root_or_cwd_sub_path(
    if_exists: Callable[[Path], _T],
    /,
    *,
    cwd: PathLike = ...,
    if_missing: Callable[[Path], _U] = ...,
) -> _T | _U:
    ...


def get_repo_root_or_cwd_sub_path(
    if_exists: Callable[[Path], _T],
    /,
    *,
    cwd: PathLike = valid_path_cwd(),
    if_missing: Callable[[Path], _U] | None = None,
) -> _T | _U | None:
    """Get a path under the repo root, if it exists, else under the CWD."""
    try:
        root = get_repo_root(cwd=cwd)
    except (FileNotFoundError, GetRepoRootError):
        if if_missing is None:
            return None
        return if_missing(valid_path(cwd))
    return if_exists(root)


def valid_path_repo(
    *parts: PathLike, cwd: PathLike = valid_path_cwd(), sanitize: bool = False
) -> Path:
    """Build & validate a path from home."""
    return valid_path(get_repo_root(cwd=cwd), *parts, sanitize=sanitize)


__all__ = [
    "GetRepoRootError",
    "get_branch_name",
    "get_repo_name",
    "get_repo_root",
    "get_repo_root_or_cwd_sub_path",
    "valid_path_repo",
]
