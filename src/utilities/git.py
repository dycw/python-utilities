from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from re import IGNORECASE, search
from subprocess import PIPE, CalledProcessError, check_output
from typing import TYPE_CHECKING

from typing_extensions import override

from utilities.pathlib import PWD

if TYPE_CHECKING:
    from utilities.types import PathLike

_GET_BRANCH_NAME = ["git", "rev-parse", "--abbrev-ref", "HEAD"]


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


@dataclass(kw_only=True, slots=True)
class GetRepoRootError(Exception):
    cwd: PathLike

    @override
    def __str__(self) -> str:
        return f"Path is not part of a `git` repository: {self.cwd}"


__all__ = ["GetRepoRootError", "get_branch_name", "get_repo_name", "get_repo_root"]
