from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from re import IGNORECASE, search
from subprocess import PIPE, CalledProcessError, check_call, check_output
from typing import TYPE_CHECKING, override

from utilities.pathlib import PWD

if TYPE_CHECKING:
    from utilities.types import PathLike


def fetch_all_tags(*, cwd: PathLike = PWD) -> None:
    """Fetch the tags."""
    _ = check_call(["git", "fetch", "--all", "--tags"], cwd=cwd)


##


def get_branch_name(*, cwd: PathLike = PWD) -> str:
    """Get the current branch name."""
    output = check_output(
        _GIT_REV_PARSE_ABBREV_REV_HEAD, stderr=PIPE, cwd=cwd, text=True
    )
    return output.strip("\n")


_GIT_REV_PARSE_ABBREV_REV_HEAD = ["git", "rev-parse", "--abbrev-ref", "HEAD"]


##


def get_ref_tags(ref: str, /, *, cwd: PathLike = PWD) -> list[str]:
    """Get the tags of a reference."""
    output = check_output([*_GIT_TAG_POINTS_AT, ref], stderr=PIPE, cwd=cwd, text=True)
    return output.strip("\n").splitlines()


_GIT_TAG_POINTS_AT = ["git", "tag", "--points-at"]


##


def get_repo_name(*, cwd: PathLike = PWD) -> str:
    """Get the repo name."""
    output = check_output(_GIT_REMOTE_GET_URL_ORIGIN, stderr=PIPE, cwd=cwd, text=True)
    return Path(output.strip("\n")).stem  # not valid_path


_GIT_REMOTE_GET_URL_ORIGIN = ["git", "remote", "get-url", "origin"]


##


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


__all__ = [
    "GetRepoRootError",
    "fetch_all_tags",
    "get_branch_name",
    "get_ref_tags",
    "get_repo_name",
    "get_repo_root",
]
