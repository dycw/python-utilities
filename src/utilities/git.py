from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from re import IGNORECASE, search
from subprocess import PIPE, CalledProcessError, check_output
from typing import TYPE_CHECKING, override

from utilities.pathlib import PWD

if TYPE_CHECKING:
    from utilities.types import PathLike


def get_repo_root(*, path: PathLike = PWD) -> Path:
    """Get the repo root."""
    try:
        output = check_output(
            ["git", "rev-parse", "--show-toplevel"], stderr=PIPE, cwd=path, text=True
        )
    except CalledProcessError as error:
        # newer versions of git report "Not a git repository", whilst older
        # versions report "not a git repository"
        if search("fatal: not a git repository", error.stderr, flags=IGNORECASE):
            raise GetRepoRootError(cwd=path) from error
        raise  # pragma: no cover
    else:
        return Path(output.strip("\n"))


@dataclass(kw_only=True, slots=True)
class GetRepoRootError(Exception):
    cwd: PathLike

    @override
    def __str__(self) -> str:
        return f"Path is not part of a `git` repository: {self.cwd}"


__all__ = ["GetRepoRootError", "get_repo_root"]
