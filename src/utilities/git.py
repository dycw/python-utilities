from pathlib import Path
from re import search
from subprocess import PIPE, CalledProcessError, check_output

from beartype import beartype

from utilities.pathlib import PathLike

_CWD = Path.cwd()


@beartype
def get_branch_name(*, cwd: PathLike = _CWD) -> str:
    """Get the current branch name."""
    root = get_repo_root(cwd=cwd)
    output = check_output(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        stderr=PIPE,
        cwd=root,
        text=True,
    )
    return output.strip("\n")


@beartype
def get_repo_name(*, cwd: PathLike = _CWD) -> str:
    """Get the repo name."""
    root = get_repo_root(cwd=cwd)
    output = check_output(
        ["git", "remote", "get-url", "origin"],
        stderr=PIPE,
        cwd=root,
        text=True,
    )
    return Path(output.strip("\n")).stem


@beartype
def get_repo_root(*, cwd: PathLike = _CWD) -> Path:
    """Get the repo root."""
    try:
        output = check_output(
            ["git", "rev-parse", "--show-toplevel"],
            stderr=PIPE,
            cwd=cwd,
            text=True,
        )
    except CalledProcessError as error:
        if search("fatal: not a git repository", error.stderr):
            raise InvalidRepoError(cwd) from None
        raise  # pragma: no cover
    else:
        return Path(output.strip("\n"))


class InvalidRepoError(TypeError):
    """Raised when an invalid repo is encountered."""
