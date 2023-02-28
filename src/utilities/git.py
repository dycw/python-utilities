from pathlib import Path
from re import IGNORECASE
from re import search
from subprocess import PIPE
from subprocess import CalledProcessError
from subprocess import check_output

from beartype import beartype

from utilities.pathlib import PathLike


@beartype
def get_branch_name(*, cwd: PathLike = Path.cwd()) -> str:
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
def get_repo_name(*, cwd: PathLike = Path.cwd()) -> str:
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
def get_repo_root(*, cwd: PathLike = Path.cwd()) -> Path:
    """Get the repo root."""
    try:
        output = check_output(
            ["git", "rev-parse", "--show-toplevel"],
            stderr=PIPE,
            cwd=cwd,
            text=True,
        )
    except CalledProcessError as error:
        # newer versions of git report "Not a git repository", whilst older
        # versions report "not a git repository"
        if search(
            "fatal: not a git repository",
            error.stderr,
            flags=IGNORECASE,
        ):
            raise InvalidRepoError(cwd) from None
        raise  # pragma: no cover
    else:
        return Path(output.strip("\n"))


class InvalidRepoError(TypeError):
    """Raised when an invalid repo is encountered."""
