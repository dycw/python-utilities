from pathlib import Path
from re import search
from subprocess import PIPE  # noqa: S404
from subprocess import CalledProcessError  # noqa: S404
from subprocess import check_output  # noqa: S404

from beartype import beartype

from utilities.pathlib import PathLike


@beartype
def get_branch_name(*, cwd: PathLike = Path.cwd()) -> str:
    """Get the current branch name."""

    root = get_repo_root(cwd=cwd)
    output = check_output(  # noqa: S603, S607
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
    output = check_output(  # noqa: S603, S607
        ["git", "remote", "get-url", "origin"], stderr=PIPE, cwd=root, text=True
    )
    return Path(output.strip("\n")).stem


@beartype
def get_repo_root(*, cwd: PathLike = Path.cwd()) -> Path:
    """Get the repo root."""

    try:
        output = check_output(  # noqa: S603, S607
            ["git", "rev-parse", "--show-toplevel"],
            stderr=PIPE,
            cwd=cwd,
            text=True,
        )
    except CalledProcessError as error:
        if search("fatal: not a git repository", error.stderr):
            raise InvalidRepo(cwd) from None
        else:  # pragma: no cover
            raise
    else:
        return Path(output.strip("\n"))


class InvalidRepo(TypeError):
    ...
