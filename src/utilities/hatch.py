from pathlib import Path

from beartype import beartype

from utilities.pathlib import PathLike
from utilities.re import extract_groups
from utilities.subprocess import get_shell_output


@beartype
def get_hatch_version(
    *, cwd: PathLike = Path.cwd(), activate: PathLike | None = None
) -> tuple[int, int, int]:
    """Get the `hatch` version."""

    version = get_shell_output(
        "hatch version", cwd=cwd, activate=activate
    ).strip("\n")
    major, minor, patch = extract_groups(r"^(\d+)\.(\d+)\.(\d+)$", version)
    return int(major), int(minor), int(patch)
