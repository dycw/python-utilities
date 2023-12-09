from __future__ import annotations

from utilities.pathvalidate import valid_path_cwd
from utilities.re import extract_groups
from utilities.subprocess import get_shell_output
from utilities.types import PathLike


def get_hatch_version(
    *, cwd: PathLike = valid_path_cwd(), activate: PathLike | None = None
) -> tuple[int, int, int]:
    """Get the `hatch` version."""
    version = get_shell_output("hatch version", cwd=cwd, activate=activate).strip("\n")
    major, minor, patch = extract_groups(r"^(\d+)\.(\d+)\.(\d+)$", version)
    return int(major), int(minor), int(patch)


__all__ = ["get_hatch_version"]
