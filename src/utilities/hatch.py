from __future__ import annotations

from subprocess import check_output


def get_version() -> str | None:
    """Get the version."""
    result = check_output(["hatch", "version"], text=True)
    return None if result is None else result.strip("\n")


__all__ = ["get_version"]
