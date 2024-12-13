from __future__ import annotations

from subprocess import CalledProcessError, check_output


def get_version() -> str | None:
    """Get the version."""
    try:
        result = check_output(["hatch", "version"], text=True)
    except CalledProcessError:  # pragma: no cover
        return None
    return result.strip("\n")


__all__ = ["get_version"]
