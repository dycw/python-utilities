from __future__ import annotations

from subprocess import CalledProcessError, check_output


def get_version() -> str | None:
    """Get the version."""
    try:
        output = check_output(["hatch", "version"], text=True)
    except CalledProcessError:  # pragma: no cover
        return None
    return output.strip("\n")


__all__ = ["get_version"]
