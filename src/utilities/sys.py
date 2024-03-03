from __future__ import annotations

from sys import modules, version_info

VERSION_MAJOR_MINOR = (version_info.major, version_info.minor)


def is_pytest() -> bool:
    """Check if `pytest` is running."""

    return "pytest" in modules


__all__ = ["VERSION_MAJOR_MINOR", "is_pytest"]
