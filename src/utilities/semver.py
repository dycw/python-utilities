from __future__ import annotations

from semver import Version


def ensure_version(version: Version | str, /) -> Version:
    """Ensure the object is a `Version`."""
    return version if isinstance(version, Version) else Version.parse(version)


__all__ = ["ensure_version"]
