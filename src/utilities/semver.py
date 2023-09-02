from __future__ import annotations

from typing import Union

from semver import Version


def ensure_version(version: Version | str, /) -> Version:
    """Ensure the object is a `Version`."""
    return version if isinstance(version, Version) else Version.parse(version)
