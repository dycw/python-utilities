from __future__ import annotations

from luigi import Parameter
from semver import Version

from utilities.semver import ensure_version


class VersionParameter(Parameter):
    """Parameter taking the value of a `Version`."""

    def normalize(self, version: Version | str, /) -> Version:
        """Normalize a `Version` argument."""
        return ensure_version(version)

    def parse(self, version: str, /) -> Version:
        """Parse a `Version` argument."""
        return Version.parse(version)

    def serialize(self, version: Version, /) -> str:
        """Serialize a `Version` argument."""
        return str(version)
