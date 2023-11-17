from __future__ import annotations

from luigi import Parameter
from semver import Version
from typing_extensions import override

from utilities.semver import ensure_version


class VersionParameter(Parameter):
    """Parameter taking the value of a `Version`."""

    @override
    def normalize(self, x: Version | str) -> Version:
        """Normalize a `Version` argument."""
        return ensure_version(x)

    @override
    def parse(self, x: str) -> Version:
        """Parse a `Version` argument."""
        return Version.parse(x)

    @override
    def serialize(self, x: Version) -> str:
        """Serialize a `Version` argument."""
        return str(x)


__all__ = ["VersionParameter"]
