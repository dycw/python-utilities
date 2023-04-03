from typing import Union

from beartype import beartype
from luigi import Parameter
from semver import Version

from utilities.semver import ensure_version


class VersionParameter(Parameter):
    """Parameter taking the value of a `Version`."""

    @beartype
    def normalize(self, version: Union[Version, str], /) -> Version:
        """Normalize a `Version` argument."""
        return ensure_version(version)

    @beartype
    def parse(self, version: str, /) -> Version:
        """Parse a `Version` argument."""
        return Version.parse(version)

    @beartype
    def serialize(self, version: Version, /) -> str:
        """Serialize a `Version` argument."""
        return str(version)
