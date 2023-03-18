from typing import Union

from beartype import beartype
from luigi import Parameter
from semver import VersionInfo

from utilities.semver import ensure_version_info


class VersionParameter(Parameter):
    """Parameter taking the value of a `VersionInfo`."""

    @beartype
    def normalize(self, version: Union[VersionInfo, str], /) -> VersionInfo:
        """Normalize a `VersionInfo` argument."""
        return ensure_version_info(version)

    @beartype
    def parse(self, version: str, /) -> VersionInfo:
        """Parse a `VersionInfo` argument."""
        return VersionInfo.parse(version)

    @beartype
    def serialize(self, version: VersionInfo, /) -> str:
        """Serialize a `VersionInfo` argument."""
        return str(version)
