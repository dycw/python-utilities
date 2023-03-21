from typing import Union

from beartype import beartype
from semver import VersionInfo


@beartype
def ensure_version_info(version: Union[VersionInfo, str], /) -> VersionInfo:
    """Ensure the object is a `VersionInfo`."""
    if isinstance(version, VersionInfo):
        return version
    return VersionInfo.parse(version)
