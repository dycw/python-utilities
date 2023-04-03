from typing import Union

from beartype import beartype
from semver import Version


@beartype
def ensure_version(version: Union[Version, str], /) -> Version:
    """Ensure the object is a `Version`."""
    return version if isinstance(version, Version) else Version.parse(version)
