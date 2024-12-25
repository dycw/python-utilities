from __future__ import annotations

from dataclasses import dataclass
from subprocess import CalledProcessError, check_output
from typing import TYPE_CHECKING, Self

from typing_extensions import override

from utilities.git import get_ref_tags
from utilities.pathlib import PWD

if TYPE_CHECKING:
    from utilities.types import PathLike


@dataclass(repr=False, kw_only=True, slots=True)
class Version:
    """A version identifier."""

    major: int = 0
    minor: int = 1
    patch: int = 0
    suffix: str | None = None

    def __post_init__(self) -> None:
        if self.major < 0:
            raise _VersionNegativeMajorVersionError(major=self.major)

    @override
    def __repr__(self) -> str:
        version = f"{self.major}.{self.minor}.{self.patch}"
        if self.suffix is not None:
            version = f"{version}-{self.suffix}"
        return version

    def bump_major(self) -> Self:
        return type(self)(major=self.major + 1, minor=0, patch=0)

    def bump_minor(self) -> Self:
        return type(self)(major=self.major, minor=self.minor + 1, patch=0)

    def bump_patch(self) -> Self:
        return type(self)(major=self.major, minor=self.minor, patch=self.patch + 1)
@dataclass(kw_only=True, slots=True)

def get_git_origin_master_version() -> Version:
    """Get the version according to the `git` `origin/master` tag."""


def get_hatch_version() -> Version:
    """Get the version."""
    output = check_output(["hatch", "version"], text=True)
    return parse_version(output.strip("\n"))


def get_version(*, cwd: PathLike = PWD) -> str:
    """Get the version."""
    hatch_version = get_hatch_version()
    try:
        output = check_output(["hatch", "version"], text=True)
    except CalledProcessError:  # pragma: no cover
        return None
    return output.strip("\n")


def parse_version(version: str, /) -> Version:
    """Parse a string into a version object."""


__all__ = ["get_git_origin_master_version", "get_hatch_version", "get_version"]
