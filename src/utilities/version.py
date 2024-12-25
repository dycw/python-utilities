from __future__ import annotations

import re
from dataclasses import dataclass, replace
from subprocess import check_output
from typing import TYPE_CHECKING, Self

from typing_extensions import override

from utilities.git import get_ref_tags
from utilities.iterables import one
from utilities.pathlib import PWD

if TYPE_CHECKING:
    from utilities.types import PathLike


_PATTERN = re.compile(r"^(\d+)\.(\d+)\.(\d+)(?:-(\w+))?")


@dataclass(repr=False, order=True, frozen=True, kw_only=True, slots=True)
class Version:
    """A version identifier."""

    major: int = 0
    minor: int = 1
    patch: int = 0
    suffix: str | None = None

    def __post_init__(self) -> None:
        if self.major < 0:
            raise _VersionNegativeMajorVersionError(major=self.major)
        if self.minor < 0:
            raise _VersionNegativeMinorVersionError(minor=self.minor)
        if self.patch < 0:
            raise _VersionNegativePatchVersionError(patch=self.patch)
        if (self.suffix is not None) and (len(self.suffix) == 0):
            raise _VersionEmptySuffixError(suffix=self.suffix)

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

    def with_suffix(self, *, suffix: str | None = None) -> Self:
        return replace(self, suffix=suffix)


@dataclass(kw_only=True, slots=True)
class VersionError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _VersionNegativeMajorVersionError(VersionError):
    major: int

    @override
    def __str__(self) -> str:
        return f"Major version must be non-negative; got {self.major}"


@dataclass(kw_only=True, slots=True)
class _VersionNegativeMinorVersionError(VersionError):
    minor: int

    @override
    def __str__(self) -> str:
        return f"Minor version must be non-negative; got {self.minor}"


@dataclass(kw_only=True, slots=True)
class _VersionNegativePatchVersionError(VersionError):
    patch: int

    @override
    def __str__(self) -> str:
        return f"Patch version must be non-negative; got {self.patch}"


@dataclass(kw_only=True, slots=True)
class _VersionEmptySuffixError(VersionError):
    suffix: str

    @override
    def __str__(self) -> str:
        return f"Suffix must be non-empty; got {self.suffix!r}"


def get_git_version(*, cwd: PathLike = PWD) -> Version:
    """Get the version according to the `git`."""
    tags = get_ref_tags("origin/master", cwd=cwd)
    tag = one(tags)
    return parse_version(tag)


def get_hatch_version(*, cwd: PathLike = PWD) -> Version:
    """Get the version according to `hatch`."""
    output = check_output(["hatch", "version"], cwd=cwd, text=True)
    return parse_version(output.strip("\n"))


def get_version(*, cwd: PathLike = PWD) -> Version:
    """Get the version."""
    git = get_git_version(cwd=cwd)
    hatch = get_hatch_version(cwd=cwd)
    if hatch == git:  # pragma: no cover
        return hatch
    if hatch in {  # pragma: no cover
        git.bump_major(),
        git.bump_minor(),
        git.bump_patch(),
    }:
        return hatch.with_suffix(suffix="dirty")
    raise GetVersionError(git=git, hatch=hatch)  # pragma: no cover


@dataclass(kw_only=True, slots=True)
class GetVersionError(Exception):
    git: Version
    hatch: Version

    @override
    def __str__(self) -> str:
        return f"`git` and `hatch` versions are incompatible; got {self.git} and {self.hatch}"  # pragma: no cover


def parse_version(version: str, /) -> Version:
    """Parse a string into a version object."""
    result = _PATTERN.search(version)
    if not result:
        raise ParseVersionError(version=version)
    major_str, minor_str, patch_str, suffix = result.groups()
    return Version(
        major=int(major_str), minor=int(minor_str), patch=int(patch_str), suffix=suffix
    )


@dataclass(kw_only=True, slots=True)
class ParseVersionError(Exception):
    version: str

    @override
    def __str__(self) -> str:
        return f"Invalid version string: {self.version!r}"


__all__ = [
    "GetVersionError",
    "ParseVersionError",
    "Version",
    "VersionError",
    "get_git_version",
    "get_hatch_version",
    "get_version",
    "parse_version",
]
