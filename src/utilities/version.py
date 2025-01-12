from __future__ import annotations

import re
from dataclasses import dataclass, field, replace
from functools import total_ordering
from subprocess import check_output
from typing import TYPE_CHECKING, Any, Self

from typing_extensions import override

from utilities.git import MASTER, fetch_all_tags, get_ref_tags
from utilities.iterables import OneEmptyError, one
from utilities.os import temp_environ
from utilities.pathlib import PWD

if TYPE_CHECKING:
    from utilities.types import PathLike


_PATTERN = re.compile(r"^(\d+)\.(\d+)\.(\d+)(?:-(\w+))?")


##


@dataclass(repr=False, frozen=True, slots=True)
@total_ordering
class Version:
    """A version identifier."""

    major: int = 0
    minor: int = 0
    patch: int = 1
    suffix: str | None = field(default=None, kw_only=True)

    def __post_init__(self) -> None:
        if (self.major == 0) and (self.minor == 0) and (self.patch == 0):
            raise _VersionZeroError(
                major=self.major, minor=self.minor, patch=self.patch
            )
        if self.major < 0:
            raise _VersionNegativeMajorVersionError(major=self.major)
        if self.minor < 0:
            raise _VersionNegativeMinorVersionError(minor=self.minor)
        if self.patch < 0:
            raise _VersionNegativePatchVersionError(patch=self.patch)
        if (self.suffix is not None) and (len(self.suffix) == 0):
            raise _VersionEmptySuffixError(suffix=self.suffix)

    def __le__(self, other: Any, /) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        self_as_tuple = (
            self.major,
            self.minor,
            self.patch,
            "" if self.suffix is None else self.suffix,
        )
        other_as_tuple = (
            other.major,
            other.minor,
            other.patch,
            "" if other.suffix is None else other.suffix,
        )
        return self_as_tuple <= other_as_tuple

    @override
    def __repr__(self) -> str:
        version = f"{self.major}.{self.minor}.{self.patch}"
        if self.suffix is not None:
            version = f"{version}-{self.suffix}"
        return version

    def bump_major(self) -> Self:
        return type(self)(self.major + 1, 0, 0)

    def bump_minor(self) -> Self:
        return type(self)(self.major, self.minor + 1, 0)

    def bump_patch(self) -> Self:
        return type(self)(self.major, self.minor, self.patch + 1)

    def with_suffix(self, *, suffix: str | None = None) -> Self:
        return replace(self, suffix=suffix)


@dataclass(kw_only=True, slots=True)
class VersionError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _VersionZeroError(VersionError):
    major: int
    minor: int
    patch: int

    @override
    def __str__(self) -> str:
        return f"Version must be greater than zero; got {self.major}.{self.minor}.{self.patch}"


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


##


def get_git_version(*, cwd: PathLike = PWD, ref: str = MASTER) -> Version:
    """Get the version according to the `git`."""
    fetch_all_tags(cwd=cwd)
    tags = get_ref_tags(ref, cwd=cwd)
    try:
        tag = one(tags)
    except OneEmptyError:
        raise GetGitVersionError(cwd=cwd) from None
    return parse_version(tag)


@dataclass(kw_only=True, slots=True)
class GetGitVersionError(Exception):
    cwd: PathLike
    ref: str = MASTER

    @override
    def __str__(self) -> str:
        return f"Reference {self.ref!r} at {str(self.cwd)!r} has no tags"


##


def get_hatch_version(*, cwd: PathLike = PWD) -> Version:
    """Get the version according to `hatch`."""
    with temp_environ(NO_COLOR="1"):
        output = check_output(["hatch", "version"], cwd=cwd, text=True)
    return parse_version(output.strip("\n"))


##


def get_version(*, cwd: PathLike = PWD, ref: str = MASTER) -> Version:
    """Get the version."""
    git = get_git_version(cwd=cwd, ref=ref)
    hatch = get_hatch_version(cwd=cwd)
    if hatch < git:
        return hatch.with_suffix(suffix="behind")
    if hatch == git:
        return hatch
    if hatch in {git.bump_major(), git.bump_minor(), git.bump_patch()}:
        return hatch.with_suffix(suffix="dirty")
    raise GetVersionError(git=git, hatch=hatch)


@dataclass(kw_only=True, slots=True)
class GetVersionError(Exception):
    git: Version
    hatch: Version

    @override
    def __str__(self) -> str:
        return f"`hatch` version is ahead of `git` version in an incompatible way; got {self.hatch} and {self.git}"


##


def parse_version(version: str, /) -> Version:
    """Parse a string into a version object."""
    result = _PATTERN.search(version)
    if not result:
        raise ParseVersionError(version=version)
    major_str, minor_str, patch_str, suffix = result.groups()
    return Version(int(major_str), int(minor_str), int(patch_str), suffix=suffix)


@dataclass(kw_only=True, slots=True)
class ParseVersionError(Exception):
    version: str

    @override
    def __str__(self) -> str:
        return f"Invalid version string: {self.version!r}"


__all__ = [
    "GetGitVersionError",
    "GetVersionError",
    "ParseVersionError",
    "Version",
    "VersionError",
    "get_git_version",
    "get_hatch_version",
    "get_version",
    "parse_version",
]
