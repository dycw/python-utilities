from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from functools import total_ordering
from typing import Any, Self, assert_never, overload, override

from utilities.sentinel import Sentinel, sentinel
from utilities.types import MaybeCallable, MaybeStr

type VersionLike = MaybeStr[Version]
type MaybeCallableVersionLike = MaybeCallable[VersionLike]


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


@overload
def get_version(*, version: MaybeCallableVersionLike) -> Version: ...
@overload
def get_version(*, version: None) -> None: ...
@overload
def get_version(*, version: Sentinel) -> Sentinel: ...
@overload
def get_version(
    *, version: MaybeCallableVersionLike | Sentinel
) -> Version | Sentinel: ...
@overload
def get_version(
    *, version: MaybeCallableVersionLike | None | Sentinel = sentinel
) -> Version | None | Sentinel: ...
def get_version(
    *, version: MaybeCallableVersionLike | None | Sentinel = sentinel
) -> Version | None | Sentinel:
    """Get the version."""
    match version:
        case Version() | None | Sentinel():
            return version
        case str():
            return parse_version(version)
        case Callable() as func:
            return get_version(version=func())
        case _ as never:
            assert_never(never)


##


def parse_version(version: str, /) -> Version:
    """Parse a string into a version object."""
    try:
        ((major, minor, patch, suffix),) = _PARSE_VERSION_PATTERN.findall(version)
    except ValueError:
        raise ParseVersionError(version=version) from None
    return Version(
        int(major), int(minor), int(patch), suffix=None if suffix == "" else suffix
    )


_PARSE_VERSION_PATTERN = re.compile(r"^(\d+)\.(\d+)\.(\d+)(?:-(\w+))?")


@dataclass(kw_only=True, slots=True)
class ParseVersionError(Exception):
    version: str

    @override
    def __str__(self) -> str:
        return f"Invalid version string: {self.version!r}"


__all__ = [
    "MaybeCallableVersionLike",
    "ParseVersionError",
    "Version",
    "VersionError",
    "VersionLike",
    "get_version",
    "parse_version",
]
