from __future__ import annotations

import re
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, field, replace
from functools import total_ordering
from typing import Any, Self, assert_never, overload, override

from utilities.constants import Sentinel
from utilities.types import MaybeCallable, MaybeStr

type Version2Like = MaybeStr[Version2]
type Version3Like = MaybeStr[Version3]
type Version2Or3 = Version2 | Version3
type MaybeCallableVersion3Like = MaybeCallable[Version3Like]


##


_PARSE_VERSION2_PATTERN = re.compile(r"^(\d+)\.(\d+)(?!\.\d)(?:-(\w+))?")


@dataclass(repr=False, frozen=True, slots=True)
@total_ordering
class Version2:
    """A version identifier."""

    major: int = 0
    minor: int = 1
    suffix: str | None = field(default=None, kw_only=True)

    def __post_init__(self) -> None:
        if (self.major == 0) and (self.minor == 0):
            raise _Version2ZeroError(major=self.major, minor=self.minor)
        if self.major < 0:
            raise _Version2NegativeMajorVersionError(major=self.major)
        if self.minor < 0:
            raise _Version2NegativeMinorVersionError(minor=self.minor)
        if (self.suffix is not None) and (len(self.suffix) == 0):
            raise _Version2EmptySuffixError(suffix=self.suffix)

    def __le__(self, other: Any, /) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        self_as_tuple = (
            self.major,
            self.minor,
            "" if self.suffix is None else self.suffix,
        )
        other_as_tuple = (
            other.major,
            other.minor,
            "" if other.suffix is None else other.suffix,
        )
        return self_as_tuple <= other_as_tuple

    @override
    def __repr__(self) -> str:
        version = f"{self.major}.{self.minor}"
        if self.suffix is not None:
            version = f"{version}-{self.suffix}"
        return version

    @classmethod
    def parse(cls, text: str, /) -> Self:
        """Parse a string into a Version2 object."""
        try:
            ((major, minor, suffix),) = _PARSE_VERSION2_PATTERN.findall(text)
        except ValueError:
            raise _Version2ParseError(text=text) from None
        return cls(int(major), int(minor), suffix=None if suffix == "" else suffix)

    def bump_major(self) -> Self:
        """Bump the major component."""
        return type(self)(self.major + 1, 0)

    def bump_minor(self) -> Self:
        """Bump the minor component."""
        return type(self)(self.major, self.minor + 1)

    def version3(self, *, patch: int = 0) -> Version3:
        """Convert to a Version3 object."""
        return Version3(self.major, self.minor, patch, suffix=self.suffix)

    def with_suffix(self, *, suffix: str | None = None) -> Self:
        """Replace the suffix."""
        return replace(self, suffix=suffix)


@dataclass(kw_only=True, slots=True)
class Version2Error(Exception): ...


@dataclass(kw_only=True, slots=True)
class _Version2ZeroError(Version2Error):
    major: int
    minor: int

    @override
    def __str__(self) -> str:
        return f"Version must be greater than zero; got {self.major}.{self.minor}"


@dataclass(kw_only=True, slots=True)
class _Version2NegativeMajorVersionError(Version2Error):
    major: int

    @override
    def __str__(self) -> str:
        return f"Major version must be non-negative; got {self.major}"


@dataclass(kw_only=True, slots=True)
class _Version2NegativeMinorVersionError(Version2Error):
    minor: int

    @override
    def __str__(self) -> str:
        return f"Minor version must be non-negative; got {self.minor}"


@dataclass(kw_only=True, slots=True)
class _Version2EmptySuffixError(Version2Error):
    suffix: str

    @override
    def __str__(self) -> str:
        return f"Suffix must be non-empty; got {self.suffix!r}"


@dataclass(kw_only=True, slots=True)
class _Version2ParseError(Version2Error):
    text: str

    @override
    def __str__(self) -> str:
        return f"Unable to parse version; got {self.text!r}"


##


_PARSE_VERSION3_PATTERN = re.compile(r"^(\d+)\.(\d+)\.(\d+)(?:-(\w+))?")


@dataclass(repr=False, frozen=True, slots=True)
@total_ordering
class Version3:
    """A version identifier."""

    major: int = 0
    minor: int = 0
    patch: int = 1
    suffix: str | None = field(default=None, kw_only=True)

    def __post_init__(self) -> None:
        if (self.major == 0) and (self.minor == 0) and (self.patch == 0):
            raise _Version3ZeroError(
                major=self.major, minor=self.minor, patch=self.patch
            )
        if self.major < 0:
            raise _Version3NegativeMajorVersionError(major=self.major)
        if self.minor < 0:
            raise _Version3NegativeMinorVersionError(minor=self.minor)
        if self.patch < 0:
            raise _Version3NegativePatchVersionError(patch=self.patch)
        if (self.suffix is not None) and (len(self.suffix) == 0):
            raise _Version3EmptySuffixError(suffix=self.suffix)

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

    @classmethod
    def parse(cls, text: str, /) -> Self:
        """Parse a string into a Version3 object."""
        try:
            ((major, minor, patch, suffix),) = _PARSE_VERSION3_PATTERN.findall(text)
        except ValueError:
            raise _Version3ParseError(text=text) from None
        return cls(
            int(major), int(minor), int(patch), suffix=None if suffix == "" else suffix
        )

    def bump_major(self) -> Self:
        """Bump the major component."""
        return type(self)(self.major + 1, 0, 0)

    def bump_minor(self) -> Self:
        """Bump the minor component."""
        return type(self)(self.major, self.minor + 1, 0)

    def bump_patch(self) -> Self:
        """Bump the patch component."""
        return type(self)(self.major, self.minor, self.patch + 1)

    @property
    def version2(self) -> Version2:
        """Return the major/minor components only."""
        return Version2(self.major, self.minor, suffix=self.suffix)

    def with_suffix(self, *, suffix: str | None = None) -> Self:
        """Replace the suffix."""
        return replace(self, suffix=suffix)


@dataclass(kw_only=True, slots=True)
class Version3Error(Exception): ...


@dataclass(kw_only=True, slots=True)
class _Version3ZeroError(Version3Error):
    major: int
    minor: int
    patch: int

    @override
    def __str__(self) -> str:
        return f"Version must be greater than zero; got {self.major}.{self.minor}.{self.patch}"


@dataclass(kw_only=True, slots=True)
class _Version3NegativeMajorVersionError(Version3Error):
    major: int

    @override
    def __str__(self) -> str:
        return f"Major version must be non-negative; got {self.major}"


@dataclass(kw_only=True, slots=True)
class _Version3NegativeMinorVersionError(Version3Error):
    minor: int

    @override
    def __str__(self) -> str:
        return f"Minor version must be non-negative; got {self.minor}"


@dataclass(kw_only=True, slots=True)
class _Version3NegativePatchVersionError(Version3Error):
    patch: int

    @override
    def __str__(self) -> str:
        return f"Patch version must be non-negative; got {self.patch}"


@dataclass(kw_only=True, slots=True)
class _Version3EmptySuffixError(Version3Error):
    suffix: str

    @override
    def __str__(self) -> str:
        return f"Suffix must be non-empty; got {self.suffix!r}"


@dataclass(kw_only=True, slots=True)
class _Version3ParseError(Version3Error):
    text: str

    @override
    def __str__(self) -> str:
        return f"Unable to parse version; got {self.text!r}"


##


def parse_version_2_or_3(text: str, /) -> Version2Or3:
    """Parse a string into a Version2 or Version3 object."""
    with suppress(_Version3ParseError):
        return Version3.parse(text)
    with suppress(_Version2ParseError):
        return Version2.parse(text)
    raise ParseVersion2Or3Error(text=text)


@dataclass(kw_only=True, slots=True)
class ParseVersion2Or3Error(Exception):
    text: str

    @override
    def __str__(self) -> str:
        return f"Unable to parse Version2 or Version3; got {self.text!r}"


##


@overload
def to_version3(version: MaybeCallableVersion3Like, /) -> Version3: ...
@overload
def to_version3(version: None, /) -> None: ...
@overload
def to_version3(version: Sentinel, /) -> Sentinel: ...
def to_version3(
    version: MaybeCallableVersion3Like | None | Sentinel, /
) -> Version3 | None | Sentinel:
    """Convert to a version."""
    match version:
        case Version3() | None | Sentinel():
            return version
        case str():
            return Version3.parse(version)
        case Callable() as func:
            return to_version3(func())
        case never:
            assert_never(never)


##
__all__ = [
    "MaybeCallableVersion3Like",
    "ParseVersion2Or3Error",
    "Version2",
    "Version2Error",
    "Version2Like",
    "Version2Or3",
    "Version3",
    "Version3Error",
    "Version3Like",
    "parse_version_2_or_3",
    "to_version3",
]
