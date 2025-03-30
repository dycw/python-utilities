from __future__ import annotations

from re import search
from typing import TYPE_CHECKING

from hypothesis import given
from hypothesis.strategies import booleans, integers, none
from pytest import raises

from utilities.hypothesis import sentinels, text_ascii, versions
from utilities.version import (
    ParseVersionError,
    Version,
    _VersionEmptySuffixError,
    _VersionNegativeMajorVersionError,
    _VersionNegativeMinorVersionError,
    _VersionNegativePatchVersionError,
    _VersionZeroError,
    get_version,
    parse_version,
)

if TYPE_CHECKING:
    from utilities.sentinel import Sentinel


class TestGetVersion:
    @given(version=versions())
    def test_version(self, *, version: Version) -> None:
        assert get_version(version=version) == version

    @given(version=versions())
    def test_str(self, *, version: Version) -> None:
        assert get_version(version=str(version)) == version

    @given(version=none() | sentinels())
    def test_none_or_sentinel(self, *, version: None | Sentinel) -> None:
        assert get_version(version=version) is version

    @given(version=versions())
    def test_callable(self, *, version: Version) -> None:
        assert get_version(version=lambda: version) == version


class TestParseVersion:
    @given(version=versions())
    def test_main(self, *, version: Version) -> None:
        parsed = parse_version(str(version))
        assert parsed == version

    def test_error(self) -> None:
        with raises(ParseVersionError, match="Invalid version string: 'invalid'"):
            _ = parse_version("invalid")


class TestVersion:
    @given(version=versions())
    def test_hashable(self, *, version: Version) -> None:
        _ = hash(version)

    @given(version1=versions(), version2=versions())
    def test_orderable(self, *, version1: Version, version2: Version) -> None:
        assert (version1 <= version2) or (version1 >= version2)

    @given(version=versions(suffix=booleans()))
    def test_repr(self, *, version: Version) -> None:
        result = repr(version)
        assert search(r"^\d+\.\d+\.\d+", result)

    @given(version=versions())
    def test_bump_major(self, *, version: Version) -> None:
        bumped = version.bump_major()
        assert version < bumped
        assert bumped.major == version.major + 1
        assert bumped.minor == 0
        assert bumped.patch == 0
        assert bumped.suffix is None

    @given(version=versions())
    def test_bump_minor(self, *, version: Version) -> None:
        bumped = version.bump_minor()
        assert version < bumped
        assert bumped.major == version.major
        assert bumped.minor == version.minor + 1
        assert bumped.patch == 0
        assert bumped.suffix is None

    @given(version=versions())
    def test_bump_patch(self, *, version: Version) -> None:
        bumped = version.bump_patch()
        assert version < bumped
        assert bumped.major == version.major
        assert bumped.minor == version.minor
        assert bumped.patch == version.patch + 1
        assert bumped.suffix is None

    @given(version=versions(), suffix=text_ascii(min_size=1) | none())
    def test_with_suffix(self, *, version: Version, suffix: str | None) -> None:
        new = version.with_suffix(suffix=suffix)
        assert new.major == version.major
        assert new.minor == version.minor
        assert new.patch == version.patch
        assert new.suffix == suffix

    @given(version=versions())
    def test_error_order(self, *, version: Version) -> None:
        with raises(TypeError):
            _ = version <= None

    def test_error_zero(self) -> None:
        with raises(
            _VersionZeroError, match="Version must be greater than zero; got 0.0.0"
        ):
            _ = Version(0, 0, 0)

    @given(major=integers(max_value=-1))
    def test_error_negative_major_version(self, *, major: int) -> None:
        with raises(
            _VersionNegativeMajorVersionError,
            match="Major version must be non-negative; got .*",
        ):
            _ = Version(major=major)

    @given(minor=integers(max_value=-1))
    def test_error_negative_minor_version(self, *, minor: int) -> None:
        with raises(
            _VersionNegativeMinorVersionError,
            match="Minor version must be non-negative; got .*",
        ):
            _ = Version(minor=minor)

    @given(patch=integers(max_value=-1))
    def test_error_negative_patch_version(self, *, patch: int) -> None:
        with raises(
            _VersionNegativePatchVersionError,
            match="Patch version must be non-negative; got .*",
        ):
            _ = Version(patch=patch)

    def test_error_empty_suffix(self) -> None:
        with raises(_VersionEmptySuffixError, match="Suffix must be non-empty; got .*"):
            _ = Version(suffix="")
