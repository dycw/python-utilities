from __future__ import annotations

from re import search
from typing import TYPE_CHECKING

from hypothesis import given
from hypothesis.strategies import booleans, integers, none
from pytest import mark, param, raises

from utilities.hypothesis import sentinels, text_ascii, version2s, version3s
from utilities.version import (
    ParseVersion2Or3Error,
    Version2,
    Version2Or3,
    Version3,
    _Version2EmptySuffixError,
    _Version2NegativeMajorVersionError,
    _Version2NegativeMinorVersionError,
    _Version2ParseError,
    _Version2ZeroError,
    _Version3EmptySuffixError,
    _Version3NegativeMajorVersionError,
    _Version3NegativeMinorVersionError,
    _Version3NegativePatchVersionError,
    _Version3ParseError,
    _Version3ZeroError,
    parse_version_2_or_3,
    to_version3,
)

if TYPE_CHECKING:
    from utilities.constants import Sentinel


class TestParseVersion2Or3:
    @mark.parametrize(
        ("text", "expected"),
        [
            param("0.1", Version2(0, 1)),
            param("0.12", Version2(0, 12)),
            param("1.23", Version2(1, 23)),
            param("12.34", Version2(12, 34)),
            param("0.0.1", Version3(0, 0, 1)),
            param("0.0.12", Version3(0, 0, 12)),
            param("0.1.23", Version3(0, 1, 23)),
            param("0.12.34", Version3(0, 12, 34)),
            param("1.23.45", Version3(1, 23, 45)),
            param("12.34.56", Version3(12, 34, 56)),
        ],
    )
    def test_main(self, *, text: str, expected: Version2Or3) -> None:
        result = parse_version_2_or_3(text)
        assert result == expected

    def test_error(self) -> None:
        with raises(
            ParseVersion2Or3Error,
            match=r"Unable to parse Version2 or Version3; got '.*'",
        ):
            _ = parse_version_2_or_3("invalid")


class TestVersion2:
    @given(version=version2s())
    def test_hashable(self, *, version: Version2) -> None:
        _ = hash(version)

    @given(version1=version2s(), version2=version2s())
    def test_orderable(self, *, version1: Version2, version2: Version2) -> None:
        assert (version1 <= version2) or (version1 >= version2)

    @given(version=version2s())
    def test_parse(self, *, version: Version2) -> None:
        parsed = Version2.parse(str(version))
        assert parsed == version

    @given(version=version2s(suffix=booleans()))
    def test_repr(self, *, version: Version2) -> None:
        result = repr(version)
        assert search(r"^\d+.\d+", result)

    @given(version=version2s())
    def test_bump_major(self, *, version: Version2) -> None:
        bumped = version.bump_major()
        assert version < bumped
        assert bumped.major == version.major + 1
        assert bumped.minor == 0
        assert bumped.suffix is None

    @given(version=version2s())
    def test_bump_minor(self, *, version: Version2) -> None:
        bumped = version.bump_minor()
        assert version < bumped
        assert bumped.major == version.major
        assert bumped.minor == version.minor + 1
        assert bumped.suffix is None

    @given(version=version2s(), patch=integers(min_value=0))
    def test_version3(self, *, version: Version2, patch: int) -> None:
        new = version.version3(patch=patch).version2
        assert new == version

    @given(version=version2s(), suffix=text_ascii(min_size=1) | none())
    def test_with_suffix(self, *, version: Version2, suffix: str | None) -> None:
        new = version.with_suffix(suffix=suffix)
        assert new.major == version.major
        assert new.minor == version.minor
        assert new.suffix == suffix

    @given(version=version2s())
    def test_error_order(self, *, version: Version2) -> None:
        with raises(TypeError):
            _ = version <= None

    def test_error_zero(self) -> None:
        with raises(
            _Version2ZeroError, match=r"Version must be greater than zero; got 0\.0"
        ):
            _ = Version2(0, 0)

    @given(major=integers(max_value=-1))
    def test_error_negative_major_version(self, *, major: int) -> None:
        with raises(
            _Version2NegativeMajorVersionError,
            match=r"Major version must be non-negative; got .*",
        ):
            _ = Version2(major=major)

    @given(minor=integers(max_value=-1))
    def test_error_negative_minor_version(self, *, minor: int) -> None:
        with raises(
            _Version2NegativeMinorVersionError,
            match=r"Minor version must be non-negative; got .*",
        ):
            _ = Version2(minor=minor)

    def test_error_empty_suffix(self) -> None:
        with raises(
            _Version2EmptySuffixError, match=r"Suffix must be non-empty; got .*"
        ):
            _ = Version2(suffix="")

    @mark.parametrize("text", [param("invalid"), param("0.0.1")])
    def test_error_parse(self, *, text: str) -> None:
        with raises(_Version2ParseError, match=r"Unable to parse version; got '.*'"):
            _ = Version2.parse(text)


class TestVersion3:
    @given(version=version3s())
    def test_hashable(self, *, version: Version3) -> None:
        _ = hash(version)

    @given(version1=version3s(), version2=version3s())
    def test_orderable(self, *, version1: Version3, version2: Version3) -> None:
        assert (version1 <= version2) or (version1 >= version2)

    @given(version=version3s())
    def test_parse(self, *, version: Version3) -> None:
        parsed = Version3.parse(str(version))
        assert parsed == version

    @given(version=version3s(suffix=booleans()))
    def test_repr(self, *, version: Version3) -> None:
        result = repr(version)
        assert search(r"^\d+\.\d+\.\d+", result)

    @given(version=version3s())
    def test_bump_major(self, *, version: Version3) -> None:
        bumped = version.bump_major()
        assert version < bumped
        assert bumped.major == version.major + 1
        assert bumped.minor == 0
        assert bumped.patch == 0
        assert bumped.suffix is None

    @given(version=version3s())
    def test_bump_minor(self, *, version: Version3) -> None:
        bumped = version.bump_minor()
        assert version < bumped
        assert bumped.major == version.major
        assert bumped.minor == version.minor + 1
        assert bumped.patch == 0
        assert bumped.suffix is None

    @given(version=version3s())
    def test_bump_patch(self, *, version: Version3) -> None:
        bumped = version.bump_patch()
        assert version < bumped
        assert bumped.major == version.major
        assert bumped.minor == version.minor
        assert bumped.patch == version.patch + 1
        assert bumped.suffix is None

    @given(version=version3s(), suffix=text_ascii(min_size=1) | none())
    def test_with_suffix(self, *, version: Version3, suffix: str | None) -> None:
        new = version.with_suffix(suffix=suffix)
        assert new.major == version.major
        assert new.minor == version.minor
        assert new.patch == version.patch
        assert new.suffix == suffix

    @given(version=version3s())
    def test_version2(self, *, version: Version3) -> None:
        new = version.version2.version3(patch=version.patch)
        assert new == version

    @given(version=version3s())
    def test_error_order(self, *, version: Version3) -> None:
        with raises(TypeError):
            _ = version <= None

    def test_error_zero(self) -> None:
        with raises(
            _Version3ZeroError, match=r"Version must be greater than zero; got 0\.0\.0"
        ):
            _ = Version3(0, 0, 0)

    @given(major=integers(max_value=-1))
    def test_error_negative_major_version(self, *, major: int) -> None:
        with raises(
            _Version3NegativeMajorVersionError,
            match=r"Major version must be non-negative; got .*",
        ):
            _ = Version3(major=major)

    @given(minor=integers(max_value=-1))
    def test_error_negative_minor_version(self, *, minor: int) -> None:
        with raises(
            _Version3NegativeMinorVersionError,
            match=r"Minor version must be non-negative; got .*",
        ):
            _ = Version3(minor=minor)

    @given(patch=integers(max_value=-1))
    def test_error_negative_patch_version(self, *, patch: int) -> None:
        with raises(
            _Version3NegativePatchVersionError,
            match=r"Patch version must be non-negative; got .*",
        ):
            _ = Version3(patch=patch)

    def test_error_empty_suffix(self) -> None:
        with raises(
            _Version3EmptySuffixError, match=r"Suffix must be non-empty; got .*"
        ):
            _ = Version3(suffix="")

    def test_error_parse(self) -> None:
        with raises(
            _Version3ParseError, match=r"Unable to parse version; got 'invalid'"
        ):
            _ = Version3.parse("invalid")


class TestToVersion3:
    @given(version=version3s())
    def test_version(self, *, version: Version3) -> None:
        assert to_version3(version) == version

    @given(version=version3s())
    def test_str(self, *, version: Version3) -> None:
        assert to_version3(str(version)) == version

    @given(version=none() | sentinels())
    def test_none_or_sentinel(self, *, version: None | Sentinel) -> None:
        assert to_version3(version) is version

    @given(version=version3s())
    def test_callable(self, *, version: Version3) -> None:
        assert to_version3(lambda: version) == version
