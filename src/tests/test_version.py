from __future__ import annotations

from hypothesis import given
from hypothesis.strategies import integers, none
from pytest import raises

from utilities.hypothesis import text_ascii
from utilities.version import (
    Version,
    _VersionEmptySuffixError,
    _VersionNegativeMajorVersionError,
    _VersionNegativeMinorVersionError,
    _VersionNegativePatchVersionError,
    get_hatch_version,
    parse_version,
)


class TestGetHatchVersion:
    def test_main(self) -> None:
        version = get_hatch_version()
        assert isinstance(version, Version)


class TestParseVersion:
    @given(
        major=integers(min_value=0),
        minor=integers(min_value=0),
        patch=integers(min_value=0),
        suffix=text_ascii(min_size=1) | none(),
    )
    def test_main(
        self, *, major: int, minor: int, patch: int, suffix: str | None
    ) -> None:
        version = Version(major=major, minor=minor, patch=patch, suffix=suffix)
        parsed = parse_version(str(version))
        assert parsed == version

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

    def test_error_empy_suffix(self) -> None:
        with raises(_VersionEmptySuffixError, match="Suffix must be non-empty; got .*"):
            _ = Version(suffix="")
