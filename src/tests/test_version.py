from __future__ import annotations

from re import search
from typing import TYPE_CHECKING

from hypothesis import assume, given, settings
from hypothesis.strategies import (
    DataObject,
    booleans,
    data,
    integers,
    none,
    sampled_from,
)
from pytest import raises

from utilities.git import MASTER
from utilities.hypothesis import git_repos, pairs, text_ascii, versions
from utilities.version import (
    GetGitVersionError,
    GetVersionError,
    ParseVersionError,
    Version,
    _VersionEmptySuffixError,
    _VersionNegativeMajorVersionError,
    _VersionNegativeMinorVersionError,
    _VersionNegativePatchVersionError,
    _VersionZeroError,
    get_git_version,
    get_hatch_version,
    get_version,
    parse_version,
)

if TYPE_CHECKING:
    from pathlib import Path


class TestGetGitVersion:
    @given(data=data(), version=versions())
    @settings(max_examples=1)
    def test_main(self, *, data: DataObject, version: Version) -> None:
        repo = data.draw(git_repos(git_version=version))
        result = get_git_version(cwd=repo, ref=MASTER)
        assert result == version

    @given(repo=git_repos())
    @settings(max_examples=1)
    def test_error(self, *, repo: Path) -> None:
        with raises(GetGitVersionError, match="Reference '.*' at '.*' has no tags"):
            _ = get_git_version(cwd=repo, ref=MASTER)


class TestGetHatchVersion:
    @given(data=data(), version=versions())
    @settings(max_examples=1)
    def test_main(self, *, data: DataObject, version: Version) -> None:
        repo = data.draw(git_repos(hatch_version=version))
        result = get_hatch_version(cwd=repo)
        assert result == version


class TestGetVersion:
    @given(data=data(), version=versions())
    @settings(max_examples=1)
    def test_equal(self, *, data: DataObject, version: Version) -> None:
        repo = data.draw(git_repos(git_version=version, hatch_version=version))
        result = get_version(cwd=repo, ref=MASTER)
        assert result == version

    @given(data=data(), versions=pairs(versions(), unique=True, sorted=True))
    @settings(max_examples=1)
    def test_behind(
        self, *, data: DataObject, versions: tuple[Version, Version]
    ) -> None:
        hatch, git = versions
        repo = data.draw(git_repos(git_version=git, hatch_version=hatch))
        result = get_version(cwd=repo, ref=MASTER)
        expected = hatch.with_suffix(suffix="behind")
        assert result == expected

    @given(data=data(), git=versions())
    @settings(max_examples=1)
    def test_dirty(self, *, data: DataObject, git: Version) -> None:
        hatch = data.draw(
            sampled_from([git.bump_major(), git.bump_minor(), git.bump_patch()])
        )
        repo = data.draw(git_repos(git_version=git, hatch_version=hatch))
        result = get_version(cwd=repo, ref=MASTER)
        expected = hatch.with_suffix(suffix="dirty")
        assert result == expected

    @given(data=data(), versions=pairs(versions(), unique=True, sorted=True))
    @settings(max_examples=1)
    def test_error(
        self, *, data: DataObject, versions: tuple[Version, Version]
    ) -> None:
        git, hatch = versions
        _ = assume(hatch not in [git.bump_major(), git.bump_minor(), git.bump_patch()])
        repo = data.draw(git_repos(git_version=git, hatch_version=hatch))
        with raises(
            GetVersionError,
            match="`hatch` version is ahead of `git` version in an incompatible way; got .* and .*",
        ):
            _ = get_version(cwd=repo, ref=MASTER)


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
