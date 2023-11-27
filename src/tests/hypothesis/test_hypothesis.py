from __future__ import annotations

import datetime as dt
from itertools import pairwise
from re import search
from subprocess import PIPE, check_output
from typing import Any, cast

from hypothesis import HealthCheck, Phase, assume, given, settings
from hypothesis.errors import InvalidArgument
from hypothesis.strategies import (
    DataObject,
    booleans,
    data,
    datetimes,
    floats,
    integers,
    just,
    none,
)
from pytest import raises
from semver import Version

from utilities.datetime import UTC
from utilities.git import _GET_BRANCH_NAME
from utilities.hypothesis import (
    assume_does_not_raise,
    datetimes_utc,
    git_repos,
    hashables,
    lists_fixed_length,
    settings_with_reduced_examples,
    setup_hypothesis_profiles,
    slices,
    text_ascii,
    text_clean,
    text_printable,
    versions,
)
from utilities.os import temp_environ
from utilities.pathvalidate import valid_path


class TestAssumeDoesNotRaise:
    @given(x=booleans())
    def test_no_match_and_suppressed(self, *, x: bool) -> None:
        with assume_does_not_raise(ValueError):
            if x is True:
                msg = "x is True"
                raise ValueError(msg)
        assert x is False

    @given(x=booleans())
    def test_no_match_and_not_suppressed(self, *, x: bool) -> None:
        msg = "x is True"
        if x is True:
            with raises(ValueError, match=msg), assume_does_not_raise(RuntimeError):
                raise ValueError(msg)

    @given(x=booleans())
    def test_with_match_and_suppressed(self, *, x: bool) -> None:
        msg = "x is True"
        if x is True:
            with assume_does_not_raise(ValueError, match=msg):
                raise ValueError(msg)
        assert x is False

    @given(x=just(True))
    def test_with_match_and_not_suppressed(self, *, x: bool) -> None:
        msg = "x is True"
        if x is True:
            with raises(ValueError, match=msg), assume_does_not_raise(
                ValueError, match="wrong"
            ):
                raise ValueError(msg)


class TestDatetimesUTC:
    @given(data=data(), min_value=datetimes(), max_value=datetimes())
    def test_main(
        self, *, data: DataObject, min_value: dt.datetime, max_value: dt.datetime
    ) -> None:
        min_value, max_value = (v.replace(tzinfo=UTC) for v in [min_value, max_value])
        _ = assume(min_value <= max_value)
        datetime = data.draw(datetimes_utc(min_value=min_value, max_value=max_value))
        assert min_value <= datetime <= max_value


class TestGitRepos:
    @given(data=data())
    @settings_with_reduced_examples(suppress_health_check={HealthCheck.filter_too_much})
    def test_main(self, *, data: DataObject) -> None:
        branch = data.draw(text_ascii(min_size=1) | none())
        path = data.draw(git_repos(branch=branch))
        assert set(path.iterdir()) == {valid_path(path, ".git")}
        if branch is not None:
            output = check_output(
                _GET_BRANCH_NAME,  # noqa: S603
                stderr=PIPE,
                cwd=path,
                text=True,
            )
            assert output.strip("\n") == branch


class TestHashables:
    @given(data=data())
    def test_main(self, *, data: DataObject) -> None:
        x = data.draw(hashables())
        _ = hash(x)


class TestReducedExamples:
    @given(frac=floats(0.0, 10.0))
    def test_main(self, *, frac: float) -> None:
        @settings_with_reduced_examples(frac)
        def test() -> None:
            pass

        result = cast(Any, test)._hypothesis_internal_use_settings.max_examples  # noqa: SLF001
        expected = max(round(frac * settings().max_examples), 1)
        assert result == expected


class TestSlices:
    @given(data=data(), iter_len=integers(0, 10))
    def test_main(self, *, data: DataObject, iter_len: int) -> None:
        slice_len = data.draw(integers(0, iter_len) | none())
        slice_ = data.draw(slices(iter_len, slice_len=slice_len))
        range_slice = range(iter_len)[slice_]
        assert all(i + 1 == j for i, j in pairwise(range_slice))
        if slice_len is not None:
            assert len(range_slice) == slice_len

    @given(data=data(), iter_len=integers(0, 10))
    def test_error(self, *, data: DataObject, iter_len: int) -> None:
        with raises(
            InvalidArgument, match=r"Slice length \d+ exceeds iterable length \d+"
        ):
            _ = data.draw(slices(iter_len, slice_len=iter_len + 1))


class TestSetupHypothesisProfiles:
    def test_main(self) -> None:
        setup_hypothesis_profiles()
        curr = settings()
        assert Phase.shrink in curr.phases
        assert curr.max_examples in {10, 100, 1000}

    def test_no_shrink(self) -> None:
        with temp_environ({"HYPOTHESIS_NO_SHRINK": "1"}):
            setup_hypothesis_profiles()
        assert Phase.shrink not in settings().phases

    @given(max_examples=integers(1, 100))
    def test_max_examples(self, *, max_examples: int) -> None:
        with temp_environ({"HYPOTHESIS_MAX_EXAMPLES": str(max_examples)}):
            setup_hypothesis_profiles()
        assert settings().max_examples == max_examples


class TestTextClean:
    @given(
        data=data(),
        min_size=integers(0, 100),
        max_size=integers(0, 100) | none(),
        disallow_na=booleans(),
    )
    def test_main(
        self,
        *,
        data: DataObject,
        min_size: int,
        max_size: int | None,
        disallow_na: bool,
    ) -> None:
        with assume_does_not_raise(InvalidArgument, AssertionError):
            text = data.draw(
                text_clean(
                    min_size=min_size, max_size=max_size, disallow_na=disallow_na
                )
            )
        assert search("^\\S[^\\r\\n]*$|^$", text)
        assert len(text) >= min_size
        if max_size is not None:
            assert len(text) <= max_size
        if disallow_na:
            assert text != "NA"


class TestTextPrintable:
    @given(
        data=data(),
        min_size=integers(0, 100),
        max_size=integers(0, 100) | none(),
        disallow_na=booleans(),
    )
    def test_main(
        self,
        *,
        data: DataObject,
        min_size: int,
        max_size: int | None,
        disallow_na: bool,
    ) -> None:
        with assume_does_not_raise(InvalidArgument, AssertionError):
            text = data.draw(
                text_printable(
                    min_size=min_size, max_size=max_size, disallow_na=disallow_na
                )
            )
        assert search(r"^[0-9A-Za-z!\"#$%&'()*+,-./:;<=>?@\[\\\]^_`{|}~\s]*$", text)
        assert len(text) >= min_size
        if max_size is not None:
            assert len(text) <= max_size
        if disallow_na:
            assert text != "NA"


class TestVersions:
    @given(data=data())
    def test_main(self, data: DataObject) -> None:
        version = data.draw(versions())
        assert isinstance(version, Version)

    @given(data=data())
    def test_min_version(self, data: DataObject) -> None:
        min_version = data.draw(versions())
        version = data.draw(versions(min_version=min_version))
        assert version >= min_version

    @given(data=data())
    def test_max_version(self, data: DataObject) -> None:
        max_version = data.draw(versions())
        version = data.draw(versions(max_version=max_version))
        assert version <= max_version

    @given(data=data())
    def test_min_and_max_version(self, data: DataObject) -> None:
        version1, version2 = data.draw(lists_fixed_length(versions(), 2))
        min_version = min(version1, version2)
        max_version = max(version1, version2)
        version = data.draw(versions(min_version=min_version, max_version=max_version))
        assert min_version <= version <= max_version

    @given(data=data())
    def test_error(self, data: DataObject) -> None:
        version1, version2 = data.draw(lists_fixed_length(versions(), 2))
        _ = assume(version1 != version2)
        with raises(InvalidArgument):
            _ = data.draw(
                versions(
                    min_version=max(version1, version2),
                    max_version=min(version1, version2),
                )
            )
