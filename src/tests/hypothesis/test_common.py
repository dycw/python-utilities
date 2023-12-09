from __future__ import annotations

from collections.abc import Set as AbstractSet
from math import inf, isfinite, isinf, isnan
from pathlib import Path
from re import search

from hypothesis import given
from hypothesis.errors import InvalidArgument
from hypothesis.strategies import (
    DataObject,
    DrawFn,
    booleans,
    composite,
    data,
    floats,
    integers,
    none,
    sets,
)
from pytest import mark, param

from utilities._hypothesis.common import (
    floats_extra,
    lists_fixed_length,
    temp_dirs,
    temp_paths,
    text_ascii,
)
from utilities.hypothesis import assume_does_not_raise
from utilities.pathvalidate import valid_path
from utilities.platform import maybe_yield_lower_case
from utilities.tempfile import TemporaryDirectory


class TestFloatsExtra:
    @given(
        data=data(),
        min_value=floats() | none(),
        max_value=floats() | none(),
        allow_nan=booleans(),
        allow_inf=booleans(),
        allow_pos_inf=booleans(),
        allow_neg_inf=booleans(),
        integral=booleans(),
    )
    def test_main(
        self,
        *,
        data: DataObject,
        min_value: float | None,
        max_value: float | None,
        allow_nan: bool,
        allow_inf: bool,
        allow_pos_inf: bool,
        allow_neg_inf: bool,
        integral: bool,
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            x = data.draw(
                floats_extra(
                    min_value=min_value,
                    max_value=max_value,
                    allow_nan=allow_nan,
                    allow_inf=allow_inf,
                    allow_pos_inf=allow_pos_inf,
                    allow_neg_inf=allow_neg_inf,
                    integral=integral,
                )
            )
        if min_value is not None:
            assert (isfinite(x) and x >= min_value) or not isfinite(x)
        if max_value is not None:
            assert (isfinite(x) and x <= max_value) or not isfinite(x)
        if not allow_nan:
            assert not isnan(x)
        if not allow_inf:
            if not (allow_pos_inf or allow_neg_inf):
                assert not isinf(x)
            if not allow_pos_inf:
                assert x != inf
            if not allow_neg_inf:
                assert x != -inf
        if integral:
            assert (isfinite(x) and x == round(x)) or not isfinite(x)

    @given(data=data(), min_value=floats() | none(), max_value=floats() | none())
    def test_finite_and_integral(
        self, *, data: DataObject, min_value: float | None, max_value: float | None
    ) -> None:  # hard to reach
        with assume_does_not_raise(InvalidArgument):
            x = data.draw(
                floats_extra(
                    min_value=min_value,
                    max_value=max_value,
                    allow_nan=False,
                    allow_inf=False,
                    allow_pos_inf=False,
                    allow_neg_inf=False,
                    integral=True,
                )
            )
        assert isfinite(x)
        if min_value is not None:
            assert x >= min_value
        if max_value is not None:
            assert x <= max_value
        assert x == round(x)


class TestLiftDraw:
    @given(data=data(), x=booleans())
    def test_fixed(self, *, data: DataObject, x: bool) -> None:
        @composite
        def func(_draw: DrawFn, /) -> bool:
            _ = _draw(booleans())
            return x

        result = data.draw(func())
        assert result is x

    @given(data=data())
    def test_strategy(self, *, data: DataObject) -> None:
        @composite
        def func(_draw: DrawFn, /) -> bool:
            return _draw(booleans())

        result = data.draw(func())
        assert isinstance(result, bool)


class TestListsFixedLength:
    @given(data=data(), size=integers(1, 10))
    @mark.parametrize(
        "unique", [param(True, id="unique"), param(False, id="no unique")]
    )
    @mark.parametrize(
        "sorted_", [param(True, id="sorted"), param(False, id="no sorted")]
    )
    def test_main(
        self, *, data: DataObject, size: int, unique: bool, sorted_: bool
    ) -> None:
        result = data.draw(
            lists_fixed_length(integers(), size, unique=unique, sorted=sorted_)
        )
        assert isinstance(result, list)
        assert len(result) == size
        if unique:
            assert len(set(result)) == len(result)
        if sorted_:
            assert sorted(result) == result


class TestTempDirs:
    @given(temp_dir=temp_dirs())
    def test_main(self, *, temp_dir: TemporaryDirectory) -> None:
        path = temp_dir.path
        assert path.is_dir()
        assert len(set(path.iterdir())) == 0

    @given(temp_dir=temp_dirs(), contents=sets(text_ascii(min_size=1), max_size=10))
    def test_writing_files(
        self, *, temp_dir: TemporaryDirectory, contents: AbstractSet[str]
    ) -> None:
        path = temp_dir.path
        assert len(set(path.iterdir())) == 0
        as_set = set(maybe_yield_lower_case(contents))
        for content in as_set:
            valid_path(path, content).touch()
        assert len(set(path.iterdir())) == len(as_set)


class TestTempPaths:
    @given(path=temp_paths())
    def test_main(self, *, path: Path) -> None:
        assert path.is_dir()
        assert len(set(path.iterdir())) == 0

    @given(path=temp_paths(), contents=sets(text_ascii(min_size=1), max_size=10))
    def test_writing_files(self, *, path: Path, contents: AbstractSet[str]) -> None:
        assert len(set(path.iterdir())) == 0
        as_set = set(maybe_yield_lower_case(contents))
        for content in as_set:
            valid_path(path, content).touch()
        assert len(set(path.iterdir())) == len(as_set)


class TestTextAscii:
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
                text_ascii(
                    min_size=min_size, max_size=max_size, disallow_na=disallow_na
                )
            )
        assert search("^[A-Za-z]*$", text)
        assert len(text) >= min_size
        if max_size is not None:
            assert len(text) <= max_size
        if disallow_na:
            assert text != "NA"
