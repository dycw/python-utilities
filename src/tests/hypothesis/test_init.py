from pathlib import Path
from re import search
from typing import Optional

from hypothesis import given
from hypothesis.errors import InvalidArgument
from hypothesis.strategies import (
    DataObject,
    DrawFn,
    booleans,
    composite,
    data,
    integers,
    just,
    none,
    sets,
)
from pytest import mark, param, raises

from utilities.hypothesis import (
    assume_does_not_raise,
    lists_fixed_length,
    setup_hypothesis_profiles,
    temp_dirs,
    temp_paths,
    text_ascii,
    text_clean,
    text_printable,
)
from utilities.tempfile import TemporaryDirectory


class TestAssumeDoesNotRaise:
    @given(x=booleans())
    def test_no_match_and_suppressed(self, x: bool) -> None:
        with assume_does_not_raise(ValueError):
            if x is True:
                msg = "x is True"
                raise ValueError(msg)
        assert x is False

    @given(x=booleans())
    def test_no_match_and_not_suppressed(self, x: bool) -> None:
        msg = "x is True"
        if x is True:
            with raises(ValueError, match=msg), assume_does_not_raise(
                RuntimeError,
            ):
                raise ValueError(msg)

    @given(x=booleans())
    def test_with_match_and_suppressed(self, x: bool) -> None:
        msg = "x is True"
        if x is True:
            with assume_does_not_raise(ValueError, match=msg):
                raise ValueError(msg)
        assert x is False

    @given(x=just(True))
    def test_with_match_and_not_suppressed(self, x: bool) -> None:
        msg = "x is True"
        if x is True:
            with raises(ValueError, match=msg), assume_does_not_raise(
                ValueError,
                match="wrong",
            ):
                raise ValueError(msg)


class TestLiftDraw:
    @given(data=data(), x=booleans())
    def test_fixed(self, data: DataObject, x: bool) -> None:
        @composite
        def func(_draw: DrawFn, /) -> bool:
            _ = _draw
            return x

        result = data.draw(func())
        assert result is x

    @given(data=data())
    def test_strategy(self, data: DataObject) -> None:
        @composite
        def func(_draw: DrawFn, /) -> bool:
            return _draw(booleans())

        result = data.draw(func())
        assert isinstance(result, bool)


class TestListsFixedLength:
    @given(data=data(), size=integers(1, 10))
    @mark.parametrize(
        "unique",
        [param(True, id="unique"), param(False, id="no unique")],
    )
    @mark.parametrize(
        "sorted_",
        [param(True, id="sorted"), param(False, id="no sorted")],
    )
    def test_main(
        self,
        data: DataObject,
        size: int,
        unique: bool,
        sorted_: bool,
    ) -> None:
        result = data.draw(
            lists_fixed_length(integers(), size, unique=unique, sorted=sorted_),
        )
        assert isinstance(result, list)
        assert len(result) == size
        if unique:
            assert len(set(result)) == len(result)
        if sorted_:
            assert sorted(result) == result


class TestSetupHypothesisProfiles:
    def test_main(self) -> None:
        setup_hypothesis_profiles()


class TestTempDirs:
    @given(temp_dir=temp_dirs())
    def test_main(self, temp_dir: TemporaryDirectory) -> None:
        _test_temp_path(temp_dir.name)

    @given(
        temp_dir=temp_dirs(),
        contents=sets(text_ascii(min_size=1), max_size=10),
    )
    def test_writing_files(
        self,
        temp_dir: TemporaryDirectory,
        contents: set[str],
    ) -> None:
        _test_writing_to_temp_path(temp_dir.name, contents)


class TestTempPaths:
    @given(temp_path=temp_paths())
    def test_main(self, temp_path: Path) -> None:
        _test_temp_path(temp_path)

    @given(
        temp_path=temp_paths(),
        contents=sets(text_ascii(min_size=1), max_size=10),
    )
    def test_writing_files(self, temp_path: Path, contents: set[str]) -> None:
        _test_writing_to_temp_path(temp_path, contents)


def _test_temp_path(path: Path, /) -> None:
    assert path.is_dir()
    assert len(set(path.iterdir())) == 0


def _test_writing_to_temp_path(path: Path, contents: set[str], /) -> None:
    assert len(set(path.iterdir())) == 0
    for content in contents:
        path.joinpath(content).touch()
    assert len(set(path.iterdir())) == len(contents)


class TestTextAscii:
    @given(
        data=data(),
        min_size=integers(0, 100),
        max_size=integers(0, 100) | none(),
    )
    def test_main(
        self,
        data: DataObject,
        min_size: int,
        max_size: Optional[int],
    ) -> None:
        with assume_does_not_raise(InvalidArgument, AssertionError):
            text = data.draw(text_ascii(min_size=min_size, max_size=max_size))
        assert search("^[A-Za-z]*$", text)
        assert len(text) >= min_size
        if max_size is not None:
            assert len(text) <= max_size


class TestTextClean:
    @given(
        data=data(),
        min_size=integers(0, 100),
        max_size=integers(0, 100) | none(),
    )
    def test_main(
        self,
        data: DataObject,
        min_size: int,
        max_size: Optional[int],
    ) -> None:
        with assume_does_not_raise(InvalidArgument, AssertionError):
            text = data.draw(text_clean(min_size=min_size, max_size=max_size))
        assert search("^\\S[^\\r\\n]*$|^$", text)
        assert len(text) >= min_size
        if max_size is not None:
            assert len(text) <= max_size


class TestTextPrintable:
    @given(
        data=data(),
        min_size=integers(0, 100),
        max_size=integers(0, 100) | none(),
    )
    def test_main(
        self,
        data: DataObject,
        min_size: int,
        max_size: Optional[int],
    ) -> None:
        with assume_does_not_raise(InvalidArgument, AssertionError):
            text = data.draw(
                text_printable(min_size=min_size, max_size=max_size),
            )
        assert search(
            r"^[0-9A-Za-z!\"#$%&'()*+,-./:;<=>?@\[\\\]^_`{|}~\s]*$",
            text,
        )
        assert len(text) >= min_size
        if max_size is not None:
            assert len(text) <= max_size
