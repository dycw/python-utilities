import builtins
from re import search

from hypothesis import given
from hypothesis.errors import InvalidArgument
from hypothesis.strategies import DataObject
from hypothesis.strategies import SearchStrategy
from hypothesis.strategies import booleans
from hypothesis.strategies import data
from hypothesis.strategies import integers
from hypothesis.strategies import just
from hypothesis.strategies import none
from pytest import mark
from pytest import param
from pytest import raises

from utilities.hypothesis import assume_does_not_raise
from utilities.hypothesis import draw_and_flatmap
from utilities.hypothesis import draw_and_map
from utilities.hypothesis import lists_fixed_length
from utilities.hypothesis import setup_hypothesis_profiles
from utilities.hypothesis import text_ascii
from utilities.hypothesis import text_clean


class TestAssumeDoesNotRaise:
    @given(x=booleans())
    def test_no_match_and_suppressed(self, x: bool) -> None:
        with assume_does_not_raise(ValueError):
            if x is True:
                raise ValueError("x is True")
        assert x is False

    @given(x=just(True))
    def test_no_match_and_not_suppressed(self, x: bool) -> None:
        with raises(ValueError, match="x is True"), assume_does_not_raise(
            RuntimeError
        ):
            if x is True:
                raise ValueError("x is True")

    @given(x=booleans())
    def test_with_match_and_suppressed(self, x: bool) -> None:
        with assume_does_not_raise(ValueError, match="x is True"):
            if x is True:
                raise ValueError("x is True")
        assert x is False

    @given(x=just(True))
    def test_with_match_and_not_suppressed(self, x: bool) -> None:
        with raises(ValueError, match="x is True"), assume_does_not_raise(
            ValueError, match="wrong"
        ):
            if x is True:
                raise ValueError("x is True")


def uses_draw_and_map(
    x: bool | SearchStrategy[bool], /
) -> SearchStrategy[bool]:
    def inner(x: bool, /) -> bool:
        return x

    return draw_and_map(inner, x)


class TestDrawAndMap:
    @given(data=data(), x=booleans())
    def test_fixed(self, data: DataObject, x: bool) -> None:
        result = data.draw(uses_draw_and_map(x))
        assert result is x

    @given(x=uses_draw_and_map(booleans()))
    def test_strategy(self, x: bool) -> None:
        assert isinstance(x, bool)


def uses_draw_and_flatmap(
    x: bool | SearchStrategy[bool], /
) -> SearchStrategy[bool]:
    def inner(x: bool, /) -> SearchStrategy[bool]:
        return just(x)

    return draw_and_flatmap(inner, x)


class TestDrawAndFlatMap:
    @given(data=data(), x=booleans())
    def test_fixed(self, data: DataObject, x: bool) -> None:
        result = data.draw(uses_draw_and_flatmap(x))
        assert result is x

    @given(x=uses_draw_and_flatmap(booleans()))
    def test_strategy(self, x: bool) -> None:
        assert isinstance(x, bool)


class TestListsFixedLength:
    @given(data=data(), size=integers(1, 10))
    @mark.parametrize(
        "unique", [param(True, id="unique"), param(False, id="no unique")]
    )
    @mark.parametrize(
        "sorted", [param(True, id="sorted"), param(False, id="no sorted")]
    )
    def test_main(
        self, data: DataObject, size: int, unique: bool, sorted: bool
    ) -> None:
        result = data.draw(
            lists_fixed_length(integers(), size, unique=unique, sorted=sorted)
        )
        assert isinstance(result, list)
        assert len(result) == size
        if unique:
            assert len(set(result)) == len(result)
        if sorted:
            assert builtins.sorted(result) == result


class TestSetupHypothesisProfiles:
    def test_main(self) -> None:
        setup_hypothesis_profiles()


class TestTextAscii:
    @given(
        data=data(),
        min_size=integers(0, 100),
        max_size=integers(0, 100) | none(),
    )
    def test_main(
        self, data: DataObject, min_size: int, max_size: int | None
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
        self, data: DataObject, min_size: int, max_size: int | None
    ) -> None:
        with assume_does_not_raise(InvalidArgument, AssertionError):
            text = data.draw(text_clean(min_size=min_size, max_size=max_size))
        assert search("^\\S[^\\r\\n]*$|^$", text)
        assert len(text) >= min_size
        if max_size is not None:
            assert len(text) <= max_size
