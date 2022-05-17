from typing import Union

from hypothesis import given
from hypothesis.strategies import DataObject
from hypothesis.strategies import SearchStrategy
from hypothesis.strategies import booleans
from hypothesis.strategies import data
from hypothesis.strategies import just
from pytest import raises

from dycw_utilities.hypothesis import assume_does_not_raise
from dycw_utilities.hypothesis import draw_and_flatmap
from dycw_utilities.hypothesis import draw_and_map
from dycw_utilities.hypothesis import setup_hypothesis_profiles


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
    x: Union[bool, SearchStrategy[bool]], /
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
    x: Union[bool, SearchStrategy[bool]], /
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


class TestSetupHypothesisProfiles:
    def test_main(self) -> None:
        setup_hypothesis_profiles()
