from hypothesis import given
from hypothesis.strategies import booleans
from hypothesis.strategies import just
from pytest import raises

from dycw_utilities.hypothesis import assume_does_not_raise
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


class TestSetupHypothesisProfiles:
    def test_main(self) -> None:
        setup_hypothesis_profiles()
