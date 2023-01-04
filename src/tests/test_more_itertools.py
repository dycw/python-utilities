from re import escape

from pytest import raises

from utilities.more_itertools import EmptyIterableError
from utilities.more_itertools import MultipleElementsError
from utilities.more_itertools import one


class TestOne:
    def test_empty(self) -> None:
        with raises(
            EmptyIterableError,
            match=escape("too few items in iterable (expected 1)"),
        ):
            _ = one([])

    def test_one(self) -> None:
        assert one([None]) is None

    def test_multiple(self) -> None:
        with raises(
            MultipleElementsError,
            match="Expected exactly one item in iterable, but got 1, 2, and "
            "perhaps more",
        ):
            _ = one([1, 2, 3])
