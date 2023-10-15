from __future__ import annotations

from collections.abc import Iterable
from collections.abc import Sequence
from itertools import chain
from typing import Any

from hypothesis import given
from hypothesis.strategies import DataObject
from hypothesis.strategies import data
from hypothesis.strategies import integers
from hypothesis.strategies import lists
from hypothesis.strategies import sampled_from
from hypothesis.strategies import sets
from pytest import mark
from pytest import param
from pytest import raises

from utilities.itertools import EmptyIterableError
from utilities.itertools import IterableContainsDuplicatesError
from utilities.itertools import MultipleElementsError
from utilities.itertools import check_duplicates
from utilities.itertools import chunked
from utilities.itertools import is_iterable_not_str
from utilities.itertools import one
from utilities.itertools import take


class TestCheckDuplicates:
    @given(x=sets(integers()))
    def test_main(self, *, x: set[int]) -> None:
        check_duplicates(x)

    @given(data=data(), x=lists(integers(), min_size=1))
    def test_error(self, *, data: DataObject, x: Sequence[int]) -> None:
        x_i = data.draw(sampled_from(x))
        y = chain(x, [x_i])
        with raises(IterableContainsDuplicatesError):
            check_duplicates(y)


class TestChunked:
    @mark.parametrize(
        ("iterable", "expected"),
        [
            param([1, 2, 3, 4, 5, 6], [[1, 2, 3], [4, 5, 6]]),
            param([1, 2, 3, 4, 5, 6, 7, 8], [[1, 2, 3], [4, 5, 6], [7, 8]]),
        ],
    )
    def test_main(
        self, *, iterable: list[int], expected: list[list[int]]
    ) -> None:
        result = list(chunked(iterable, n=3))
        assert result == expected


class TestIsIterableNotStr:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(None, False),
            param([], True),
            param((), True),
            param("", False),
        ],
    )
    def test_main(self, *, x: Any, expected: bool) -> None:
        assert is_iterable_not_str(x) is expected


class TestOne:
    def test_empty(self) -> None:
        with raises(EmptyIterableError):
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


class TestTake:
    @mark.parametrize(
        ("n", "iterable"), [param(3, range(10)), param(10, range(3))]
    )
    def test_main(self, *, n: int, iterable: Iterable[int]) -> None:
        result = take(n, iterable)
        assert result == [0, 1, 2]
