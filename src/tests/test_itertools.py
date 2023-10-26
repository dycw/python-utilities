from __future__ import annotations

from collections.abc import Iterable, Sequence
from itertools import chain
from typing import Any

from hypothesis import given
from hypothesis.strategies import (
    DataObject,
    data,
    integers,
    lists,
    sampled_from,
    sets,
)
from pytest import mark, param, raises

from utilities.itertools import (
    EmptyIterableError,
    IterableContainsDuplicatesError,
    MultipleElementsError,
    check_duplicates,
    chunked,
    is_iterable_not_str,
    one,
    take,
)


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
    def test_main(self, *, iterable: list[int], expected: list[list[int]]) -> None:
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
    @mark.parametrize(("n", "iterable"), [param(3, range(10)), param(10, range(3))])
    def test_main(self, *, n: int, iterable: Iterable[int]) -> None:
        result = take(n, iterable)
        assert result == [0, 1, 2]
