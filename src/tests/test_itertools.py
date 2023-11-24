from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from itertools import chain
from typing import Any

from hypothesis import given
from hypothesis.strategies import (
    DataObject,
    binary,
    data,
    dictionaries,
    integers,
    lists,
    sampled_from,
    sets,
    text,
)
from pytest import mark, param, raises

from utilities.itertools import (
    EmptyIterableError,
    IterableContainsDuplicatesError,
    MultipleElementsError,
    always_iterable,
    check_duplicates,
    chunked,
    is_iterable_not_str,
    is_sized_not_str,
    one,
    take,
)


class TestAlwaysIterable:
    @given(x=binary())
    def test_bytes(self, *, x: bytes) -> None:
        assert list(always_iterable(x)) == [x]
        assert list(always_iterable(x, base_type=None)) == list(x)
        assert list(always_iterable(x, base_type=bytes)) == [x]
        assert list(always_iterable(x, base_type=(bytes,))) == [x]

    @given(x=integers())
    def test_integer(self, *, x: int) -> None:
        assert list(always_iterable(x)) == [x]
        assert list(always_iterable(x, base_type=None)) == [x]
        assert list(always_iterable(x, base_type=int)) == [x]
        assert list(always_iterable(x, base_type=(int,))) == [x]

    @given(x=text())
    def test_string(self, *, x: str) -> None:
        assert list(always_iterable(x)) == [x]
        assert list(always_iterable(x, base_type=None)) == list(x)
        assert list(always_iterable(x, base_type=str)) == [x]
        assert list(always_iterable(x, base_type=(str,))) == [x]

    @given(x=dictionaries(text(), integers()))
    def test_dict(self, *, x: dict[str, int]) -> None:
        assert list(always_iterable(x)) == list(x)
        assert list(always_iterable(x, base_type=dict)) == [x]
        assert list(always_iterable(x, base_type=(dict,))) == [x]

    @given(x=lists(integers()))
    def test_lists(self, *, x: list[int]) -> None:
        assert list(always_iterable(x)) == x
        assert list(always_iterable(x, base_type=None)) == x
        assert list(always_iterable(x, base_type=list)) == [x]
        assert list(always_iterable(x, base_type=(list,))) == [x]

    def test_none(self) -> None:
        assert list(always_iterable(None)) == []

    def test_generator(self) -> None:
        def yield_ints() -> Iterator[int]:
            yield 0
            yield 1

        assert list(always_iterable(yield_ints())) == [0, 1]


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
        ("obj", "expected"),
        [param(None, False), param([], True), param((), True), param("", False)],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        assert is_iterable_not_str(obj) is expected


class TestIsSizedNotStr:
    @mark.parametrize(
        ("obj", "expected"),
        [param(None, False), param([], True), param((), True), param("", False)],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        assert is_sized_not_str(obj) is expected


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
