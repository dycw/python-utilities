from __future__ import annotations

from typing import TYPE_CHECKING

from hypothesis import given
from hypothesis.strategies import binary, dictionaries, integers, lists, text

from utilities.more_itertools import always_iterable, peekable, windowed_complete

if TYPE_CHECKING:
    from collections.abc import Iterator


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


class TestPeekable:
    def test_dropwhile(self) -> None:
        it: peekable[int] = peekable(range(10))
        it.dropwhile(lambda x: x <= 4)
        assert it.peek() == 5
        result = list(it)
        expected = [5, 6, 7, 8, 9]
        assert result == expected

    def test_takewhile(self) -> None:
        it: peekable[int] = peekable(range(10))
        result1 = list(it.takewhile(lambda x: x <= 4))
        expected1 = [0, 1, 2, 3, 4]
        assert result1 == expected1
        assert it.peek() == 5
        result2 = list(it)
        expected2 = [5, 6, 7, 8, 9]
        assert result2 == expected2

    def test_combined(self) -> None:
        it: peekable[int] = peekable(range(10))
        result1 = list(it.takewhile(lambda x: x <= 2))
        expected1 = [0, 1, 2]
        assert result1 == expected1
        assert it.peek() == 3
        it.dropwhile(lambda x: x <= 4)
        assert it.peek() == 5
        result2 = list(it.takewhile(lambda x: x <= 6))
        expected2 = [5, 6]
        assert result2 == expected2
        result3 = list(it)
        expected3 = [7, 8, 9]
        assert result3 == expected3


class TestWindowedComplete:
    def test_main(self) -> None:
        result = list(windowed_complete([1, 2, 3, 4, 5], 3))
        expected = [
            ((), (1, 2, 3), (4, 5)),
            ((1,), (2, 3, 4), (5,)),
            ((1, 2), (3, 4, 5), ()),
        ]
        assert result == expected

    def test_zero_length(self) -> None:
        result = list(windowed_complete([1, 2, 3], 0))
        expected = [
            ((), (), (1, 2, 3)),
            ((1,), (), (2, 3)),
            ((1, 2), (), (3,)),
            ((1, 2, 3), (), ()),
        ]
        assert result == expected
