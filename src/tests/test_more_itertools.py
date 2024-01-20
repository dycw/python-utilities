from __future__ import annotations

from collections.abc import Iterator

from hypothesis import given
from hypothesis.strategies import binary, dictionaries, integers, lists, text
from pytest import raises

from utilities.more_itertools import (
    OneEmptyError,
    OneNonUniqueError,
    always_iterable,
    one,
    transpose,
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


class TestOne:
    def test_main(self) -> None:
        assert one([None]) is None

    def test_error_empty(self) -> None:
        with raises(OneEmptyError, match=r"Iterable .* must not be empty\."):
            _ = one([])

    def test_error_non_unique(self) -> None:
        with raises(
            OneNonUniqueError,
            match=r"Iterable .* must contain exactly one item; got .*, .* and perhaps more\.",
        ):
            _ = one([1, 2])


class TestTranspose:
    @given(n=integers(1, 10))
    def test_singles(self, *, n: int) -> None:
        iterable = ((i,) for i in range(n))
        result = transpose(iterable)
        assert isinstance(result, tuple)
        (first,) = result
        assert isinstance(first, tuple)
        assert len(first) == n
        for i in first:
            assert isinstance(i, int)

    @given(n=integers(1, 10))
    def test_pairs(self, *, n: int) -> None:
        iterable = ((i, i) for i in range(n))
        result = transpose(iterable)
        assert isinstance(result, tuple)
        first, second = result
        for part in [first, second]:
            assert len(part) == n
            for i in part:
                assert isinstance(i, int)

    @given(n=integers(1, 10))
    def test_triples(self, *, n: int) -> None:
        iterable = ((i, i, i) for i in range(n))
        result = transpose(iterable)
        assert isinstance(result, tuple)
        first, second, third = result
        for part in [first, second, third]:
            assert len(part) == n
            for i in part:
                assert isinstance(i, int)

    @given(n=integers(1, 10))
    def test_quadruples(self, *, n: int) -> None:
        iterable = ((i, i, i, i) for i in range(n))
        result = transpose(iterable)
        assert isinstance(result, tuple)
        first, second, third, fourth = result
        for part in [first, second, third, fourth]:
            assert len(part) == n
            for i in part:
                assert isinstance(i, int)

    @given(n=integers(1, 10))
    def test_quintuples(self, *, n: int) -> None:
        iterable = ((i, i, i, i, i) for i in range(n))
        result = transpose(iterable)
        assert isinstance(result, tuple)
        first, second, third, fourth, fifth = result
        for part in [first, second, third, fourth, fifth]:
            assert len(part) == n
            for i in part:
                assert isinstance(i, int)
