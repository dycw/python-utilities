from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from hypothesis import given
from hypothesis.strategies import binary, dictionaries, integers, lists, text
from pytest import mark, param, raises

from utilities.more_itertools import (
    ResolveIncludeAndExcludeError,
    Split,
    always_iterable,
    filter_include_and_exclude,
    peekable,
    resolve_include_and_exclude,
    windowed_complete,
    yield_splits,
)
from utilities.text import strip_and_dedent

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator


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


class TestFilterIncludeAndExclude:
    def test_none(self) -> None:
        rng = list(range(5))
        result = list(filter_include_and_exclude(rng))
        assert result == rng

    def test_include_singleton(self) -> None:
        result = list(filter_include_and_exclude(range(5), include=0))
        expected = [0]
        assert result == expected

    def test_include_iterable(self) -> None:
        result = list(filter_include_and_exclude(range(5), include=[0, 1, 2]))
        expected = [0, 1, 2]
        assert result == expected

    def test_exclude_singleton(self) -> None:
        result = list(filter_include_and_exclude(range(5), exclude=0))
        expected = [1, 2, 3, 4]
        assert result == expected

    def test_exclude_iterable(self) -> None:
        result = list(filter_include_and_exclude(range(5), exclude=[0, 1, 2]))
        expected = [3, 4]
        assert result == expected

    def test_both(self) -> None:
        result = list(
            filter_include_and_exclude(range(5), include=[0, 1], exclude=[3, 4])
        )
        expected = [0, 1]
        assert result == expected

    def test_include_key(self) -> None:
        @dataclass(frozen=True, kw_only=True)
        class Example:
            n: int

        result = list(
            filter_include_and_exclude(
                [Example(n=n) for n in range(5)], include=[0, 1, 2], key=lambda x: x.n
            )
        )
        expected = [Example(n=n) for n in [0, 1, 2]]
        assert result == expected

    def test_exclude_key(self) -> None:
        @dataclass(frozen=True, kw_only=True)
        class Example:
            n: int

        result = list(
            filter_include_and_exclude(
                [Example(n=n) for n in range(5)], exclude=[0, 1, 2], key=lambda x: x.n
            )
        )
        expected = [Example(n=n) for n in [3, 4]]
        assert result == expected


class TestPeekable:
    def test_dropwhile(self) -> None:
        it = peekable(range(10))
        it.dropwhile(lambda x: x <= 4)
        assert it.peek() == 5
        result = list(it)
        expected = [5, 6, 7, 8, 9]
        assert result == expected

    def test_next(self) -> None:
        it = peekable(range(10))
        value = next(it)
        assert isinstance(value, int)

    def test_peek_non_empty(self) -> None:
        it = peekable(range(10))
        value = it.peek()
        assert isinstance(value, int)

    def test_peek_empty_without_default(self) -> None:
        it: peekable[int] = peekable([])
        with raises(StopIteration):
            _ = it.peek()

    def test_peek_empty_with_default(self) -> None:
        it: peekable[int] = peekable([])
        value = it.peek(default="default")
        assert isinstance(value, str)

    def test_takewhile(self) -> None:
        it = peekable(range(10))
        result1 = list(it.takewhile(lambda x: x <= 4))
        expected1 = [0, 1, 2, 3, 4]
        assert result1 == expected1
        assert it.peek() == 5
        result2 = list(it)
        expected2 = [5, 6, 7, 8, 9]
        assert result2 == expected2

    def test_combined(self) -> None:
        it = peekable(range(10))
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


class TestResolveIncludeAndExclude:
    def test_none(self) -> None:
        include, exclude = resolve_include_and_exclude()
        assert include is None
        assert exclude is None

    def test_include_singleton(self) -> None:
        include, exclude = resolve_include_and_exclude(include=1)
        assert include == {1}
        assert exclude is None

    def test_include_iterable(self) -> None:
        include, exclude = resolve_include_and_exclude(include=[1, 2, 3])
        assert include == {1, 2, 3}
        assert exclude is None

    def test_exclude_singleton(self) -> None:
        include, exclude = resolve_include_and_exclude(exclude=1)
        assert include is None
        assert exclude == {1}

    def test_exclude_iterable(self) -> None:
        include, exclude = resolve_include_and_exclude(exclude=[1, 2, 3])
        assert include is None
        assert exclude == {1, 2, 3}

    def test_both(self) -> None:
        include, exclude = resolve_include_and_exclude(
            include=[1, 2, 3], exclude=[4, 5, 6]
        )
        assert include == {1, 2, 3}
        assert exclude == {4, 5, 6}

    def test_error(self) -> None:
        with raises(
            ResolveIncludeAndExcludeError,
            match="Iterables .* and .* must not overlap; got .*",
        ):
            _ = resolve_include_and_exclude(include=[1, 2, 3], exclude=[3, 4, 5])


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


class TestYieldSplits:
    @mark.parametrize(
        ("iterable", "head", "tail", "min_frac", "freq", "expected"),
        [
            param(
                "abcde",
                3,
                1,
                None,
                None,
                [
                    Split(head=["a", "b", "c"], tail=["d"]),
                    Split(head=["b", "c", "d"], tail=["e"]),
                ],
                id="3/1",
            ),
            param(
                "abcde",
                3,
                1,
                0.4,
                None,
                [
                    Split(head=["a", "b"], tail=["c"]),
                    Split(head=["a", "b", "c"], tail=["d"]),
                    Split(head=["b", "c", "d"], tail=["e"]),
                ],
                id="3/1, min-frac=0.4",
            ),
            param(
                "abcdefg",
                3,
                2,
                None,
                None,
                [
                    Split(head=["a", "b", "c"], tail=["d", "e"]),
                    Split(head=["c", "d", "e"], tail=["f", "g"]),
                ],
                id="3/2, clean tail",
            ),
            param(
                "abcdefgh",
                3,
                2,
                None,
                None,
                [
                    Split(head=["a", "b", "c"], tail=["d", "e"]),
                    Split(head=["c", "d", "e"], tail=["f", "g"]),
                    Split(head=["e", "f", "g"], tail=["h"]),
                ],
                id="3/2, truncated tail",
            ),
            param(
                "abcdefgh",
                3,
                2,
                None,
                1,
                [
                    Split(head=["a", "b", "c"], tail=["d", "e"]),
                    Split(head=["b", "c", "d"], tail=["e", "f"]),
                    Split(head=["c", "d", "e"], tail=["f", "g"]),
                    Split(head=["d", "e", "f"], tail=["g", "h"]),
                    Split(head=["e", "f", "g"], tail=["h"]),
                ],
                id="3/2, freq=1",
            ),
            param("abc", 5, 1, None, None, [], id="len(iterable) < head"),
            param("abc", 1, 5, None, None, [], id="len(iterable) < tail"),
        ],
    )
    def test_main(
        self,
        *,
        iterable: Iterable[str],
        head: int,
        tail: int,
        min_frac: float | None,
        freq: int | None,
        expected: list[Split[list[str]]],
    ) -> None:
        splits = list(yield_splits(iterable, head, tail, min_frac=min_frac, freq=freq))
        assert splits == expected

    def test_repr(self) -> None:
        split = Split(head=["a", "b", "c"], tail=["d"])
        result = repr(split)
        expected = strip_and_dedent(
            """
            Split(
                head=
                    ['a', 'b', 'c']
                tail=
                    ['d']
            )
            """
        )
        assert result == expected
