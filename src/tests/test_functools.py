from __future__ import annotations

from operator import sub

from hypothesis import given
from hypothesis.strategies import booleans, integers

from utilities.functools import cache, lru_cache, partial


class TestCache:
    def test_main(self) -> None:
        counter = 0

        @cache
        def func(x: int, /) -> int:
            nonlocal counter
            counter += 1
            return x

        for _ in range(2):
            assert func(0) == 0
        assert counter == 1


class TestLRUCache:
    def test_no_arguments(self) -> None:
        counter = 0

        @lru_cache
        def func(x: int, /) -> int:
            nonlocal counter
            counter += 1
            return x

        for _ in range(2):
            assert func(0) == 0
        assert counter == 1

    @given(max_size=integers(1, 10), typed=booleans())
    def test_with_arguments(self, *, max_size: int, typed: bool) -> None:
        counter = 0

        @lru_cache(max_size=max_size, typed=typed)
        def func(x: int, /) -> int:
            nonlocal counter
            counter += 1
            return x

        for _ in range(2):
            assert func(0) == 0
        assert counter == 1


class TestPartial:
    @given(x=integers(), y=integers())
    def test_main(self, *, x: int, y: int) -> None:
        func = partial(sub, ..., y)
        assert func(x) == x - y
