from __future__ import annotations

from hypothesis import example, given
from hypothesis.strategies import floats, integers, none

from utilities.cachetools import cache


class TestCache:
    @example(max_size=None, max_duration=None)
    @example(max_size=None, max_duration=1.0)
    @example(max_size=1, max_duration=None)
    @example(max_size=1, max_duration=1.0)
    @given(max_size=integers(1, 10) | none(), max_duration=floats(0.1, 10.0) | none())
    def test_main(self, *, max_size: int, max_duration: float) -> None:
        counter = 0

        @cache(max_size=max_size, max_duration=max_duration)
        def func(x: int, /) -> int:
            nonlocal counter
            counter += 1
            return x

        for _ in range(2):
            assert func(0) == 0
