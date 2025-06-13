from __future__ import annotations

from typing import TYPE_CHECKING

from hypothesis import example, given
from hypothesis.strategies import integers, none

from utilities.asyncio import sleep_td
from utilities.cachetools import TTLSet, cache
from utilities.hypothesis import time_deltas
from utilities.whenever import SECOND

if TYPE_CHECKING:
    from whenever import TimeDelta


class TestCache:
    @example(max_size=None, max_duration=None)
    @example(max_size=None, max_duration=SECOND)
    @example(max_size=1, max_duration=None)
    @example(max_size=1, max_duration=SECOND)
    @given(
        max_size=integers(1, 10) | none(),
        max_duration=time_deltas(min_value=0.1 * SECOND, max_value=10.0 * SECOND)
        | none(),
    )
    def test_main(self, *, max_size: int, max_duration: TimeDelta) -> None:
        counter = 0

        @cache(max_size=max_size, max_duration=max_duration)
        def func(x: int, /) -> int:
            nonlocal counter
            counter += 1
            return x

        for _ in range(2):
            assert func(0) == 0


class TestTTLSet:
    def test_contains(self) -> None:
        set_ = TTLSet(range(3))
        assert 0 in set_
        assert 3 not in set_

    def test_discard(self) -> None:
        set_ = TTLSet(range(3))
        set_.discard(1)
        assert set_ == {0, 2}

    def test_init_with_iterable(self) -> None:
        _ = TTLSet(range(3))

    def test_init_without_iterable(self) -> None:
        _ = TTLSet()

    def test_len(self) -> None:
        set_ = TTLSet(range(3))
        assert len(set_) == 3

    async def test_max_duration(self) -> None:
        delta = 0.1 * SECOND
        set_ = TTLSet(range(3), max_duration=delta)
        assert set_ == {0, 1, 2}
        await sleep_td(2 * delta)
        assert set_ == set()

    def test_max_size(self) -> None:
        set_ = TTLSet(max_size=3)
        set_.add(1)
        assert set_ == {1}
        set_.add(2)
        assert set_ == {1, 2}
        set_.add(3)
        assert set_ == {1, 2, 3}
        set_.add(4)
        assert set_ == {2, 3, 4}
        set_.add(5)
        assert set_ == {3, 4, 5}

    def test_repr_and_str(self) -> None:
        set_ = TTLSet([None])
        expected = "{None}"
        assert repr(set_) == expected
        assert str(set_) == expected
