from __future__ import annotations

from random import Random, SystemRandom
from typing import TYPE_CHECKING

from hypothesis import given
from hypothesis.strategies import integers, iterables, just

from utilities.random import SYSTEM_RANDOM, get_state, shuffle

if TYPE_CHECKING:
    from collections.abc import Iterable


class TestGetState:
    @given(seed=integers() | just(SYSTEM_RANDOM))
    def test_main(self, *, seed: int | SystemRandom) -> None:
        state = get_state(seed=seed)
        assert isinstance(state, Random)


class TestShuffle:
    @given(iterable=iterables(integers()), seed=integers())
    def test_main(self, *, iterable: Iterable[int], seed: int) -> None:
        as_set = set(iterable)
        result = shuffle(as_set, seed=seed)
        assert set(result) == as_set
        result2 = shuffle(as_set, seed=seed)
        assert result == result2


class TestSystemRandom:
    def test_main(self) -> None:
        assert isinstance(SYSTEM_RANDOM, SystemRandom)
