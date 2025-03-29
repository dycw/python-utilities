from __future__ import annotations

from random import Random, SystemRandom
from re import search
from typing import TYPE_CHECKING

from hypothesis import given
from hypothesis.strategies import integers, iterables, just, sampled_from

from utilities.random import (
    SYSTEM_RANDOM,
    bernoulli,
    get_docker_name,
    get_state,
    shuffle,
)

if TYPE_CHECKING:
    from collections.abc import Iterable


class TestBernoulli:
    @given(case=sampled_from([(0.0, False), (1.0, True)]))
    def test_main(self, *, case: tuple[float, bool]) -> None:
        true, expected = case
        result = bernoulli(true=true)
        assert result is expected


class TestGetDockerName:
    def test_main(self) -> None:
        name = get_docker_name()
        assert search(r"^[a-z]+_[a-z]+\d$", name)


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
