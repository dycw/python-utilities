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

    def test_deterministic(self) -> None:
        expected = [
            "priceless_hypatia8",
            "quizzical_aryabhata6",
            "priceless_wing2",
            "dazzling_mclean2",
            "epic_montalcini8",
        ]
        for exp in expected:
            result = get_docker_name(TestGetDockerName.test_deterministic.__qualname__)
            assert result == exp


class TestGetState:
    @given(seed=integers() | just(SYSTEM_RANDOM))
    def test_main(self, *, seed: int | SystemRandom) -> None:
        state = get_state(seed)
        assert isinstance(state, Random)

    def test_deterministic(self) -> None:
        expected = [6, 4, 8, 10, 6]
        for exp in expected:
            result = get_state(TestGetState.test_deterministic.__qualname__).randint(
                0, 10
            )
            assert result == exp


class TestShuffle:
    @given(iterable=iterables(integers()), seed=integers())
    def test_main(self, *, iterable: Iterable[int], seed: int) -> None:
        as_set = set(iterable)
        result = shuffle(as_set, seed=seed)
        assert set(result) == as_set

    def test_deterministic(self) -> None:
        expected = [
            [2, 3, 0, 4, 1],
            [3, 1, 2, 0, 4],
            [1, 0, 4, 3, 2],
            [1, 0, 2, 3, 4],
            [4, 0, 3, 1, 2],
        ]
        for exp in expected:
            result = shuffle(range(5), seed=TestShuffle.test_deterministic.__qualname__)
            assert result == exp


class TestSystemRandom:
    def test_main(self) -> None:
        assert isinstance(SYSTEM_RANDOM, SystemRandom)
