from __future__ import annotations

from itertools import starmap
from operator import neg, sub

from hypothesis import given
from hypothesis.strategies import integers, lists, sampled_from, tuples

from utilities.concurrent import Parallelism, concurrent_map, concurrent_starmap
from utilities.hypothesis import int64s, settings_with_reduced_examples
from utilities.typing import get_args


class TestConcurrentMap:
    @given(
        xs=lists(int64s(), max_size=10),
        parallelism=sampled_from(get_args(Parallelism)),
        max_workers=integers(1, 2),
    )
    @settings_with_reduced_examples()
    def test_unary(
        self, *, xs: list[int], parallelism: Parallelism, max_workers: int
    ) -> None:
        result = concurrent_map(
            neg, xs, parallelism=parallelism, max_workers=max_workers
        )
        expected = list(map(neg, xs))
        assert result == expected

    @given(
        xs=lists(int64s(), max_size=10),
        ys=lists(int64s(), max_size=10),
        parallelism=sampled_from(get_args(Parallelism)),
        max_workers=integers(1, 2),
    )
    @settings_with_reduced_examples()
    def test_binary(
        self,
        *,
        xs: list[int],
        ys: list[int],
        parallelism: Parallelism,
        max_workers: int,
    ) -> None:
        result = concurrent_map(
            pow, xs, ys, parallelism=parallelism, max_workers=max_workers
        )
        expected = [x - y for x, y in zip(xs, ys, strict=False)]
        assert result == expected


class TestPStarMap:
    @given(
        iterable=lists(tuples(int64s()), max_size=10),
        parallelism=sampled_from(get_args(Parallelism)),
        max_workers=integers(1, 2),
    )
    @settings_with_reduced_examples()
    def test_unary(
        self, *, iterable: list[tuple[int]], parallelism: Parallelism, max_workers: int
    ) -> None:
        result = concurrent_starmap(
            neg, iterable, parallelism=parallelism, max_workers=max_workers
        )
        expected = [-1, -2, -3]
        assert result == expected

    @given(
        iterable=lists(tuples(int64s(), int64s()), max_size=10),
        parallelism=sampled_from(get_args(Parallelism)),
        max_workers=integers(1, 2),
    )
    @settings_with_reduced_examples()
    def test_binary(
        self,
        *,
        iterable: list[tuple[int, int]],
        parallelism: Parallelism,
        max_workers: int,
    ) -> None:
        result = concurrent_starmap(
            sub, iterable, parallelism=parallelism, max_workers=max_workers
        )
        expected = list(starmap(sub, iterable))
        assert result == expected
