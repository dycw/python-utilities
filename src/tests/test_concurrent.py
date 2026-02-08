from __future__ import annotations

from functools import partial
from itertools import starmap
from operator import neg, sub

from hypothesis import given
from hypothesis.strategies import integers, lists, sampled_from, tuples

from utilities.concurrent import concurrent_apply, concurrent_map, concurrent_starmap
from utilities.core import transpose
from utilities.hypothesis import int32s, pairs, settings_with_reduced_examples
from utilities.types import Parallelism
from utilities.typing import get_args


class TestConcurrentApply:
    def test_main(self) -> None:
        values: set[int] = set()

        def func(n: int) -> None:
            values.add(n)

        funcs = [partial(func, n) for n in range(10)]
        concurrent_apply(*funcs, parallelism="threads", max_workers=2)
        assert values == set(range(10))


class TestConcurrentMap:
    @given(xs=lists(int32s(), max_size=10))
    @settings_with_reduced_examples()
    def test_unary(self, *, xs: list[int]) -> None:
        result = concurrent_map(neg, xs, parallelism="threads", max_workers=2)
        expected = list(map(neg, xs))
        assert result == expected

    @given(iterable=lists(pairs(int32s()), min_size=1, max_size=10))
    @settings_with_reduced_examples()
    def test_binary(self, *, iterable: list[tuple[int, int]]) -> None:
        xs, ys = transpose(iterable)
        result = concurrent_map(sub, xs, ys, parallelism="threads", max_workers=2)
        expected = list(starmap(sub, iterable))
        assert result == expected


class TestConcurrentStarMap:
    @given(
        iterable=lists(tuples(int32s()), max_size=10),
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
        expected = list(starmap(neg, iterable))
        assert result == expected

    @given(
        iterable=lists(pairs(int32s()), max_size=10),
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
