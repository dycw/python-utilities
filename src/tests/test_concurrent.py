from __future__ import annotations

from operator import neg

from hypothesis import given
from hypothesis.strategies import integers, lists, sampled_from, tuples

from utilities.concurrent import _Parallelism, concurrent_map, concurrent_starmap
from utilities.hypothesis import settings_with_reduced_examples
from utilities.typing import get_args


class TestConcurrentMap:
    @given(
        xs=lists(integers(), max_size=10),
        parallelism=sampled_from(get_args(_Parallelism)),
        max_workers=integers(1, 2),
    )
    @settings_with_reduced_examples()
    def test_unary(
        self, *, xs: list[int], parallelism: _Parallelism, max_workers: int
    ) -> None:
        result = concurrent_map(
            neg, xs, parallelism=parallelism, max_workers=max_workers
        )
        expected = list(map(neg, xs))
        assert result == expected

    @given(
        xs=lists(integers(), max_size=10),
        ys=lists(integers(), max_size=10),
        parallelism=sampled_from(get_args(_Parallelism)),
        max_workers=integers(1, 2),
    )
    @settings_with_reduced_examples()
    def test_binary(
        self,
        *,
        xs: list[int],
        ys: list[int],
        parallelism: _Parallelism,
        max_workers: int,
    ) -> None:
        result = concurrent_map(
            pow, xs, ys, parallelism=parallelism, max_workers=max_workers
        )
        expected = [x - y for x, y in zip(xs, ys, strict=True)]
        assert result == expected


class TestPStarMap:
    @given(
        xs=lists(tuples(integers()), max_size=10),
        parallelism=sampled_from(get_args(_Parallelism)),
        n_jobs=integers(1, 3),
    )
    @settings_with_reduced_examples()
    def test_unary(
        self, *, xs: list[tuple[int]], parallelism: _Parallelism, n_jobs: int
    ) -> None:
        result = concurrent_starmap(neg, xs, parallelism=parallelism, n_jobs=n_jobs)
        expected = [-1, -2, -3]
        assert result == expected

    @given(parallelism=sampled_from(get_args(_Parallelism)), n_jobs=integers(1, 3))
    @settings_with_reduced_examples()
    def test_binary(self, *, parallelism: _Parallelism, n_jobs: int) -> None:
        result = pstarmap(
            pow, [(2, 5), (3, 2), (10, 3)], parallelism=parallelism, n_jobs=n_jobs
        )
        expected = [32, 9, 1000]
        assert result == expected
