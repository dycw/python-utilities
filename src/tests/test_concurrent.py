from __future__ import annotations

from operator import neg

from hypothesis import given
from hypothesis.strategies import integers, sampled_from

from utilities.concurrent import _Parallelism, concurrent_map
from utilities.typing import get_args


class TestConcurrentMap:
    @given(parallelism=sampled_from(get_args(_Parallelism)), max_workers=integers(1, 3))
    def test_unary(self, *, parallelism: _Parallelism, max_workers: int) -> None:
        result = concurrent_map(
            neg, [1, 2, 3], parallelism=parallelism, max_workers=max_workers
        )
        expected = [-1, -2, -3]
        assert result == expected

    @given(parallelism=sampled_from(get_args(_Parallelism)), max_workers=integers(1, 3))
    def test_binary(self, *, parallelism: _Parallelism, max_workers: int) -> None:
        result = concurrent_map(
            pow, [2, 3, 10], [5, 2, 3], parallelism=parallelism, max_workers=max_workers
        )
        expected = [32, 9, 1000]
        assert result == expected
