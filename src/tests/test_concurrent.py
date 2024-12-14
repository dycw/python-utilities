from __future__ import annotations

from operator import neg

from pytest import mark, param

from utilities.concurrent import _PARALLELISM, concurrent_map
from utilities.typing import get_args


class TestConcurrentMap:
    @mark.parametrize("parallelism", list(get_args(_PARALLELISM)))
    @mark.parametrize("max_workers", [param(1), param(2)])
    def test_unary(self, *, parallelism: _PARALLELISM, max_workers: int) -> None:
        result = concurrent_map(
            neg, [1, 2, 3], parallelism=parallelism, max_workers=max_workers
        )
        expected = [-1, -2, -3]
        assert result == expected

    @mark.parametrize("parallelism", list(get_args(_PARALLELISM)))
    @mark.parametrize("max_workers", [param(1), param(2)])
    def test_binary(self, *, parallelism: _PARALLELISM, max_workers: int) -> None:
        result = concurrent_map(
            pow, [2, 3, 10], [5, 2, 3], parallelism=parallelism, max_workers=max_workers
        )
        expected = [32, 9, 1000]
        assert result == expected
