from __future__ import annotations

from functools import partial
from operator import neg, sub

from pytest import mark, param

from utilities.concurrent import concurrent_apply, concurrent_map, concurrent_starmap
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
    def test_unary(self) -> None:
        xs = range(10)
        result = concurrent_map(neg, xs, parallelism="threads", max_workers=2)
        expected = list(map(neg, xs))
        assert result == expected

    def test_binary(self) -> None:
        xs = range(10)
        ys = range(0, 20, 2)
        result = concurrent_map(sub, xs, ys, parallelism="threads", max_workers=2)
        expected = [x - y for x, y in zip(xs, ys, strict=True)]
        assert result == expected

    def test_empty(self) -> None:
        result = concurrent_map(neg, [], parallelism="threads", max_workers=2)
        assert result == []


class TestConcurrentStarMap:
    @mark.parametrize("parallelism", list(get_args(Parallelism)))
    @mark.parametrize("max_workers", [param(1), param(2)])
    def test_unary(self, *, parallelism: Parallelism, max_workers: int) -> None:
        iterable = [(x,) for x in range(10)]
        result = concurrent_starmap(
            neg, iterable, parallelism=parallelism, max_workers=max_workers
        )
        expected = [-x for x in range(10)]
        assert result == expected

    @mark.parametrize("parallelism", list(get_args(Parallelism)))
    @mark.parametrize("max_workers", [param(1), param(2)])
    def test_binary(self, *, parallelism: Parallelism, max_workers: int) -> None:
        iterable = [(x, 2 * x) for x in range(10)]
        result = concurrent_starmap(
            sub, iterable, parallelism=parallelism, max_workers=max_workers
        )
        expected = [-x for x in range(10)]
        assert result == expected
