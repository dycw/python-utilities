from functools import partial
from operator import neg, pow
from typing import Any, Callable, Literal, Optional, Union

from pytest import mark, param

from utilities.pqdm import _get_desc, pmap, pstarmap
from utilities.sentinel import Sentinel, sentinel


class TestGetDesc:
    @mark.parametrize(
        ("func", "desc", "expected"),
        [
            param(neg, sentinel, {"desc": "neg"}),
            param(neg, None, {}),
            param(partial(neg), sentinel, {}),
            param(partial(neg), None, {}),
        ],
    )
    def test_main(
        self,
        func: Callable[..., Any],
        desc: Union[Optional[str], Sentinel],
        expected: dict[str, str],
    ) -> None:
        assert _get_desc(func, desc) == expected


class TestPMap:
    @mark.parametrize("parallelism", [param("processes"), param("threads")])
    @mark.parametrize("n_jobs", [param(1), param(2)])
    def test_unary(
        self, parallelism: Literal["processes", "threads"], n_jobs: int
    ) -> None:
        result = pmap(neg, [1, 2, 3], parallelism=parallelism, n_jobs=n_jobs)
        expected = [-1, -2, -3]
        assert result == expected

    @mark.parametrize("parallelism", [param("processes"), param("threads")])
    @mark.parametrize("n_jobs", [param(1), param(2)])
    def test_binary(
        self, parallelism: Literal["processes", "threads"], n_jobs: int
    ) -> None:
        result = pmap(
            pow, [2, 3, 10], [5, 2, 3], parallelism=parallelism, n_jobs=n_jobs
        )
        expected = [32, 9, 1000]
        assert result == expected


class TestPStarMap:
    @mark.parametrize("parallelism", [param("processes"), param("threads")])
    @mark.parametrize("n_jobs", [param(1), param(2)])
    def test_unary(
        self, parallelism: Literal["processes", "threads"], n_jobs: int
    ) -> None:
        result = pstarmap(
            neg, [(1,), (2,), (3,)], parallelism=parallelism, n_jobs=n_jobs
        )
        expected = [-1, -2, -3]
        assert result == expected

    @mark.parametrize("parallelism", [param("processes"), param("threads")])
    @mark.parametrize("n_jobs", [param(1), param(2)])
    def test_binary(
        self, parallelism: Literal["processes", "threads"], n_jobs: int
    ) -> None:
        result = pstarmap(
            pow, [(2, 5), (3, 2), (10, 3)], parallelism=parallelism, n_jobs=n_jobs
        )
        expected = [32, 9, 1000]
        assert result == expected
