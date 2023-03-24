from operator import neg, pow
from typing import Any, Literal, Optional, Union

from pytest import mark, param

from utilities.pqdm import _get_desc, _get_total, pmap, pstarmap
from utilities.sentinel import Sentinel, sentinel


class TestGetDesc:
    @mark.parametrize(
        ("desc", "expected"), [param(None, {}), param(sentinel, {"desc": "neg"})]
    )
    def test_main(
        self, desc: Union[Optional[str], Sentinel], expected: dict[str, str]
    ) -> None:
        assert _get_desc(neg, desc) == expected


class TestGetTotal:
    @mark.parametrize(
        ("iterables", "total", "expected"),
        [
            param((), None, 0),
            param((range(3),), None, 3),
            param((range(3), range(4)), None, 3),
            param((iter(range(3)),), None, None),
            param((iter(range(3)), range(4)), None, None),
            param((range(3), iter(range(4))), None, None),
            param((), 0, 0),
            param((), 1, 1),
            param((), 0.0, 0.0),
            param((), 1.0, 1.0),
        ],
    )
    def test_main(
        self,
        iterables: tuple[Any, ...],
        total: Optional[Union[int, float]],
        expected: Optional[Union[int, float]],
    ) -> None:
        assert _get_total(iterables, total) == expected


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
