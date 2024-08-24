from __future__ import annotations

from functools import partial
from operator import neg, pow
from typing import TYPE_CHECKING, Any, Literal

from pytest import mark, param

from utilities.functions import get_class_name
from utilities.pqdm import _get_desc, pmap, pstarmap
from utilities.sentinel import Sentinel, sentinel

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping


class TestGetDesc:
    @mark.parametrize(
        ("desc", "func", "expected"),
        [
            param(sentinel, neg, {"desc": "neg"}),
            param(sentinel, partial(neg), {"desc": "neg"}),
            param(None, neg, {}),
            param("custom", neg, {"desc": "custom"}),
        ],
    )
    def test_main(
        self,
        *,
        desc: str | None | Sentinel,
        func: Callable[..., Any],
        expected: Mapping[str, str],
    ) -> None:
        assert _get_desc(desc, func) == expected

    def test_class(self) -> None:
        class Example:
            def __call__(self) -> None:
                return

        assert _get_desc(sentinel, Example()) == {"desc": get_class_name(Example)}


class TestPMap:
    @mark.parametrize("parallelism", [param("processes"), param("threads")])
    @mark.parametrize("n_jobs", [param(1), param(2)])
    def test_unary(
        self, *, parallelism: Literal["processes", "threads"], n_jobs: int
    ) -> None:
        result = pmap(neg, [1, 2, 3], parallelism=parallelism, n_jobs=n_jobs)
        expected = [-1, -2, -3]
        assert result == expected

    @mark.parametrize("parallelism", [param("processes"), param("threads")])
    @mark.parametrize("n_jobs", [param(1), param(2)])
    def test_binary(
        self, *, parallelism: Literal["processes", "threads"], n_jobs: int
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
        self, *, parallelism: Literal["processes", "threads"], n_jobs: int
    ) -> None:
        result = pstarmap(
            neg, [(1,), (2,), (3,)], parallelism=parallelism, n_jobs=n_jobs
        )
        expected = [-1, -2, -3]
        assert result == expected

    @mark.parametrize("parallelism", [param("processes"), param("threads")])
    @mark.parametrize("n_jobs", [param(1), param(2)])
    def test_binary(
        self, *, parallelism: Literal["processes", "threads"], n_jobs: int
    ) -> None:
        result = pstarmap(
            pow, [(2, 5), (3, 2), (10, 3)], parallelism=parallelism, n_jobs=n_jobs
        )
        expected = [32, 9, 1000]
        assert result == expected
