from __future__ import annotations

from functools import partial
from operator import neg
from typing import TYPE_CHECKING, Any

from hypothesis import given
from hypothesis.strategies import integers, sampled_from
from pytest import mark, param

from utilities.concurrent import _Parallelism
from utilities.functions import get_class_name
from utilities.pqdm import _get_desc, pmap, pstarmap
from utilities.sentinel import Sentinel, sentinel
from utilities.typing import get_args

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
    @given(parallelism=sampled_from(get_args(_Parallelism)), n_jobs=integers(1, 3))
    def test_unary(self, *, parallelism: _Parallelism, n_jobs: int) -> None:
        result = pmap(neg, [1, 2, 3], parallelism=parallelism, n_jobs=n_jobs)
        expected = [-1, -2, -3]
        assert result == expected

    @given(parallelism=sampled_from(get_args(_Parallelism)), n_jobs=integers(1, 3))
    def test_binary(self, *, parallelism: _Parallelism, n_jobs: int) -> None:
        result = pmap(
            pow, [2, 3, 10], [5, 2, 3], parallelism=parallelism, n_jobs=n_jobs
        )
        expected = [32, 9, 1000]
        assert result == expected


class TestPStarMap:
    @given(parallelism=sampled_from(get_args(_Parallelism)), n_jobs=integers(1, 3))
    def test_unary(self, *, parallelism: _Parallelism, n_jobs: int) -> None:
        result = pstarmap(
            neg, [(1,), (2,), (3,)], parallelism=parallelism, n_jobs=n_jobs
        )
        expected = [-1, -2, -3]
        assert result == expected

    @given(parallelism=sampled_from(get_args(_Parallelism)), n_jobs=integers(1, 3))
    def test_binary(self, *, parallelism: _Parallelism, n_jobs: int) -> None:
        result = pstarmap(
            pow, [(2, 5), (3, 2), (10, 3)], parallelism=parallelism, n_jobs=n_jobs
        )
        expected = [32, 9, 1000]
        assert result == expected
