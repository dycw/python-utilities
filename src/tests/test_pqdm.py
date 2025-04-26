from __future__ import annotations

from functools import partial
from itertools import starmap
from operator import neg, sub
from typing import TYPE_CHECKING, Any

from hypothesis import given
from hypothesis.strategies import integers, lists, sampled_from, tuples
from pytest import mark, param

from utilities.functions import get_class_name
from utilities.hypothesis import int32s, pairs, settings_with_reduced_examples
from utilities.iterables import transpose
from utilities.pqdm import _get_desc, pqdm_map, pqdm_starmap
from utilities.sentinel import Sentinel, sentinel
from utilities.types import Parallelism, StrStrMapping
from utilities.typing import get_args

if TYPE_CHECKING:
    from collections.abc import Callable


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
        expected: StrStrMapping,
    ) -> None:
        assert _get_desc(desc, func) == expected

    def test_class(self) -> None:
        class Example:
            def __call__(self) -> None:
                return

        assert _get_desc(sentinel, Example()) == {"desc": get_class_name(Example)}


class TestPqdmMap:
    @given(
        xs=lists(int32s(), max_size=10),
        parallelism=sampled_from(get_args(Parallelism)),
        n_jobs=integers(1, 2),
    )
    @settings_with_reduced_examples()
    def test_unary(
        self, *, xs: list[int], parallelism: Parallelism, n_jobs: int
    ) -> None:
        result = pqdm_map(neg, xs, parallelism=parallelism, n_jobs=n_jobs)
        expected = list(map(neg, xs))
        assert result == expected

    @given(
        iterable=lists(pairs(int32s()), min_size=1, max_size=10),
        parallelism=sampled_from(get_args(Parallelism)),
        n_jobs=integers(1, 2),
    )
    @settings_with_reduced_examples()
    def test_binary(
        self, *, iterable: list[tuple[int, int]], parallelism: Parallelism, n_jobs: int
    ) -> None:
        xs, ys = transpose(iterable)
        result = pqdm_map(sub, xs, ys, parallelism=parallelism, n_jobs=n_jobs)
        expected = list(starmap(sub, iterable))
        assert result == expected


class TestPqdmStarMap:
    @given(
        iterable=lists(tuples(int32s()), max_size=10),
        parallelism=sampled_from(get_args(Parallelism)),
        n_jobs=integers(1, 2),
    )
    @settings_with_reduced_examples()
    def test_unary(
        self, *, iterable: list[tuple[int]], parallelism: Parallelism, n_jobs: int
    ) -> None:
        result = pqdm_starmap(neg, iterable, parallelism=parallelism, n_jobs=n_jobs)
        expected = list(starmap(neg, iterable))
        assert result == expected

    @given(
        iterable=lists(pairs(int32s()), min_size=1, max_size=10),
        parallelism=sampled_from(get_args(Parallelism)),
        n_jobs=integers(1, 2),
    )
    @settings_with_reduced_examples()
    def test_binary(
        self, *, iterable: list[tuple[int, int]], parallelism: Parallelism, n_jobs: int
    ) -> None:
        result = pqdm_starmap(sub, iterable, parallelism=parallelism, n_jobs=n_jobs)
        expected = list(starmap(sub, iterable))
        assert result == expected
