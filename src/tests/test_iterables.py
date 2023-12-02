from __future__ import annotations

from collections.abc import Sequence
from itertools import chain
from typing import Any

from hypothesis import given
from hypothesis.strategies import DataObject, data, integers, lists, sampled_from, sets
from pytest import mark, param, raises

from utilities.iterables import (
    CheckDuplicatesError,
    CheckLengthError,
    check_duplicates,
    check_length,
    ensure_hashables,
    is_iterable_not_str,
)


class TestCheckDuplicates:
    @given(x=sets(integers()))
    def test_main(self, *, x: set[int]) -> None:
        check_duplicates(x)

    @given(data=data(), x=lists(integers(), min_size=1))
    def test_error(self, *, data: DataObject, x: Sequence[int]) -> None:
        x_i = data.draw(sampled_from(x))
        y = chain(x, [x_i])
        with raises(CheckDuplicatesError):
            check_duplicates(y)


class TestCheckLength:
    def test_equal_pass(self) -> None:
        check_length(range(0), equal=0)

    def test_equal_fail(self) -> None:
        with raises(CheckLengthError):
            check_length(range(0), equal=1)

    @mark.parametrize("equal_or_approx", [param(10), param((11, 0.1))])
    def test_equal_or_approx_pass(
        self, *, equal_or_approx: int | tuple[int, float]
    ) -> None:
        check_length(range(10), equal_or_approx=equal_or_approx)

    @mark.parametrize("equal_or_approx", [param(10), param((11, 0.1))])
    def test_equal_or_approx_fail(
        self, *, equal_or_approx: int | tuple[int, float]
    ) -> None:
        with raises(CheckLengthError):
            check_length(range(0), equal_or_approx=equal_or_approx)

    def test_min_pass(self) -> None:
        check_length(range(1), min=1)

    def test_min_error(self) -> None:
        with raises(CheckLengthError):
            check_length(range(0), min=1)

    def test_max_pass(self) -> None:
        check_length(range(0), max=1)

    def test_max_error(self) -> None:
        with raises(CheckLengthError):
            check_length(range(2), max=1)


class TestEnsureHashables:
    def test_main(self) -> None:
        assert ensure_hashables(1, 2, a=3, b=4) == ([1, 2], {"a": 3, "b": 4})


class TestIsIterableNotStr:
    @mark.parametrize(
        ("obj", "expected"),
        [param(None, False), param([], True), param((), True), param("", False)],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        assert is_iterable_not_str(obj) is expected
