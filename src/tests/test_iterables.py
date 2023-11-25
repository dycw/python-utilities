from __future__ import annotations

from collections.abc import Sequence
from itertools import chain
from typing import Any

from hypothesis import given
from hypothesis.strategies import DataObject, data, integers, lists, sampled_from, sets
from pytest import mark, param, raises

from utilities.iterables import (
    CheckDuplicatesError,
    check_duplicates,
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
