from __future__ import annotations

from collections.abc import Sequence
from itertools import chain
from re import escape
from typing import Any

from hypothesis import given
from hypothesis.strategies import DataObject, data, integers, lists, sampled_from, sets
from pytest import mark, param, raises

from utilities.itertools import (
    CheckDuplicatesError,
    OneEmptyError,
    OneNonUniqueError,
    check_duplicates,
    is_iterable_not_str,
    is_sized_not_str,
    one,
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


class TestIsIterableNotStr:
    @mark.parametrize(
        ("obj", "expected"),
        [param(None, False), param([], True), param((), True), param("", False)],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        assert is_iterable_not_str(obj) is expected


class TestIsSizedNotStr:
    @mark.parametrize(
        ("obj", "expected"),
        [param(None, False), param([], True), param((), True), param("", False)],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        assert is_sized_not_str(obj) is expected


class TestOne:
    def test_empty(self) -> None:
        with raises(
            OneEmptyError, match=escape("too few items in iterable (expected 1)")
        ):
            _ = one([])

    def test_one(self) -> None:
        assert one([None]) is None

    def test_multiple(self) -> None:
        with raises(
            OneNonUniqueError,
            match="Expected exactly one item in iterable, but got 1, 2, and "
            "perhaps more",
        ):
            _ = one([1, 2, 3])
