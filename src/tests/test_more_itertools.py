from __future__ import annotations

from re import escape

from pytest import raises

from utilities.more_itertools import OneEmptyError, OneNonUniqueError, one


class TestOne:
    def test_main(self) -> None:
        assert one([None]) is None

    def test_error_empty(self) -> None:
        with raises(
            OneEmptyError, match=escape("too few items in iterable (expected 1)")
        ):
            _ = one([])

    def test_error_non_unique(self) -> None:
        with raises(
            OneNonUniqueError,
            match="Expected exactly one item in iterable, but got 1, 2, and "
            "perhaps more",
        ):
            _ = one([1, 2, 3])
