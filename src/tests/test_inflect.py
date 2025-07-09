from __future__ import annotations

from typing import TYPE_CHECKING

from pytest import mark, param

from utilities.inflect import counted_noun

if TYPE_CHECKING:
    from collections.abc import Sized


class TestCountedNoun:
    @mark.parametrize(
        ("obj", "expected"),
        [
            param(0, "0 words"),
            param(1, "1 word"),
            param(2, "2 words"),
            param(3, "3 words"),
            param([], "0 words"),
            param(["a"], "1 word"),
            param(["a", "b"], "2 words"),
            param(["a", "b", "c"], "3 words"),
        ],
    )
    def test_main(self, *, obj: int | Sized, expected: str) -> None:
        result = counted_noun(obj, "word")
        assert result == expected
