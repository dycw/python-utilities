from __future__ import annotations

from pytest import mark, param

from utilities.inflect import counted_noun


class TestCountedNoun:
    @mark.parametrize(
        ("num", "expected"),
        [
            param(0, "0 words"),
            param(1, "1 word"),
            param(2, "2 words"),
            param(3, "3 words"),
        ],
    )
    def test_main(self, *, num: int, expected: str) -> None:
        result = counted_noun(num, "word")
        assert result == expected
