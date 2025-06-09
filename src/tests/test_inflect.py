from __future__ import annotations

from pytest import mark, param

from utilities.inflect import counted_noun


class TestCountedNoun:
    @mark.parametrize(("num", "noun", "expected"), [param(0, "word", "0 words")])
    def test_main(self, *, num: int, noun: str, expected: str) -> None:
        result = counted_noun(num, noun)
        assert result == expected
