from __future__ import annotations

from hypothesis import given
from hypothesis.strategies import integers, lists, sets
from pytest import mark, param

from utilities.hypothesis import text_ascii
from utilities.sentinel import sentinel
from utilities.text import (
    join_strs,
    repr_encode,
    split_str,
    str_encode,
    strip_and_dedent,
)


class TestReprEncode:
    @given(n=integers())
    def test_main(self, *, n: int) -> None:
        result = repr_encode(n)
        expected = repr(n).encode()
        assert result == expected


class TestSplitStrAndJoinStr:
    @mark.parametrize(
        ("text", "texts"),
        [
            param("", [""]),
            param("1", ["1"]),
            param("1,2", ["1", "2"]),
            param(",", ["", ""]),
            param(str(sentinel), []),
        ],
    )
    def test_main(self, *, text: str, texts: list[str]) -> None:
        assert split_str(text) == texts
        assert join_strs(texts) == text

    @given(texts=lists(text_ascii()))
    def test_generic(self, *, texts: list[str]) -> None:
        assert split_str(join_strs(texts)) == texts

    @given(texts=sets(text_ascii()))
    def test_sort(self, *, texts: set[str]) -> None:
        assert split_str(join_strs(texts, sort=True)) == sorted(texts)


class TestStrEncode:
    @given(n=integers())
    def test_main(self, *, n: int) -> None:
        result = str_encode(n)
        expected = str(n).encode()
        assert result == expected


class TestStripAndDedent:
    @mark.parametrize("trailing", [param(True), param(False)])
    def test_main(self, *, trailing: bool) -> None:
        text = """
               This is line 1.
               This is line 2.
               """
        result = strip_and_dedent(text, trailing=trailing)
        expected = "This is line 1.\nThis is line 2." + ("\n" if trailing else "")
        assert result == expected
