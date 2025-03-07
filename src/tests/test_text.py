from __future__ import annotations

from hypothesis import given
from hypothesis.strategies import integers, lists, sampled_from, sets
from pytest import mark, param

from utilities.hypothesis import text_ascii
from utilities.sentinel import sentinel
from utilities.text import (
    join_strs,
    repr_encode,
    snake_case,
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


class TestSnakeCase:
    @given(
        case=sampled_from([
            ("API", "api"),
            ("APIResponse", "api_response"),
            ("ApplicationController", "application_controller"),
            ("Area51Controller", "area51_controller"),
            ("FreeBSD", "free_bsd"),
            ("HTML", "html"),
            ("HTMLTidy", "html_tidy"),
            ("HTMLTidyGenerator", "html_tidy_generator"),
            ("HTMLVersion", "html_version"),
            ("NoHTML", "no_html"),
            ("One   Two", "one_two"),
            ("One  Two", "one_two"),
            ("One Two", "one_two"),
            ("OneTwo", "one_two"),
            ("One_Two", "one_two"),
            ("One__Two", "one_two"),
            ("One___Two", "one_two"),
            ("Product", "product"),
            ("SpecialGuest", "special_guest"),
            ("Text", "text"),
            ("Text123", "text123"),
            ("_APIResponse_", "_api_response_"),
            ("_API_", "_api_"),
            ("__APIResponse__", "_api_response_"),
            ("__API__", "_api_"),
            ("__impliedVolatility_", "_implied_volatility_"),
            ("_itemID", "_item_id"),
            ("_lastPrice__", "_last_price_"),
            ("_symbol", "_symbol"),
            ("aB", "a_b"),
            ("changePct", "change_pct"),
            ("changePct_", "change_pct_"),
            ("impliedVolatility", "implied_volatility"),
            ("lastPrice", "last_price"),
            ("memMB", "mem_mb"),
            ("sizeX", "size_x"),
            ("symbol", "symbol"),
            ("testNTest", "test_n_test"),
            ("text", "text"),
            ("text123", "text123"),
        ])
    )
    def test_main(self, *, case: tuple[str, str]) -> None:
        text, expected = case
        result = snake_case(text)
        assert result == expected


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
