from __future__ import annotations

from itertools import chain
from typing import TYPE_CHECKING

from hypothesis import given
from hypothesis.strategies import (
    DataObject,
    booleans,
    data,
    integers,
    just,
    lists,
    none,
    sampled_from,
    sets,
)
from pytest import mark, param, raises

from utilities.hypothesis import sentinels, text_ascii
from utilities.text import (
    ParseBoolError,
    ParseNoneError,
    _SplitKeyValuePairsDuplicateKeysError,
    _SplitKeyValuePairsSplitError,
    _SplitStrClosingBracketMismatchedError,
    _SplitStrClosingBracketUnmatchedError,
    _SplitStrCountError,
    _SplitStrOpeningBracketUnmatchedError,
    join_strs,
    kebab_case,
    parse_bool,
    parse_none,
    pascal_case,
    prompt_bool,
    repr_encode,
    secret_str,
    snake_case,
    split_key_value_pairs,
    split_str,
    str_encode,
    strip_and_dedent,
    to_bool,
    to_str,
    unique_str,
)

if TYPE_CHECKING:
    from utilities.sentinel import Sentinel


class TestParseBool:
    @given(data=data(), value=booleans())
    def test_main(self, *, data: DataObject, value: bool) -> None:
        match value:
            case True:
                extra_cased_texts = ["Y", "Yes", "On"]
            case False:
                extra_cased_texts = ["N", "No", "Off"]
        all_cased_texts = list(chain([str(value), str(int(value))], extra_cased_texts))
        all_texts = list(
            chain(
                extra_cased_texts,
                map(str.lower, all_cased_texts),
                map(str.upper, all_cased_texts),
            )
        )
        text = data.draw(sampled_from(all_texts))
        result = parse_bool(text)
        assert result is value

    @mark.parametrize(
        "text",
        [
            param("00"),
            param("11"),
            param("ffalsee"),
            param("invalid"),
            param("nn"),
            param("nnoo"),
            param("oofff"),
            param("oonn"),
            param("ttruee"),
            param("yy"),
            param("yyess"),
        ],
    )
    def test_error(self, *, text: str) -> None:
        with raises(ParseBoolError, match=r"Unable to parse boolean value; got '.*'"):
            _ = parse_bool(text)


class TestParseNone:
    @given(data=data())
    def test_main(self, *, data: DataObject) -> None:
        text = str(None)
        text_use = data.draw(sampled_from(["", text, text.lower(), text.upper()]))
        result = parse_none(text_use)
        assert result is None

    @mark.parametrize("text", [param("invalid"), param("nnonee")])
    def test_error(self, *, text: str) -> None:
        with raises(ParseNoneError, match=r"Unable to parse null value; got '.*'"):
            _ = parse_none(text)


class TestReprEncode:
    @given(n=integers())
    def test_main(self, *, n: int) -> None:
        result = repr_encode(n)
        expected = repr(n).encode()
        assert result == expected


class TestPascalSnakeAndKebabCase:
    @mark.parametrize(
        ("text", "exp_pascal", "exp_snake", "exp_kebab"),
        [
            param("API", "API", "api", "api"),
            param("APIResponse", "APIResponse", "api_response", "api-response"),
            param(
                "ApplicationController",
                "ApplicationController",
                "application_controller",
                "application-controller",
            ),
            param(
                "Area51Controller",
                "Area51Controller",
                "area51_controller",
                "area51-controller",
            ),
            param("FreeBSD", "FreeBSD", "free_bsd", "free-bsd"),
            param("HTML", "HTML", "html", "html"),
            param("HTMLTidy", "HTMLTidy", "html_tidy", "html-tidy"),
            param(
                "HTMLTidyGenerator",
                "HTMLTidyGenerator",
                "html_tidy_generator",
                "html-tidy-generator",
            ),
            param("HTMLVersion", "HTMLVersion", "html_version", "html-version"),
            param("NoHTML", "NoHTML", "no_html", "no-html"),
            param("One   Two", "OneTwo", "one_two", "one-two"),
            param("One  Two", "OneTwo", "one_two", "one-two"),
            param("One Two", "OneTwo", "one_two", "one-two"),
            param("OneTwo", "OneTwo", "one_two", "one-two"),
            param("One_Two", "OneTwo", "one_two", "one-two"),
            param("One__Two", "OneTwo", "one_two", "one-two"),
            param("One___Two", "OneTwo", "one_two", "one-two"),
            param("Product", "Product", "product", "product"),
            param("SpecialGuest", "SpecialGuest", "special_guest", "special-guest"),
            param("Text", "Text", "text", "text"),
            param("Text123", "Text123", "text123", "text123"),
            param(
                "Text123Text456", "Text123Text456", "text123_text456", "text123-text456"
            ),
            param("_APIResponse_", "APIResponse", "_api_response_", "-api-response-"),
            param("_API_", "API", "_api_", "-api-"),
            param("__APIResponse__", "APIResponse", "_api_response_", "-api-response-"),
            param("__API__", "API", "_api_", "-api-"),
            param(
                "__impliedVolatility_",
                "ImpliedVolatility",
                "_implied_volatility_",
                "-implied-volatility-",
            ),
            param("_itemID", "ItemID", "_item_id", "-item-id"),
            param("_lastPrice__", "LastPrice", "_last_price_", "-last-price-"),
            param("_symbol", "Symbol", "_symbol", "-symbol"),
            param("aB", "AB", "a_b", "a-b"),
            param("changePct", "ChangePct", "change_pct", "change-pct"),
            param("changePct_", "ChangePct", "change_pct_", "change-pct-"),
            param(
                "impliedVolatility",
                "ImpliedVolatility",
                "implied_volatility",
                "implied-volatility",
            ),
            param("lastPrice", "LastPrice", "last_price", "last-price"),
            param("memMB", "MemMB", "mem_mb", "mem-mb"),
            param("sizeX", "SizeX", "size_x", "size-x"),
            param("symbol", "Symbol", "symbol", "symbol"),
            param("testNTest", "TestNTest", "test_n_test", "test-n-test"),
            param("text", "Text", "text", "text"),
            param("text123", "Text123", "text123", "text123"),
        ],
    )
    def test_main(
        self, *, text: str, exp_pascal: str, exp_snake: str, exp_kebab: str
    ) -> None:
        assert pascal_case(text) == exp_pascal
        assert snake_case(text) == exp_snake
        assert kebab_case(text) == exp_kebab


class TestPromptBool:
    def test_main(self) -> None:
        assert prompt_bool(confirm=True)


class TestSecretStr:
    def test_main(self) -> None:
        s = secret_str("text")
        assert repr(s) == secret_str._REPR
        assert str(s) == secret_str._REPR

    def test_open(self) -> None:
        s = secret_str("text")
        assert isinstance(s.str, str)
        assert not isinstance(s.str, secret_str)
        assert repr(s.str) == repr("text")
        assert str(s.str) == "text"


class TestSplitKeyValuePairs:
    @mark.parametrize(
        ("text", "expected"),
        [
            param("", []),
            param("a=1", [("a", "1")]),
            param("a=1,b=22", [("a", "1"), ("b", "22")]),
            param("a=1,b=22,c=333", [("a", "1"), ("b", "22"), ("c", "333")]),
            param("=1", [("", "1")]),
            param("a=", [("a", "")]),
            param("a=1,=22,c=333", [("a", "1"), ("", "22"), ("c", "333")]),
            param("a=1,b=,c=333", [("a", "1"), ("b", ""), ("c", "333")]),
            param(
                "a=1,b=(22,22,22),c=333",
                [("a", "1"), ("b", "(22,22,22)"), ("c", "333")],
            ),
            param("a=1,b=(c=22),c=333", [("a", "1"), ("b", "(c=22)"), ("c", "333")]),
        ],
    )
    def test_main(self, *, text: str, expected: str) -> None:
        result = split_key_value_pairs(text)
        assert result == expected

    def test_mapping(self) -> None:
        result = split_key_value_pairs("a=1,b=22,c=333", mapping=True)
        expected = {"a": "1", "b": "22", "c": "333"}
        assert result == expected

    def test_error_split_list(self) -> None:
        with raises(
            _SplitKeyValuePairsSplitError,
            match=r"Unable to split 'a=1,b=\(c=22\],d=333' into key-value pairs",
        ):
            _ = split_key_value_pairs("a=1,b=(c=22],d=333")

    def test_error_split_pair(self) -> None:
        with raises(
            _SplitKeyValuePairsSplitError,
            match=r"Unable to split 'a=1,b=22=22,c=333' into key-value pairs",
        ):
            _ = split_key_value_pairs("a=1,b=22=22,c=333")

    def test_error_duplicate_keys(self) -> None:
        with raises(
            _SplitKeyValuePairsDuplicateKeysError,
            match=r"Unable to split 'a=1,a=22,a=333' into a mapping since there are duplicate keys; got \{'a': 3\}",
        ):
            _ = split_key_value_pairs("a=1,a=22,a=333", mapping=True)


class TestSplitAndJoinStr:
    @given(data=data())
    @mark.parametrize(
        ("text", "n", "expected"),
        [
            param("", 0, ()),
            param(r"\,", 1, ("",)),
            param(",", 2, ("", "")),
            param(",,", 3, ("", "", "")),
            param("1", 1, ("1",)),
            param("1,22", 2, ("1", "22")),
            param("1,22,333", 3, ("1", "22", "333")),
            param("1,,333", 3, ("1", "", "333")),
            param("1,(22,22,22),333", 5, ("1", "(22", "22", "22)", "333")),
        ],
    )
    def test_main(
        self, *, data: DataObject, text: str, n: int, expected: list[str]
    ) -> None:
        n_use = data.draw(just(n) | none())
        result = split_str(text, n=n_use)
        if n_use is None:
            assert result == expected
        else:
            assert result == tuple(expected)
        assert join_strs(result) == text

    @given(data=data())
    @mark.parametrize(
        ("text", "n", "expected"),
        [
            param("1", 1, ("1",)),
            param("1,22", 2, ("1", "22")),
            param("1,22,333", 3, ("1", "22", "333")),
            param("1,(22),333", 3, ("1", "(22)", "333")),
            param("1,(22,22),333", 3, ("1", "(22,22)", "333")),
            param("1,(22,22,22),333", 3, ("1", "(22,22,22)", "333")),
        ],
    )
    def test_brackets(
        self, *, data: DataObject, text: str, n: int, expected: list[str]
    ) -> None:
        n_use = data.draw(just(n) | none())
        result = split_str(text, brackets=[("(", ")")], n=n_use)
        if n_use is None:
            assert result == expected
        else:
            assert result == tuple(expected)
        assert join_strs(result) == text

    @given(texts=lists(text_ascii()).map(tuple))
    def test_generic(self, *, texts: tuple[str, ...]) -> None:
        assert split_str(join_strs(texts)) == texts

    @given(texts=sets(text_ascii()))
    def test_sort(self, *, texts: set[str]) -> None:
        assert split_str(join_strs(texts, sort=True)) == tuple(sorted(texts))

    def test_error_closing_bracket_mismatched(self) -> None:
        with raises(
            _SplitStrClosingBracketMismatchedError,
            match=r"Unable to split '1,\(22\},333'; got mismatched '\(' at position 2 and '}' at position 5",
        ):
            _ = split_str("1,(22},333", brackets=[("(", ")"), ("{", "}")])

    def test_error_closing_bracket_unmatched(self) -> None:
        with raises(
            _SplitStrClosingBracketUnmatchedError,
            match=r"Unable to split '1,22\),333'; got unmatched '\)' at position 4",
        ):
            _ = split_str("1,22),333", brackets=[("(", ")")])

    def test_error_count(self) -> None:
        with raises(
            _SplitStrCountError,
            match=r"Unable to split '1,22,333' into 4 part\(s\); got 3",
        ):
            _ = split_str("1,22,333", n=4)

    def test_error_opening_bracket(self) -> None:
        with raises(
            _SplitStrOpeningBracketUnmatchedError,
            match=r"Unable to split '1,\(22,333'; got unmatched '\(' at position 2",
        ):
            _ = split_str("1,(22,333", brackets=[("(", ")")])


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


class TestToBool:
    @given(bool_=booleans() | none() | sentinels())
    def test_bool_none_or_sentinel(self, *, bool_: bool | None | Sentinel) -> None:
        assert to_bool(bool_) is bool_

    @given(bool_=booleans())
    def test_str(self, *, bool_: bool) -> None:
        assert to_bool(str(bool_)) is bool_

    @given(bool_=booleans())
    def test_callable(self, *, bool_: bool) -> None:
        assert to_bool(lambda: bool_) is bool_


class TestToStr:
    @given(text=text_ascii())
    def test_str(self, *, text: str) -> None:
        assert to_str(text) == text

    @given(text=text_ascii())
    def test_callable(self, *, text: str) -> None:
        assert to_str(lambda: text) == text

    @given(text=none() | sentinels())
    def test_none_or_sentinel(self, *, text: None | Sentinel) -> None:
        assert to_str(text) is text


class TestUniqueStrs:
    def test_main(self) -> None:
        first, second = [unique_str() for _ in range(2)]
        assert first != second
