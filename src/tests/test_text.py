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

from utilities.hypothesis import text_ascii
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
    parse_bool,
    parse_none,
    repr_encode,
    snake_case,
    split_key_value_pairs,
    split_str,
    str_encode,
    strip_and_dedent,
)

if TYPE_CHECKING:
    from collections.abc import Sequence


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

    @given(
        text=sampled_from([
            "00",
            "11",
            "ffalsee",
            "invalid",
            "nn",
            "nnoo",
            "oofff",
            "oonn",
            "ttruee",
            "yy",
            "yyess",
        ])
    )
    def test_error(self, *, text: str) -> None:
        with raises(ParseBoolError, match="Unable to parse boolean value; got '.*'"):
            _ = parse_bool(text)


class TestParseNone:
    @given(data=data())
    def test_main(self, *, data: DataObject) -> None:
        text = str(None)
        text_use = data.draw(sampled_from(["", text, text.lower(), text.upper()]))
        result = parse_none(text_use)
        assert result is None

    @given(text=sampled_from(["invalid", "nnonee"]))
    def test_error(self, *, text: str) -> None:
        with raises(ParseNoneError, match="Unable to parse null value; got '.*'"):
            _ = parse_none(text)


class TestReprEncode:
    @given(n=integers())
    def test_main(self, *, n: int) -> None:
        result = repr_encode(n)
        expected = repr(n).encode()
        assert result == expected


class TestSplitKeyValuePairs:
    @given(
        case=sampled_from([
            ("", []),
            ("a=1", [("a", "1")]),
            ("a=1,b=22", [("a", "1"), ("b", "22")]),
            ("a=1,b=22,c=333", [("a", "1"), ("b", "22"), ("c", "333")]),
            ("=1", [("", "1")]),
            ("a=", [("a", "")]),
            ("a=1,=22,c=333", [("a", "1"), ("", "22"), ("c", "333")]),
            ("a=1,b=,c=333", [("a", "1"), ("b", ""), ("c", "333")]),
            ("a=1,b=(22,22,22),c=333", [("a", "1"), ("b", "(22,22,22)"), ("c", "333")]),
            ("a=1,b=(c=22),c=333", [("a", "1"), ("b", "(c=22)"), ("c", "333")]),
        ])
    )
    def test_main(self, *, case: tuple[str, Sequence[tuple[str, str]]]) -> None:
        text, expected = case
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
    @given(
        data=data(),
        case=sampled_from([
            ("", 0, []),
            (r"\,", 1, [""]),
            (",", 2, ["", ""]),
            (",,", 3, ["", "", ""]),
            ("1", 1, ["1"]),
            ("1,22", 2, ["1", "22"]),
            ("1,22,333", 3, ["1", "22", "333"]),
            ("1,,333", 3, ["1", "", "333"]),
            ("1,(22,22,22),333", 5, ["1", "(22", "22", "22)", "333"]),
        ]),
    )
    def test_main(self, *, data: DataObject, case: tuple[str, int, list[str]]) -> None:
        text, n, expected = case
        n_use = data.draw(just(n) | none())
        result = split_str(text, n=n_use)
        if n_use is None:
            assert result == expected
        else:
            assert result == tuple(expected)
        assert join_strs(result) == text

    @given(
        data=data(),
        case=sampled_from([
            ("1", 1, ["1"]),
            ("1,22", 2, ["1", "22"]),
            ("1,22,333", 3, ["1", "22", "333"]),
            ("1,(22),333", 3, ["1", "(22)", "333"]),
            ("1,(22,22),333", 3, ["1", "(22,22)", "333"]),
            ("1,(22,22,22),333", 3, ["1", "(22,22,22)", "333"]),
        ]),
    )
    def test_brackets(
        self, *, data: DataObject, case: tuple[str, int, list[str]]
    ) -> None:
        text, n, expected = case
        n_use = data.draw(just(n) | none())
        result = split_str(text, brackets=[("(", ")")], n=n_use)
        if n_use is None:
            assert result == expected
        else:
            assert result == tuple(expected)
        assert join_strs(result) == text

    @given(texts=lists(text_ascii()))
    def test_generic(self, *, texts: list[str]) -> None:
        assert split_str(join_strs(texts)) == texts

    @given(texts=sets(text_ascii()))
    def test_sort(self, *, texts: set[str]) -> None:
        assert split_str(join_strs(texts, sort=True)) == sorted(texts)

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
