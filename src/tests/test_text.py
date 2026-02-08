from __future__ import annotations

from typing import TYPE_CHECKING, Any

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
    ParseNoneError,
    _SplitKeyValuePairsDuplicateKeysError,
    _SplitKeyValuePairsSplitError,
    _SplitStrClosingBracketMismatchedError,
    _SplitStrClosingBracketUnmatchedError,
    _SplitStrCountError,
    _SplitStrOpeningBracketUnmatchedError,
    join_strs,
    parse_none,
    prompt_bool,
    secret_str,
    split_f_str_equals,
    split_key_value_pairs,
    split_str,
    str_encode,
    to_bool,
    to_str,
)

if TYPE_CHECKING:
    from utilities.constants import Sentinel


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


class TestSplitFStrEquals:
    @mark.parametrize(
        ("x", "expected"),
        [param(123, "123"), param("a=1", "'a=1'"), param("a=1,b=2", "'a=1,b=2'")],
    )
    def test_main2(self, *, x: Any, expected: str) -> None:
        result1, result2 = split_f_str_equals(f"{x=}")
        assert result1 == "x"
        assert result2 == expected

    def test_variable_with_digits(self) -> None:
        x123 = 123
        result1, result2 = split_f_str_equals(f"{x123=}")
        assert result1 == "x123"
        assert result2 == "123"

    def test_variable_with_underscore(self) -> None:
        x_y_z = 123
        result1, result2 = split_f_str_equals(f"{x_y_z=}")
        assert result1 == "x_y_z"
        assert result2 == "123"


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
