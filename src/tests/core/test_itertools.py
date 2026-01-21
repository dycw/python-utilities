from __future__ import annotations

import re
from re import DOTALL
from typing import TYPE_CHECKING, Any

from hypothesis import given
from hypothesis.strategies import (
    DataObject,
    binary,
    data,
    dictionaries,
    integers,
    lists,
    sampled_from,
    sets,
    text,
)
from pytest import mark, param, raises

from utilities.core import (
    OneEmptyError,
    OneNonUniqueError,
    OneStrEmptyError,
    OneStrNonUniqueError,
    always_iterable,
    one,
    one_str,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator


class TestAlwaysIterable:
    @given(x=binary())
    def test_bytes(self, *, x: bytes) -> None:
        assert list(always_iterable(x)) == [x]

    @given(x=dictionaries(text(), integers()))
    def test_dict(self, *, x: dict[str, int]) -> None:
        assert list(always_iterable(x)) == list(x)

    @given(x=integers())
    def test_integer(self, *, x: int) -> None:
        assert list(always_iterable(x)) == [x]

    @given(x=lists(binary()))
    def test_list_of_bytes(self, *, x: list[bytes]) -> None:
        assert list(always_iterable(x)) == x

    @given(x=text())
    def test_string(self, *, x: str) -> None:
        assert list(always_iterable(x)) == [x]

    @given(x=lists(integers()))
    def test_list_of_integers(self, *, x: list[int]) -> None:
        assert list(always_iterable(x)) == x

    @given(x=lists(text()))
    def test_list_of_strings(self, *, x: list[str]) -> None:
        assert list(always_iterable(x)) == x

    def test_generator(self) -> None:
        def yield_ints() -> Iterator[int]:
            yield 0
            yield 1

        assert list(always_iterable(yield_ints())) == [0, 1]


class TestOne:
    @mark.parametrize(
        "args", [param(([None],)), param(([None], [])), param(([None], [], []))]
    )
    def test_main(self, *, args: tuple[Iterable[Any], ...]) -> None:
        assert one(*args) is None

    @mark.parametrize("args", [param([]), param(([], [])), param(([], [], []))])
    def test_error_empty(self, *, args: tuple[Iterable[Any], ...]) -> None:
        with raises(OneEmptyError, match=r"Iterable\(s\) .* must not be empty"):
            _ = one(*args)

    @given(iterable=sets(integers(), min_size=2))
    def test_error_non_unique(self, *, iterable: set[int]) -> None:
        with raises(
            OneNonUniqueError,
            match=re.compile(
                r"Iterable\(s\) .* must contain exactly one item; got .*, .* and perhaps more",
                flags=DOTALL,
            ),
        ):
            _ = one(iterable)


class TestOneStr:
    @given(data=data(), text=sampled_from(["a", "b", "c"]))
    def test_exact_match_case_insensitive(self, *, data: DataObject, text: str) -> None:
        text_use = data.draw(sampled_from([text.lower(), text.upper()]))
        assert one_str(["a", "b", "c"], text_use) == text

    @given(
        data=data(), case=sampled_from([("ab", "abc"), ("ad", "ade"), ("af", "afg")])
    )
    def test_head_case_insensitive(
        self, *, data: DataObject, case: tuple[str, str]
    ) -> None:
        head, expected = case
        head_use = data.draw(sampled_from([head.lower(), head.upper()]))
        assert one_str(["abc", "ade", "afg"], head_use, head=True) == expected

    @given(text=sampled_from(["a", "b", "c"]))
    def test_exact_match_case_sensitive(self, *, text: str) -> None:
        assert one_str(["a", "b", "c"], text, case_sensitive=True) == text

    @given(case=sampled_from([("ab", "abc"), ("ad", "ade"), ("af", "afg")]))
    def test_head_case_sensitive(self, *, case: tuple[str, str]) -> None:
        head, expected = case
        assert (
            one_str(["abc", "ade", "afg"], head, head=True, case_sensitive=True)
            == expected
        )

    def test_error_exact_match_case_insensitive_empty_error(self) -> None:
        with raises(
            OneStrEmptyError, match=r"Iterable .* does not contain 'd' \(modulo case\)"
        ):
            _ = one_str(["a", "b", "c"], "d")

    def test_error_exact_match_case_insensitive_non_unique_error(self) -> None:
        with raises(
            OneStrNonUniqueError,
            match=r"Iterable .* must contain 'a' exactly once \(modulo case\); got 'a', 'A' and perhaps more",
        ):
            _ = one_str(["a", "A"], "a")

    def test_error_head_case_insensitive_empty_error(self) -> None:
        with raises(
            OneStrEmptyError,
            match=r"Iterable .* does not contain any string starting with 'ac' \(modulo case\)",
        ):
            _ = one_str(["abc", "ade", "afg"], "ac", head=True)

    def test_error_head_case_insensitive_non_unique_error(self) -> None:
        with raises(
            OneStrNonUniqueError,
            match=r"Iterable .* must contain exactly one string starting with 'ab' \(modulo case\); got 'abc', 'ABC' and perhaps more",
        ):
            _ = one_str(["abc", "ABC"], "ab", head=True)

    def test_error_exact_match_case_sensitive_empty_error(self) -> None:
        with raises(OneStrEmptyError, match=r"Iterable .* does not contain 'A'"):
            _ = one_str(["a", "b", "c"], "A", case_sensitive=True)

    def test_error_exact_match_case_sensitive_non_unique(self) -> None:
        with raises(
            OneStrNonUniqueError,
            match=r"Iterable .* must contain 'a' exactly once; got 'a', 'a' and perhaps more",
        ):
            _ = one_str(["a", "a"], "a", case_sensitive=True)

    def test_error_head_case_sensitive_empty_error(self) -> None:
        with raises(
            OneStrEmptyError,
            match=r"Iterable .* does not contain any string starting with 'AB'",
        ):
            _ = one_str(["abc", "ade", "afg"], "AB", head=True, case_sensitive=True)

    def test_error_head_case_sensitive_non_unique(self) -> None:
        with raises(
            OneStrNonUniqueError,
            match=r"Iterable .* must contain exactly one string starting with 'ab'; got 'abc', 'abd' and perhaps more",
        ):
            _ = one_str(["abc", "abd"], "ab", head=True, case_sensitive=True)
