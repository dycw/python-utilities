from __future__ import annotations

import re
from re import DOTALL
from typing import TYPE_CHECKING, Any, ClassVar

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
    tuples,
)
from pytest import mark, param, raises

from utilities.core import (
    CheckUniqueError,
    OneEmptyError,
    OneNonUniqueError,
    OneStrEmptyError,
    OneStrNonUniqueError,
    always_iterable,
    check_unique,
    chunked,
    one,
    one_str,
    take,
    transpose,
    unique_everseen,
)
from utilities.hypothesis import text_ascii
from utilities.typing import is_sequence_of

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Sequence


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


class TestCheckUnique:
    def test_main(self) -> None:
        check_unique("a", "b", "c")

    def test_error(self) -> None:
        with raises(
            CheckUniqueError,
            match=r"Iterable .* must only contain unique elements; got {'a': 2}",
        ):
            check_unique("a", "a", "b", "c")


class TestChunked:
    @mark.parametrize(
        ("iterable", "expected"),
        [
            param("ABCDEF", [["A", "B", "C"], ["D", "E", "F"]]),
            param("ABCDE", [["A", "B", "C"], ["D", "E"]]),
        ],
    )
    def test_main(
        self, *, iterable: Iterable[str], expected: Sequence[list[str]]
    ) -> None:
        assert list(chunked(iterable, 3)) == expected

    def test_odd(self) -> None:
        assert list(chunked("ABCDE", 3)) == [["A", "B", "C"], ["D", "E"]]


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


class TestTake:
    def test_simple(self) -> None:
        result = take(5, range(10))
        expected = list(range(5))
        assert result == expected

    def test_null(self) -> None:
        result = take(0, range(10))
        expected = []
        assert result == expected

    def test_negative(self) -> None:
        with raises(
            ValueError,
            match=r"Indices for islice\(\) must be None or an integer: 0 <= x <= sys.maxsize\.",
        ):
            _ = take(-3, range(10))

    def test_too_much(self) -> None:
        result = take(10, range(5))
        expected = list(range(5))
        assert result == expected


class TestTranspose:
    @given(sequence=lists(tuples(integers()), min_size=1))
    def test_singles(self, *, sequence: Sequence[tuple[int]]) -> None:
        result = transpose(sequence)
        assert isinstance(result, tuple)
        for list_i in result:
            assert isinstance(list_i, list)
            assert len(list_i) == len(sequence)
        (first,) = result
        assert is_sequence_of(first, int)
        zipped = list(zip(*result, strict=True))
        assert zipped == sequence

    @given(sequence=lists(tuples(integers(), text_ascii()), min_size=1))
    def test_pairs(self, *, sequence: Sequence[tuple[int, str]]) -> None:
        result = transpose(sequence)
        assert isinstance(result, tuple)
        for list_i in result:
            assert isinstance(list_i, list)
            assert len(list_i) == len(sequence)
        first, second = result
        assert is_sequence_of(first, int)
        assert is_sequence_of(second, str)
        zipped = list(zip(*result, strict=True))
        assert zipped == sequence

    @given(sequence=lists(tuples(integers(), text_ascii(), integers()), min_size=1))
    def test_triples(self, *, sequence: Sequence[tuple[int, str, int]]) -> None:
        result = transpose(sequence)
        assert isinstance(result, tuple)
        for list_i in result:
            assert isinstance(list_i, list)
            assert len(list_i) == len(sequence)
        first, second, third = result
        assert is_sequence_of(first, int)
        assert is_sequence_of(second, str)
        assert is_sequence_of(third, int)
        zipped = list(zip(*result, strict=True))
        assert zipped == sequence

    @given(
        sequence=lists(
            tuples(integers(), text_ascii(), integers(), text_ascii()), min_size=1
        )
    )
    def test_quadruples(self, *, sequence: Sequence[tuple[int, str, int, str]]) -> None:
        result = transpose(sequence)
        assert isinstance(result, tuple)
        for list_i in result:
            assert isinstance(list_i, list)
            assert len(list_i) == len(sequence)
        first, second, third, fourth = result
        assert is_sequence_of(first, int)
        assert is_sequence_of(second, str)
        assert is_sequence_of(third, int)
        assert is_sequence_of(fourth, str)
        zipped = list(zip(*result, strict=True))
        assert zipped == sequence

    @given(
        sequence=lists(
            tuples(integers(), text_ascii(), integers(), text_ascii(), integers()),
            min_size=1,
        )
    )
    def test_quintuples(
        self, *, sequence: Sequence[tuple[int, str, int, str, int]]
    ) -> None:
        result = transpose(sequence)
        assert isinstance(result, tuple)
        for list_i in result:
            assert isinstance(list_i, list)
            assert len(list_i) == len(sequence)
        first, second, third, fourth, fifth = result
        assert is_sequence_of(first, int)
        assert is_sequence_of(second, str)
        assert is_sequence_of(third, int)
        assert is_sequence_of(fourth, str)
        assert is_sequence_of(fifth, int)
        zipped = list(zip(*result, strict=True))
        assert zipped == sequence


class TestUniqueEverseen:
    text: ClassVar[str] = "AAAABBBCCDAABBB"
    expected: ClassVar[list[str]] = ["A", "B", "C", "D"]

    def test_main(self) -> None:
        result = list(unique_everseen("AAAABBBCCDAABBB"))
        assert result == self.expected

    def test_key(self) -> None:
        result = list(unique_everseen("ABBCcAD", key=str.lower))
        assert result == self.expected

    def test_non_hashable(self) -> None:
        result = list(unique_everseen([[1, 2], [2, 3], [1, 2]]))
        expected = [[1, 2], [2, 3]]
        assert result == expected
