from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from hypothesis import given
from hypothesis.strategies import DataObject, data, integers, sampled_from, sets
from pytest import mark, param, raises

from utilities.iterables import (
    CheckBijectionError,
    CheckDuplicatesError,
    CheckIterablesEqualError,
    CheckLengthError,
    CheckLengthsEqualError,
    CheckMappingsEqualError,
    CheckSetsEqualError,
    CheckSubMappingError,
    CheckSubSetError,
    CheckSuperMappingError,
    CheckSuperSetError,
    EnsureIterableError,
    EnsureIterableNotStrError,
    OneEmptyError,
    OneNonUniqueError,
    OneStrError,
    check_bijection,
    check_duplicates,
    check_iterables_equal,
    check_length,
    check_lengths_equal,
    check_mappings_equal,
    check_sets_equal,
    check_submapping,
    check_subset,
    check_supermapping,
    check_superset,
    chunked,
    ensure_hashables,
    ensure_iterable,
    ensure_iterable_not_str,
    is_iterable,
    is_iterable_not_str,
    one,
    one_str,
    product_dicts,
    take,
    transpose,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence


class TestCheckBijection:
    @given(data=data(), n=integers(0, 10))
    def test_main(self, *, data: DataObject, n: int) -> None:
        keys = data.draw(sets(integers(0, 100), min_size=n, max_size=n))
        values = data.draw(sets(integers(0, 100), min_size=n, max_size=n))
        mapping = dict(zip(keys, values, strict=True))
        check_bijection(mapping)

    def test_error(self) -> None:
        with raises(
            CheckBijectionError,
            match=r"Mapping .* must be a bijection; got duplicates \(.*, n=2\)\.",
        ):
            check_bijection({True: None, False: None})


class TestCheckDuplicates:
    @given(x=sets(integers()))
    def test_main(self, *, x: set[int]) -> None:
        check_duplicates(x)

    def test_error(self) -> None:
        with raises(
            CheckDuplicatesError,
            match=r"Iterable .* must not contain duplicates; got {None: 2}\.",
        ):
            check_duplicates([None, None])


class TestCheckIterablesEqual:
    def test_pass(self) -> None:
        check_iterables_equal([], [])

    def test_error_differing_items_and_left_longer(self) -> None:
        with raises(
            CheckIterablesEqualError,
            match=r"Iterables .* and .* must be equal; differing items were \(.*, .*, i=.*\) and left was longer\.",
        ):
            check_iterables_equal([1, 2, 3], [9])

    def test_error_differing_items_and_right_longer(self) -> None:
        with raises(
            CheckIterablesEqualError,
            match=r"Iterables .* and .* must be equal; differing items were \(.*, .*, i=.*\) and right was longer\.",
        ):
            check_iterables_equal([9], [1, 2, 3])

    def test_error_differing_items_and_same_length(self) -> None:
        with raises(
            CheckIterablesEqualError,
            match=r"Iterables .* and .* must be equal; differing items were \(.*, .*, i=.*\)\.",
        ):
            check_iterables_equal([1, 2, 3], [1, 2, 9])

    def test_error_no_differing_items_just_left_longer(self) -> None:
        with raises(
            CheckIterablesEqualError,
            match=r"Iterables .* and .* must be equal; left was longer\.",
        ):
            check_iterables_equal([1, 2, 3], [1])

    def test_error_no_differing_items_just_right_longer(self) -> None:
        with raises(
            CheckIterablesEqualError,
            match=r"Iterables .* and .* must be equal; right was longer\.",
        ):
            check_iterables_equal([1], [1, 2, 3])


class TestCheckLength:
    def test_equal_pass(self) -> None:
        check_length([], equal=0)

    def test_equal_fail(self) -> None:
        with raises(CheckLengthError, match=r"Object .* must have length .*; got .*\."):
            check_length([], equal=1)

    @mark.parametrize("equal_or_approx", [param(10), param((11, 0.1))])
    def test_equal_or_approx_pass(
        self, *, equal_or_approx: int | tuple[int, float]
    ) -> None:
        check_length(range(10), equal_or_approx=equal_or_approx)

    @mark.parametrize(
        ("equal_or_approx", "match"),
        [
            param(10, r"Object .* must have length .*; got .*\."),
            param(
                (11, 0.1),
                r"Object .* must have approximate length .* \(error .*\); got .*\.",
            ),
        ],
    )
    def test_equal_or_approx_fail(
        self, *, equal_or_approx: int | tuple[int, float], match: str
    ) -> None:
        with raises(CheckLengthError, match=match):
            check_length([], equal_or_approx=equal_or_approx)

    def test_min_pass(self) -> None:
        check_length([], min=0)

    def test_min_error(self) -> None:
        with raises(
            CheckLengthError, match=r"Object .* must have minimum length .*; got .*\."
        ):
            check_length([], min=1)

    def test_max_pass(self) -> None:
        check_length([], max=0)

    def test_max_error(self) -> None:
        with raises(
            CheckLengthError, match=r"Object .* must have maximum length .*; got .*\."
        ):
            check_length([1], max=0)


class TestCheckLengthsEqual:
    def test_pass(self) -> None:
        check_lengths_equal([], [])

    def test_error(self) -> None:
        with raises(
            CheckLengthsEqualError,
            match=r"Sized objects .* and .* must have the same length; got .* and .*\.",
        ):
            check_lengths_equal([], [1, 2, 3])


class TestCheckMappingsEqual:
    def test_pass(self) -> None:
        check_mappings_equal({}, {})

    def test_error_extra_and_missing_and_differing_values(self) -> None:
        with raises(
            CheckMappingsEqualError,
            match=r"Mappings .* and .* must be equal; left had extra keys .*, right had extra keys .* and differing values were \(.*, .*, k=.*\)\.",
        ):
            check_mappings_equal({"a": 1, "b": 2, "c": 3}, {"b": 2, "c": 9, "d": 4})

    def test_error_extra_and_missing(self) -> None:
        with raises(
            CheckMappingsEqualError,
            match=r"Mappings .* and .* must be equal; left had extra keys .* and right had extra keys .*\.",
        ):
            check_mappings_equal({"a": 1, "b": 2, "c": 3}, {"b": 2, "c": 3, "d": 4})

    def test_error_extra_and_differing_values(self) -> None:
        with raises(
            CheckMappingsEqualError,
            match=r"Mappings .* and .* must be equal; left had extra keys .* and differing values were \(.*, .*, k=.*\)\.",
        ):
            check_mappings_equal({"a": 1, "b": 2, "c": 3}, {"a": 9})

    def test_error_missing_and_differing_values(self) -> None:
        with raises(
            CheckMappingsEqualError,
            match=r"Mappings .* and .* must be equal; right had extra keys .* and differing values were \(.*, .*, k=.*\)\.",
        ):
            check_mappings_equal({"a": 1}, {"a": 9, "b": 2, "c": 3})

    def test_error_extra_only(self) -> None:
        with raises(
            CheckMappingsEqualError,
            match=r"Mappings .* and .* must be equal; left had extra keys .*\.",
        ):
            check_mappings_equal({"a": 1, "b": 2, "c": 3}, {"a": 1})

    def test_error_missing_only(self) -> None:
        with raises(
            CheckMappingsEqualError,
            match=r"Mappings .* and .* must be equal; right had extra keys .*\.",
        ):
            check_mappings_equal({"a": 1}, {"a": 1, "b": 2, "c": 3})

    def test_error_differing_values_only(self) -> None:
        with raises(
            CheckMappingsEqualError,
            match=r"Mappings .* and .* must be equal; differing values were \(.*, .*, k=.*\)\.",
        ):
            check_mappings_equal({"a": 1, "b": 2, "c": 3}, {"a": 1, "b": 2, "c": 9})


class TestCheckSetsEqual:
    @mark.parametrize(
        ("left", "right"), [param(set(), set()), param(iter([]), iter([]))]
    )
    def test_pass(self, *, left: Iterable[Any], right: Iterable[Any]) -> None:
        check_sets_equal(left, right)

    def test_error_extra_and_missing(self) -> None:
        with raises(
            CheckSetsEqualError,
            match=r"Sets .* and .* must be equal; left had extra items .* and right had extra items .*\.",
        ):
            check_sets_equal({1, 2, 3}, {2, 3, 4})

    def test_error_extra(self) -> None:
        with raises(
            CheckSetsEqualError,
            match=r"Sets .* and .* must be equal; left had extra items .*\.",
        ):
            check_sets_equal({1, 2, 3}, set())

    def test_error_missing(self) -> None:
        with raises(
            CheckSetsEqualError,
            match=r"Sets .* and .* must be equal; right had extra items .*\.",
        ):
            check_sets_equal(set(), {1, 2, 3})


class TestCheckSubMapping:
    def test_pass(self) -> None:
        check_submapping({}, {})

    def test_error_extra_and_differing_values(self) -> None:
        with raises(
            CheckSubMappingError,
            match=r"Mapping .* must be a submapping of .*; left had extra keys .* and differing values were \(.*, .*., k=.*\)\.",
        ):
            check_submapping({"a": 1, "b": 2, "c": 3}, {"a": 9})

    def test_error_extra_only(self) -> None:
        with raises(
            CheckSubMappingError,
            match=r"Mapping .* must be a submapping of .*; left had extra keys .*\.",
        ):
            check_submapping({"a": 1, "b": 2, "c": 3}, {"a": 1})

    def test_error_differing_values_only(self) -> None:
        with raises(
            CheckSubMappingError,
            match=r"Mapping .* must be a submapping of .*; differing values were \(.*, .*, k=.*\)\.",
        ):
            check_submapping({"a": 1, "b": 2, "c": 3}, {"a": 1, "b": 2, "c": 9})


class TestCheckSubSet:
    @mark.parametrize(
        ("left", "right"), [param(set(), set()), param(iter([]), iter([]))]
    )
    def test_pass(self, *, left: Iterable[Any], right: Iterable[Any]) -> None:
        check_subset(left, right)

    def test_error(self) -> None:
        with raises(
            CheckSubSetError,
            match=r"Set .* must be a subset of .*; left had extra items .*\.",
        ):
            check_subset({1, 2, 3}, {1})


class TestCheckSuperMapping:
    def test_pass(self) -> None:
        check_supermapping({}, {})

    def test_error_missing_and_differing_values(self) -> None:
        with raises(
            CheckSuperMappingError,
            match=r"Mapping .* must be a supermapping of .*; right had extra keys .* and differing values were \(.*, .*, k=.*\)\.",
        ):
            check_supermapping({"a": 1}, {"a": 9, "b": 2, "c": 3})

    def test_error_extra_only(self) -> None:
        with raises(
            CheckSuperMappingError,
            match=r"Mapping .* must be a supermapping of .*; right had extra keys .*\.",
        ):
            check_supermapping({"a": 1}, {"a": 1, "b": 2, "c": 3})

    def test_error_differing_values_only(self) -> None:
        with raises(
            CheckSuperMappingError,
            match=r"Mapping .* must be a supermapping of .*; differing values were \(.*, .*, k=.*\)\.",
        ):
            check_supermapping({"a": 1, "b": 2, "c": 3}, {"a": 1, "b": 2, "c": 9})


class TestCheckSuperSet:
    @mark.parametrize(
        ("left", "right"), [param(set(), set()), param(iter([]), iter([]))]
    )
    def test_pass(self, *, left: Iterable[Any], right: Iterable[Any]) -> None:
        check_superset(left, right)

    def test_error(self) -> None:
        with raises(
            CheckSuperSetError,
            match=r"Set .* must be a superset of .*; right had extra items .*\.",
        ):
            check_superset({1}, {1, 2, 3})


class TestChunked:
    @mark.parametrize(
        ("iterable", "expected"),
        [
            param("ABCDEF", [["A", "B", "C"], ["D", "E", "F"]]),
            param("ABCDE", [["A", "B", "C"], ["D", "E"]]),
        ],
    )
    def test_main(
        self, *, iterable: Iterable[str], expected: Sequence[Sequence[str]]
    ) -> None:
        result = list(chunked(iterable, 3))
        assert result == expected

    def test_odd(self) -> None:
        result = list(chunked("ABCDE", 3))
        expected = [["A", "B", "C"], ["D", "E"]]
        assert result == expected


class TestEnsureHashables:
    def test_main(self) -> None:
        assert ensure_hashables(1, 2, a=3, b=4) == ([1, 2], {"a": 3, "b": 4})


class TestEnsureIterable:
    @mark.parametrize("obj", [param([]), param(()), param("")])
    def test_main(self, *, obj: Any) -> None:
        _ = ensure_iterable(obj)

    def test_error(self) -> None:
        with raises(EnsureIterableError, match=r"Object .* must be iterable\."):
            _ = ensure_iterable(None)


class TestEnsureIterableNotStr:
    @mark.parametrize("obj", [param([]), param(())])
    def test_main(self, *, obj: Any) -> None:
        _ = ensure_iterable_not_str(obj)

    @mark.parametrize("obj", [param(None), param("")])
    def test_error(self, *, obj: Any) -> None:
        with raises(
            EnsureIterableNotStrError,
            match=r"Object .* must be iterable, but not a string\.",
        ):
            _ = ensure_iterable_not_str(obj)


class TestIsIterable:
    @mark.parametrize(
        ("obj", "expected"),
        [param(None, False), param([], True), param((), True), param("", True)],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        assert is_iterable(obj) is expected


class TestIsIterableNotStr:
    @mark.parametrize(
        ("obj", "expected"),
        [param(None, False), param([], True), param((), True), param("", False)],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        assert is_iterable_not_str(obj) is expected


class TestOne:
    def test_main(self) -> None:
        assert one([None]) is None

    def test_error_empty(self) -> None:
        with raises(OneEmptyError, match=r"Iterable .* must not be empty\."):
            _ = one([])

    def test_error_non_unique(self) -> None:
        with raises(
            OneNonUniqueError,
            match=r"Iterable .* must contain exactly one item; got .*, .* and perhaps more\.",
        ):
            _ = one([1, 2])


class TestOneStr:
    @given(text=sampled_from(["a", "b", "c"]))
    def test_case_sensitive(self, *, text: str) -> None:
        assert one_str(["a", "b", "c"], text) == text

    @given(text=sampled_from(["a", "b", "c"]), case=sampled_from(["upper", "lower"]))
    def test_case_insensitive(
        self, *, text: str, case: Literal["upper", "lower"]
    ) -> None:
        text_use = text if case == "lower" else text.upper()
        assert one_str(["a", "b", "c"], text_use, case_sensitive=False) == text

    def test_error_duplicates(self) -> None:
        with raises(
            OneStrError, match=r"Iterable .* must not contain duplicates; got {'a': 2}"
        ):
            _ = one_str(["a", "a"], "a")

    def test_error_case_sensitive_empty_error(self) -> None:
        with raises(OneStrError, match=r"Iterable .* does not contain 'd'"):
            _ = one_str(["a", "b", "c"], "d")

    def test_error_bijection_error(self) -> None:
        with raises(
            OneStrError,
            match=r"Iterable .* must not contain duplicates \(case insensitive\); got {'a': 2}",
        ):
            _ = one_str(["a", "A"], "a", case_sensitive=False)

    def test_error_case_insensitive_empty_error(self) -> None:
        with raises(
            OneStrError, match=r"Iterable .* does not contain 'd' \(case insensitive\)"
        ):
            _ = one_str(["a", "b", "c"], "d", case_sensitive=False)


class TestProductDicts:
    def test_main(self) -> None:
        mapping = {"x": [1, 2], "y": [7, 8, 9]}
        result = list(product_dicts(mapping))
        expected = [
            {"x": 1, "y": 7},
            {"x": 1, "y": 8},
            {"x": 1, "y": 9},
            {"x": 2, "y": 7},
            {"x": 2, "y": 8},
            {"x": 2, "y": 9},
        ]
        assert result == expected


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
    @given(n=integers(1, 10))
    def test_singles(self, *, n: int) -> None:
        iterable = ((i,) for i in range(n))
        result = transpose(iterable)
        assert isinstance(result, tuple)
        (first,) = result
        assert isinstance(first, tuple)
        assert len(first) == n
        for i in first:
            assert isinstance(i, int)

    @given(n=integers(1, 10))
    def test_pairs(self, *, n: int) -> None:
        iterable = ((i, i) for i in range(n))
        result = transpose(iterable)
        assert isinstance(result, tuple)
        first, second = result
        for part in [first, second]:
            assert len(part) == n
            for i in part:
                assert isinstance(i, int)

    @given(n=integers(1, 10))
    def test_triples(self, *, n: int) -> None:
        iterable = ((i, i, i) for i in range(n))
        result = transpose(iterable)
        assert isinstance(result, tuple)
        first, second, third = result
        for part in [first, second, third]:
            assert len(part) == n
            for i in part:
                assert isinstance(i, int)

    @given(n=integers(1, 10))
    def test_quadruples(self, *, n: int) -> None:
        iterable = ((i, i, i, i) for i in range(n))
        result = transpose(iterable)
        assert isinstance(result, tuple)
        first, second, third, fourth = result
        for part in [first, second, third, fourth]:
            assert len(part) == n
            for i in part:
                assert isinstance(i, int)

    @given(n=integers(1, 10))
    def test_quintuples(self, *, n: int) -> None:
        iterable = ((i, i, i, i, i) for i in range(n))
        result = transpose(iterable)
        assert isinstance(result, tuple)
        first, second, third, fourth, fifth = result
        for part in [first, second, third, fourth, fifth]:
            assert len(part) == n
            for i in part:
                assert isinstance(i, int)
