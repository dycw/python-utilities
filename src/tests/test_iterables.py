from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest
from hypothesis import given
from hypothesis.strategies import integers, sets

from utilities.iterables import (
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
    take,
    transpose,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence


class TestCheckDuplicates:
    @given(x=sets(integers()))
    def test_main(self, *, x: set[int]) -> None:
        check_duplicates(x)

    def test_error(self) -> None:
        with pytest.raises(
            CheckDuplicatesError,
            match=r"Iterable .* must not contain duplicates; got \(.*, n=2\)\.",
        ):
            check_duplicates([None, None])


class TestCheckIterablesEqual:
    def test_pass(self) -> None:
        check_iterables_equal([], [])

    def test_error_differing_items_and_left_longer(self) -> None:
        with pytest.raises(
            CheckIterablesEqualError,
            match=r"Iterables .* and .* must be equal; differing items were \(.*, .*, i=.*\) and left was longer\.",
        ):
            check_iterables_equal([1, 2, 3], [9])

    def test_error_differing_items_and_right_longer(self) -> None:
        with pytest.raises(
            CheckIterablesEqualError,
            match=r"Iterables .* and .* must be equal; differing items were \(.*, .*, i=.*\) and right was longer\.",
        ):
            check_iterables_equal([9], [1, 2, 3])

    def test_error_differing_items_and_same_length(self) -> None:
        with pytest.raises(
            CheckIterablesEqualError,
            match=r"Iterables .* and .* must be equal; differing items were \(.*, .*, i=.*\)\.",
        ):
            check_iterables_equal([1, 2, 3], [1, 2, 9])

    def test_error_no_differing_items_just_left_longer(self) -> None:
        with pytest.raises(
            CheckIterablesEqualError,
            match=r"Iterables .* and .* must be equal; left was longer\.",
        ):
            check_iterables_equal([1, 2, 3], [1])

    def test_error_no_differing_items_just_right_longer(self) -> None:
        with pytest.raises(
            CheckIterablesEqualError,
            match=r"Iterables .* and .* must be equal; right was longer\.",
        ):
            check_iterables_equal([1], [1, 2, 3])


class TestCheckLength:
    def test_equal_pass(self) -> None:
        check_length([], equal=0)

    def test_equal_fail(self) -> None:
        with pytest.raises(
            CheckLengthError, match=r"Object .* must have length .*; got .*\."
        ):
            check_length([], equal=1)

    @pytest.mark.parametrize(
        "equal_or_approx", [pytest.param(10), pytest.param((11, 0.1))]
    )
    def test_equal_or_approx_pass(
        self, *, equal_or_approx: int | tuple[int, float]
    ) -> None:
        check_length(range(10), equal_or_approx=equal_or_approx)

    @pytest.mark.parametrize(
        ("equal_or_approx", "match"),
        [
            pytest.param(10, r"Object .* must have length .*; got .*\."),
            pytest.param(
                (11, 0.1),
                r"Object .* must have approximate length .* \(error .*\); got .*\.",
            ),
        ],
    )
    def test_equal_or_approx_fail(
        self, *, equal_or_approx: int | tuple[int, float], match: str
    ) -> None:
        with pytest.raises(CheckLengthError, match=match):
            check_length([], equal_or_approx=equal_or_approx)

    def test_min_pass(self) -> None:
        check_length([], min=0)

    def test_min_error(self) -> None:
        with pytest.raises(
            CheckLengthError, match=r"Object .* must have minimum length .*; got .*\."
        ):
            check_length([], min=1)

    def test_max_pass(self) -> None:
        check_length([], max=0)

    def test_max_error(self) -> None:
        with pytest.raises(
            CheckLengthError, match=r"Object .* must have maximum length .*; got .*\."
        ):
            check_length([1], max=0)


class TestCheckLengthsEqual:
    def test_pass(self) -> None:
        check_lengths_equal([], [])

    def test_error(self) -> None:
        with pytest.raises(
            CheckLengthsEqualError,
            match=r"Sized objects .* and .* must have the same length; got .* and .*\.",
        ):
            check_lengths_equal([], [1, 2, 3])


class TestCheckMappingsEqual:
    def test_pass(self) -> None:
        check_mappings_equal({}, {})

    def test_error_extra_and_missing_and_differing_values(self) -> None:
        with pytest.raises(
            CheckMappingsEqualError,
            match=r"Mappings .* and .* must be equal; left had extra keys .*, right had extra keys .* and differing values were \(.*, .*, k=.*\)\.",
        ):
            check_mappings_equal({"a": 1, "b": 2, "c": 3}, {"b": 2, "c": 9, "d": 4})

    def test_error_extra_and_missing(self) -> None:
        with pytest.raises(
            CheckMappingsEqualError,
            match=r"Mappings .* and .* must be equal; left had extra keys .* and right had extra keys .*\.",
        ):
            check_mappings_equal({"a": 1, "b": 2, "c": 3}, {"b": 2, "c": 3, "d": 4})

    def test_error_extra_and_differing_values(self) -> None:
        with pytest.raises(
            CheckMappingsEqualError,
            match=r"Mappings .* and .* must be equal; left had extra keys .* and differing values were \(.*, .*, k=.*\)\.",
        ):
            check_mappings_equal({"a": 1, "b": 2, "c": 3}, {"a": 9})

    def test_error_missing_and_differing_values(self) -> None:
        with pytest.raises(
            CheckMappingsEqualError,
            match=r"Mappings .* and .* must be equal; right had extra keys .* and differing values were \(.*, .*, k=.*\)\.",
        ):
            check_mappings_equal({"a": 1}, {"a": 9, "b": 2, "c": 3})

    def test_error_extra_only(self) -> None:
        with pytest.raises(
            CheckMappingsEqualError,
            match=r"Mappings .* and .* must be equal; left had extra keys .*\.",
        ):
            check_mappings_equal({"a": 1, "b": 2, "c": 3}, {"a": 1})

    def test_error_missing_only(self) -> None:
        with pytest.raises(
            CheckMappingsEqualError,
            match=r"Mappings .* and .* must be equal; right had extra keys .*\.",
        ):
            check_mappings_equal({"a": 1}, {"a": 1, "b": 2, "c": 3})

    def test_error_differing_values_only(self) -> None:
        with pytest.raises(
            CheckMappingsEqualError,
            match=r"Mappings .* and .* must be equal; differing values were \(.*, .*, k=.*\)\.",
        ):
            check_mappings_equal({"a": 1, "b": 2, "c": 3}, {"a": 1, "b": 2, "c": 9})


class TestCheckSetsEqual:
    @pytest.mark.parametrize(
        ("left", "right"),
        [pytest.param(set(), set()), pytest.param(iter([]), iter([]))],
    )
    def test_pass(self, *, left: Iterable[Any], right: Iterable[Any]) -> None:
        check_sets_equal(left, right)

    def test_error_extra_and_missing(self) -> None:
        with pytest.raises(
            CheckSetsEqualError,
            match=r"Sets .* and .* must be equal; left had extra items .* and right had extra items .*\.",
        ):
            check_sets_equal({1, 2, 3}, {2, 3, 4})

    def test_error_extra(self) -> None:
        with pytest.raises(
            CheckSetsEqualError,
            match=r"Sets .* and .* must be equal; left had extra items .*\.",
        ):
            check_sets_equal({1, 2, 3}, set())

    def test_error_missing(self) -> None:
        with pytest.raises(
            CheckSetsEqualError,
            match=r"Sets .* and .* must be equal; right had extra items .*\.",
        ):
            check_sets_equal(set(), {1, 2, 3})


class TestCheckSubMapping:
    def test_pass(self) -> None:
        check_submapping({}, {})

    def test_error_extra_and_differing_values(self) -> None:
        with pytest.raises(
            CheckSubMappingError,
            match=r"Mapping .* must be a submapping of .*; left had extra keys .* and differing values were \(.*, .*., k=.*\)\.",
        ):
            check_submapping({"a": 1, "b": 2, "c": 3}, {"a": 9})

    def test_error_extra_only(self) -> None:
        with pytest.raises(
            CheckSubMappingError,
            match=r"Mapping .* must be a submapping of .*; left had extra keys .*\.",
        ):
            check_submapping({"a": 1, "b": 2, "c": 3}, {"a": 1})

    def test_error_differing_values_only(self) -> None:
        with pytest.raises(
            CheckSubMappingError,
            match=r"Mapping .* must be a submapping of .*; differing values were \(.*, .*, k=.*\)\.",
        ):
            check_submapping({"a": 1, "b": 2, "c": 3}, {"a": 1, "b": 2, "c": 9})


class TestCheckSubSet:
    @pytest.mark.parametrize(
        ("left", "right"),
        [pytest.param(set(), set()), pytest.param(iter([]), iter([]))],
    )
    def test_pass(self, *, left: Iterable[Any], right: Iterable[Any]) -> None:
        check_subset(left, right)

    def test_error(self) -> None:
        with pytest.raises(
            CheckSubSetError,
            match=r"Set .* must be a subset of .*; left had extra items .*\.",
        ):
            check_subset({1, 2, 3}, {1})


class TestCheckSuperMapping:
    def test_pass(self) -> None:
        check_supermapping({}, {})

    def test_error_missing_and_differing_values(self) -> None:
        with pytest.raises(
            CheckSuperMappingError,
            match=r"Mapping .* must be a supermapping of .*; right had extra keys .* and differing values were \(.*, .*, k=.*\)\.",
        ):
            check_supermapping({"a": 1}, {"a": 9, "b": 2, "c": 3})

    def test_error_extra_only(self) -> None:
        with pytest.raises(
            CheckSuperMappingError,
            match=r"Mapping .* must be a supermapping of .*; right had extra keys .*\.",
        ):
            check_supermapping({"a": 1}, {"a": 1, "b": 2, "c": 3})

    def test_error_differing_values_only(self) -> None:
        with pytest.raises(
            CheckSuperMappingError,
            match=r"Mapping .* must be a supermapping of .*; differing values were \(.*, .*, k=.*\)\.",
        ):
            check_supermapping({"a": 1, "b": 2, "c": 3}, {"a": 1, "b": 2, "c": 9})


class TestCheckSuperSet:
    @pytest.mark.parametrize(
        ("left", "right"),
        [pytest.param(set(), set()), pytest.param(iter([]), iter([]))],
    )
    def test_pass(self, *, left: Iterable[Any], right: Iterable[Any]) -> None:
        check_superset(left, right)

    def test_error(self) -> None:
        with pytest.raises(
            CheckSuperSetError,
            match=r"Set .* must be a superset of .*; right had extra items .*\.",
        ):
            check_superset({1}, {1, 2, 3})


class TestChunked:
    @pytest.mark.parametrize(
        ("iterable", "expected"),
        [
            pytest.param("ABCDEF", [["A", "B", "C"], ["D", "E", "F"]]),
            pytest.param("ABCDE", [["A", "B", "C"], ["D", "E"]]),
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
    @pytest.mark.parametrize(
        "obj", [pytest.param([]), pytest.param(()), pytest.param("")]
    )
    def test_main(self, *, obj: Any) -> None:
        _ = ensure_iterable(obj)

    def test_error(self) -> None:
        with pytest.raises(EnsureIterableError, match=r"Object .* must be iterable\."):
            _ = ensure_iterable(None)


class TestEnsureIterableNotStr:
    @pytest.mark.parametrize("obj", [pytest.param([]), pytest.param(())])
    def test_main(self, *, obj: Any) -> None:
        _ = ensure_iterable_not_str(obj)

    @pytest.mark.parametrize("obj", [pytest.param(None), pytest.param("")])
    def test_error(self, *, obj: Any) -> None:
        with pytest.raises(
            EnsureIterableNotStrError,
            match=r"Object .* must be iterable, but not a string\.",
        ):
            _ = ensure_iterable_not_str(obj)


class TestIsIterable:
    @pytest.mark.parametrize(
        ("obj", "expected"),
        [
            pytest.param(None, False),
            pytest.param([], True),
            pytest.param((), True),
            pytest.param("", True),
        ],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        assert is_iterable(obj) is expected


class TestIsIterableNotStr:
    @pytest.mark.parametrize(
        ("obj", "expected"),
        [
            pytest.param(None, False),
            pytest.param([], True),
            pytest.param((), True),
            pytest.param("", False),
        ],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        assert is_iterable_not_str(obj) is expected


class TestOne:
    def test_main(self) -> None:
        assert one([None]) is None

    def test_error_empty(self) -> None:
        with pytest.raises(OneEmptyError, match=r"Iterable .* must not be empty\."):
            _ = one([])

    def test_error_non_unique(self) -> None:
        with pytest.raises(
            OneNonUniqueError,
            match=r"Iterable .* must contain exactly one item; got .*, .* and perhaps more\.",
        ):
            _ = one([1, 2])


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
        with pytest.raises(
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
