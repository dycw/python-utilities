from __future__ import annotations

import re
from dataclasses import dataclass
from functools import cmp_to_key
from itertools import chain, repeat
from math import isfinite, isinf, isnan, nan
from operator import neg, sub
from re import DOTALL
from typing import TYPE_CHECKING, Any, ClassVar

from hypothesis import given
from hypothesis.strategies import (
    DataObject,
    booleans,
    data,
    dictionaries,
    floats,
    frozensets,
    integers,
    lists,
    none,
    permutations,
    sampled_from,
    sets,
)
from pytest import mark, param, raises

from tests.test_objects.objects import objects
from utilities.constants import sentinel
from utilities.hypothesis import pairs, sets_fixed_length, text_ascii
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
    MergeStrMappingsError,
    ResolveIncludeAndExcludeError,
    SortIterableError,
    _ApplyBijectionDuplicateKeysError,
    _ApplyBijectionDuplicateValuesError,
    _CheckUniqueModuloCaseDuplicateLowerCaseStringsError,
    _CheckUniqueModuloCaseDuplicateStringsError,
    _sort_iterable_cmp_floats,
    apply_bijection,
    apply_to_tuple,
    apply_to_varargs,
    chain_mappings,
    chain_nullable,
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
    check_unique_modulo_case,
    cmp_nullable,
    ensure_iterable,
    ensure_iterable_not_str,
    enumerate_with_edge,
    expanding_window,
    filter_include_and_exclude,
    groupby_lists,
    hashable_to_iterable,
    is_iterable,
    is_iterable_not_str,
    map_mapping,
    merge_mappings,
    merge_sets,
    merge_str_mappings,
    resolve_include_and_exclude,
    sort_iterable,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

    from utilities.types import StrMapping


class TestApplyBijection:
    @given(text=text_ascii())
    def test_main(self, *, text: str) -> None:
        result = apply_bijection(str.upper, [text])
        expected = {text: text.upper()}
        assert result == expected

    @given(text=text_ascii(min_size=1))
    def test_error_duplicate_keys(self, *, text: str) -> None:
        with raises(
            _ApplyBijectionDuplicateKeysError,
            match=re.compile(
                "Keys .* must not contain duplicates; got .*", flags=DOTALL
            ),
        ):
            _ = apply_bijection(str.upper, [text, text])

    @given(text=text_ascii(min_size=1))
    def test_error_duplicate_values(self, *, text: str) -> None:
        with raises(
            _ApplyBijectionDuplicateValuesError,
            match=re.compile(
                "Values .* must not contain duplicates; got .*", flags=DOTALL
            ),
        ):
            _ = apply_bijection(str.upper, [text.lower(), text.upper()])


class TestApplyToTuple:
    @given(x=integers(), y=integers())
    def test_main(self, *, x: int, y: int) -> None:
        result = apply_to_tuple(sub, (x, y))
        expected = x - y
        assert result == expected


class TestApplyToVarArgs:
    @given(x=integers(), y=integers())
    def test_main(self, *, x: int, y: int) -> None:
        result = apply_to_varargs(sub, x, y)
        expected = x - y
        assert result == expected


class TestChainMappings:
    @given(mappings=lists(dictionaries(text_ascii(), integers())), list_=booleans())
    def test_main(self, *, mappings: Sequence[Mapping[str, int]], list_: bool) -> None:
        result = chain_mappings(*mappings, list=list_)
        expected = {}
        for mapping in mappings:
            for key, value in mapping.items():
                expected[key] = list(chain(expected.get(key, []), [value]))
        if list_:
            assert result == expected
        else:
            assert set(result) == set(expected)


class TestChainNullable:
    @given(values=lists(lists(integers() | none()) | none()))
    def test_main(self, *, values: list[list[int | None] | None]) -> None:
        result = list(chain_nullable(*values))
        expected = []
        for val in values:
            if val is not None:
                expected.extend(v for v in val if v is not None)
        assert result == expected


class TestCheckBijection:
    @given(data=data(), n=integers(0, 10))
    def test_main(self, *, data: DataObject, n: int) -> None:
        keys = data.draw(sets_fixed_length(integers(0, 100), n))
        values = data.draw(sets_fixed_length(integers(0, 100), n))
        mapping = dict(zip(keys, values, strict=True))
        check_bijection(mapping)

    def test_error(self) -> None:
        with raises(
            CheckBijectionError,
            match=r"Mapping .* must be a bijection; got duplicates {None: 2}",
        ):
            check_bijection({True: None, False: None})


class TestCheckDuplicates:
    @given(x=sets(integers()))
    def test_main(self, *, x: set[int]) -> None:
        check_duplicates(x)

    def test_error(self) -> None:
        with raises(
            CheckDuplicatesError,
            match=r"Iterable .* must not contain duplicates; got {None: 2}",
        ):
            check_duplicates([None, None])


class TestCheckIterablesEqual:
    def test_pass(self) -> None:
        check_iterables_equal([], [])

    def test_error_differing_items_and_left_longer(self) -> None:
        with raises(
            CheckIterablesEqualError,
            match=r"Iterables .* and .* must be equal; differing items were .* and left was longer",
        ):
            check_iterables_equal([1, 2, 3], [9])

    def test_error_differing_items_and_right_longer(self) -> None:
        with raises(
            CheckIterablesEqualError,
            match=r"Iterables .* and .* must be equal; differing items were .* and right was longer",
        ):
            check_iterables_equal([9], [1, 2, 3])

    def test_error_differing_items_and_same_length(self) -> None:
        with raises(
            CheckIterablesEqualError,
            match=r"Iterables .* and .* must be equal; differing items were .*",
        ):
            check_iterables_equal([1, 2, 3], [1, 2, 9])

    def test_error_no_differing_items_just_left_longer(self) -> None:
        with raises(
            CheckIterablesEqualError,
            match=r"Iterables .* and .* must be equal; left was longer",
        ):
            check_iterables_equal([1, 2, 3], [1])

    def test_error_no_differing_items_just_right_longer(self) -> None:
        with raises(
            CheckIterablesEqualError,
            match=r"Iterables .* and .* must be equal; right was longer",
        ):
            check_iterables_equal([1], [1, 2, 3])


class TestCheckLength:
    def test_equal_pass(self) -> None:
        check_length([], equal=0)

    def test_equal_fail(self) -> None:
        with raises(CheckLengthError, match=r"Object .* must have length .*; got .*"):
            check_length([], equal=1)

    @mark.parametrize("equal_or_approx", [param(10), param((11, 0.1))])
    def test_equal_or_approx_pass(
        self, *, equal_or_approx: int | tuple[int, float]
    ) -> None:
        check_length(range(10), equal_or_approx=equal_or_approx)

    @mark.parametrize(
        ("equal_or_approx", "match"),
        [
            param(10, "Object .* must have length .*; got .*"),
            param(
                (11, 0.1),
                r"Object .* must have approximate length .* \(error .*\); got .*",
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
            CheckLengthError, match=r"Object .* must have minimum length .*; got .*"
        ):
            check_length([], min=1)

    def test_max_pass(self) -> None:
        check_length([], max=0)

    def test_max_error(self) -> None:
        with raises(
            CheckLengthError, match=r"Object .* must have maximum length .*; got .*"
        ):
            check_length([1], max=0)


class TestCheckLengthsEqual:
    def test_pass(self) -> None:
        check_lengths_equal([], [])

    def test_error(self) -> None:
        with raises(
            CheckLengthsEqualError,
            match=r"Sized objects .* and .* must have the same length; got .* and .*",
        ):
            check_lengths_equal([], [1, 2, 3])


class TestCheckMappingsEqual:
    def test_pass(self) -> None:
        check_mappings_equal({}, {})

    def test_error_extra_and_missing_and_differing_values(self) -> None:
        with raises(
            CheckMappingsEqualError,
            match=r"Mappings .* and .* must be equal; left had extra keys .*, right had extra keys .* and differing values were .*",
        ):
            check_mappings_equal({"a": 1, "b": 2, "c": 3}, {"b": 2, "c": 9, "d": 4})

    def test_error_extra_and_missing(self) -> None:
        with raises(
            CheckMappingsEqualError,
            match=r"Mappings .* and .* must be equal; left had extra keys .* and right had extra keys .*",
        ):
            check_mappings_equal({"a": 1, "b": 2, "c": 3}, {"b": 2, "c": 3, "d": 4})

    def test_error_extra_and_differing_values(self) -> None:
        with raises(
            CheckMappingsEqualError,
            match=r"Mappings .* and .* must be equal; left had extra keys .* and differing values were .*",
        ):
            check_mappings_equal({"a": 1, "b": 2, "c": 3}, {"a": 9})

    def test_error_missing_and_differing_values(self) -> None:
        with raises(
            CheckMappingsEqualError,
            match=r"Mappings .* and .* must be equal; right had extra keys .* and differing values were .*",
        ):
            check_mappings_equal({"a": 1}, {"a": 9, "b": 2, "c": 3})

    def test_error_extra_only(self) -> None:
        with raises(
            CheckMappingsEqualError,
            match=r"Mappings .* and .* must be equal; left had extra keys .*",
        ):
            check_mappings_equal({"a": 1, "b": 2, "c": 3}, {"a": 1})

    def test_error_missing_only(self) -> None:
        with raises(
            CheckMappingsEqualError,
            match=r"Mappings .* and .* must be equal; right had extra keys .*",
        ):
            check_mappings_equal({"a": 1}, {"a": 1, "b": 2, "c": 3})

    def test_error_differing_values_only(self) -> None:
        with raises(
            CheckMappingsEqualError,
            match=r"Mappings .* and .* must be equal; differing values were .*",
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
            match=r"Sets .* and .* must be equal; left had extra items .* and right had extra items .*",
        ):
            check_sets_equal({1, 2, 3}, {2, 3, 4})

    def test_error_extra(self) -> None:
        with raises(
            CheckSetsEqualError,
            match=r"Sets .* and .* must be equal; left had extra items .*",
        ):
            check_sets_equal({1, 2, 3}, set())

    def test_error_missing(self) -> None:
        with raises(
            CheckSetsEqualError,
            match=r"Sets .* and .* must be equal; right had extra items .*",
        ):
            check_sets_equal(set(), {1, 2, 3})


class TestCheckSubMapping:
    def test_pass(self) -> None:
        check_submapping({}, {})

    def test_error_extra_and_differing_values(self) -> None:
        with raises(
            CheckSubMappingError,
            match=r"Mapping .* must be a submapping of .*; left had extra keys .* and differing values were .*",
        ):
            check_submapping({"a": 1, "b": 2, "c": 3}, {"a": 9})

    def test_error_extra_only(self) -> None:
        with raises(
            CheckSubMappingError,
            match=r"Mapping .* must be a submapping of .*; left had extra keys .*",
        ):
            check_submapping({"a": 1, "b": 2, "c": 3}, {"a": 1})

    def test_error_differing_values_only(self) -> None:
        with raises(
            CheckSubMappingError,
            match=r"Mapping .* must be a submapping of .*; differing values were .*",
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
            match=r"Set .* must be a subset of .*; left had extra items .*",
        ):
            check_subset({1, 2, 3}, {1})


class TestCheckSuperMapping:
    def test_pass(self) -> None:
        check_supermapping({}, {})

    def test_error_missing_and_differing_values(self) -> None:
        with raises(
            CheckSuperMappingError,
            match=r"Mapping .* must be a supermapping of .*; right had extra keys .* and differing values were .*",
        ):
            check_supermapping({"a": 1}, {"a": 9, "b": 2, "c": 3})

    def test_error_extra_only(self) -> None:
        with raises(
            CheckSuperMappingError,
            match=r"Mapping .* must be a supermapping of .*; right had extra keys .*",
        ):
            check_supermapping({"a": 1}, {"a": 1, "b": 2, "c": 3})

    def test_error_differing_values_only(self) -> None:
        with raises(
            CheckSuperMappingError,
            match=r"Mapping .* must be a supermapping of .*; differing values were .*",
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


class TestCheckUniqueModuloCase:
    @given(text=text_ascii())
    def test_main(self, *, text: str) -> None:
        _ = check_unique_modulo_case([text])

    @given(text=text_ascii(min_size=1))
    def test_error_duplicate_keys(self, *, text: str) -> None:
        with raises(
            _CheckUniqueModuloCaseDuplicateStringsError,
            match=re.compile(
                "Strings .* must not contain duplicates; got .*", flags=DOTALL
            ),
        ):
            _ = check_unique_modulo_case([text, text])

    @given(text=text_ascii(min_size=1))
    def test_error_duplicate_values(self, *, text: str) -> None:
        with raises(
            _CheckUniqueModuloCaseDuplicateLowerCaseStringsError,
            match=re.compile(
                r"Strings .* must not contain duplicates \(modulo case\); got .*",
                flags=DOTALL,
            ),
        ):
            _ = check_unique_modulo_case([text.lower(), text.upper()])


class TestCmpNullable:
    @given(
        data=data(),
        case=sampled_from([
            ([None, None], [None, None]),
            ([1, None], [None, 1]),
            ([1, None, None], [None, None, 1]),
            ([2, 1, None], [None, 1, 2]),
            ([2, 1, None, None], [None, None, 1, 2]),
        ]),
    )
    def test_main(
        self,
        *,
        data: DataObject,
        case: tuple[Sequence[int | None], Sequence[int | None]],
    ) -> None:
        values, expected = case
        result = sorted(data.draw(permutations(values)), key=cmp_to_key(cmp_nullable))
        assert result == expected


class TestEnsureIterable:
    @mark.parametrize("obj", [param([]), param(()), param("")])
    def test_main(self, *, obj: Any) -> None:
        _ = ensure_iterable(obj)

    def test_error(self) -> None:
        with raises(EnsureIterableError, match=r"Object .* must be iterable"):
            _ = ensure_iterable(None)


class TestEnsureIterableNotStr:
    @mark.parametrize("obj", [param([]), param(())])
    def test_main(self, *, obj: Any) -> None:
        _ = ensure_iterable_not_str(obj)

    @mark.parametrize("obj", [param(None), param("")])
    def test_error(self, *, obj: Any) -> None:
        with raises(
            EnsureIterableNotStrError,
            match=r"Object .* must be iterable, but not a string",
        ):
            _ = ensure_iterable_not_str(obj)


class TestEnumerateWithEdge:
    def test_main(self) -> None:
        result = list(enumerate_with_edge(range(100)))
        assert len(result) == 100
        for i, total, is_edge, _ in result:
            assert total == 100
            expected = (0 <= i <= 4) or (95 <= i <= 99)
            assert is_edge is expected

    def test_short(self) -> None:
        result = list(enumerate_with_edge(range(9)))
        assert len(result) == 9
        for _, total, is_edge, _ in result:
            assert total == 9
            assert is_edge


class TestExpandingWindow:
    @mark.parametrize(
        ("iterable", "expected"),
        [
            param(
                [1, 2, 3, 4, 5], [[1], [1, 2], [1, 2, 3], [1, 2, 3, 4], [1, 2, 3, 4, 5]]
            ),
            param([], []),
        ],
    )
    def test_main(self, *, iterable: Iterable[int], expected: list[list[int]]) -> None:
        result = list(expanding_window(iterable))
        assert result == expected


class TestHashableToIterable:
    def test_none(self) -> None:
        result = hashable_to_iterable(None)
        expected = None
        assert result is expected

    @given(x=lists(integers()))
    def test_integers(self, *, x: int) -> None:
        result = hashable_to_iterable(x)
        expected = (x,)
        assert result == expected


class TestFilterIncludeAndExclude:
    def test_none(self) -> None:
        rng = list(range(5))
        result = list(filter_include_and_exclude(rng))
        assert result == rng

    def test_include_singleton(self) -> None:
        result = list(filter_include_and_exclude(range(5), include=0))
        expected = [0]
        assert result == expected

    def test_include_iterable(self) -> None:
        result = list(filter_include_and_exclude(range(5), include=[0, 1, 2]))
        expected = [0, 1, 2]
        assert result == expected

    def test_exclude_singleton(self) -> None:
        result = list(filter_include_and_exclude(range(5), exclude=0))
        expected = [1, 2, 3, 4]
        assert result == expected

    def test_exclude_iterable(self) -> None:
        result = list(filter_include_and_exclude(range(5), exclude=[0, 1, 2]))
        expected = [3, 4]
        assert result == expected

    def test_both(self) -> None:
        result = list(
            filter_include_and_exclude(range(5), include=[0, 1], exclude=[3, 4])
        )
        expected = [0, 1]
        assert result == expected

    def test_include_key(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            n: int

        result = list(
            filter_include_and_exclude(
                [Example(n=n) for n in range(5)], include=[0, 1, 2], key=lambda x: x.n
            )
        )
        expected = [Example(n=n) for n in [0, 1, 2]]
        assert result == expected

    def test_exclude_key(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            n: int

        result = list(
            filter_include_and_exclude(
                [Example(n=n) for n in range(5)], exclude=[0, 1, 2], key=lambda x: x.n
            )
        )
        expected = [Example(n=n) for n in [3, 4]]
        assert result == expected


class TestGroupbyLists:
    iterable: ClassVar[str] = "AAAABBBCCDAABB"

    def test_main(self) -> None:
        result = list(groupby_lists(self.iterable))
        expected = [
            ("A", list(repeat("A", times=4))),
            ("B", list(repeat("B", times=3))),
            ("C", list(repeat("C", times=2))),
            ("D", list(repeat("D", times=1))),
            ("A", list(repeat("A", times=2))),
            ("B", list(repeat("B", times=2))),
        ]
        assert result == expected

    def test_key(self) -> None:
        result = list(groupby_lists(self.iterable, key=ord))
        expected = [
            (65, list(repeat("A", times=4))),
            (66, list(repeat("B", times=3))),
            (67, list(repeat("C", times=2))),
            (68, list(repeat("D", times=1))),
            (65, list(repeat("A", times=2))),
            (66, list(repeat("B", times=2))),
        ]
        assert result == expected


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
        result = is_iterable_not_str(obj)
        assert result is expected


class TestMapMappings:
    @given(mapping=dictionaries(text_ascii(), integers()))
    def test_main(self, *, mapping: Mapping[str, int]) -> None:
        result = map_mapping(neg, mapping)
        expected = {k: -v for k, v in mapping.items()}
        assert result == expected


class TestMergeMappings:
    def test_main(self) -> None:
        mapping1 = {"x": 1, "y": 2}
        mapping2 = {"y": 3, "z": 4}
        result = merge_mappings(mapping1, mapping2)
        expected = {"x": 1, "y": 3, "z": 4}
        assert result == expected

    def test_empty(self) -> None:
        result = merge_str_mappings()
        expected = {}
        assert result == expected


class TestMergeSets:
    @given(data=data())
    def test_lists(self, *, data: DataObject) -> None:
        list1 = data.draw(permutations(["x", "y"]))
        list2 = data.draw(permutations(["y", "z"]))
        result = merge_sets(list1, list2)
        expected = {"x", "y", "z"}
        assert result == expected

    def test_sets(self) -> None:
        set1 = {"x", "y"}
        set2 = {"y", "z"}
        result = merge_sets(set1, set2)
        expected = {"x", "y", "z"}
        assert result == expected

    def test_empty(self) -> None:
        result = merge_sets()
        expected = set()
        assert result == expected


class TestMergeStrMappings:
    @given(
        case=sampled_from([
            (True, {"x": 1, "y": 2, "X": 3, "z": 4}),
            (False, {"y": 2, "X": 3, "z": 4}),
        ])
    )
    def test_main(self, *, case: tuple[bool, StrMapping]) -> None:
        mapping1 = {"x": 1, "y": 2}
        mapping2 = {"X": 3, "z": 4}
        case_sensitive, expected = case
        result = merge_str_mappings(mapping1, mapping2, case_sensitive=case_sensitive)
        assert result == expected

    @given(case_sensitive=booleans())
    def test_empty(self, *, case_sensitive: bool) -> None:
        result = merge_str_mappings(case_sensitive=case_sensitive)
        expected = {}
        assert result == expected

    def test_error(self) -> None:
        with raises(
            MergeStrMappingsError,
            match=r"Mapping .* keys must not contain duplicates \(modulo case\); got .*",
        ):
            _ = merge_str_mappings({"x": 1, "X": 2})


class TestResolveIncludeAndExclude:
    def test_none(self) -> None:
        include, exclude = resolve_include_and_exclude()
        assert include is None
        assert exclude is None

    def test_include_singleton(self) -> None:
        include, exclude = resolve_include_and_exclude(include=1)
        assert include == {1}
        assert exclude is None

    def test_include_iterable(self) -> None:
        include, exclude = resolve_include_and_exclude(include=[1, 2, 3])
        assert include == {1, 2, 3}
        assert exclude is None

    def test_exclude_singleton(self) -> None:
        include, exclude = resolve_include_and_exclude(exclude=1)
        assert include is None
        assert exclude == {1}

    def test_exclude_iterable(self) -> None:
        include, exclude = resolve_include_and_exclude(exclude=[1, 2, 3])
        assert include is None
        assert exclude == {1, 2, 3}

    def test_both(self) -> None:
        include, exclude = resolve_include_and_exclude(
            include=[1, 2, 3], exclude=[4, 5, 6]
        )
        assert include == {1, 2, 3}
        assert exclude == {4, 5, 6}

    def test_error(self) -> None:
        with raises(
            ResolveIncludeAndExcludeError,
            match=r"Iterables .* and .* must not overlap; got .*",
        ):
            _ = resolve_include_and_exclude(include=[1, 2, 3], exclude=[3, 4, 5])


class TestSortIterable:
    @given(objs=pairs(objects(floats_allow_nan=False, sortable=True)))
    def test_main(self, *, objs: tuple[Any, Any]) -> None:
        x, y = objs
        result1 = sort_iterable([x, y])
        result2 = sort_iterable([y, x])
        assert result1 == result2

    @given(x=floats(), y=floats())
    def test_floats(self, *, x: float, y: float) -> None:
        result1 = sort_iterable([x, y])
        result2 = sort_iterable([y, x])
        for i, j in zip(result1, result2, strict=True):
            assert isfinite(i) is isfinite(j)
            assert isinf(i) is isinf(j)
            assert isnan(i) is isnan(j)

    @given(
        x=dictionaries(integers(), integers()), y=dictionaries(integers(), integers())
    )
    def test_mappings(self, *, x: Mapping[int, int], y: Mapping[int, int]) -> None:
        result1 = sort_iterable([x, y])
        result2 = sort_iterable([y, x])
        assert result1 == result2

    @given(x=text_ascii(), y=text_ascii())
    def test_strings(self, *, x: str, y: str) -> None:
        result1 = sort_iterable([x, y])
        result2 = sort_iterable([y, x])
        assert result1 == result2

    @given(x=frozensets(frozensets(integers())), y=frozensets(frozensets(integers())))
    def test_nested_frozensets(
        self, *, x: frozenset[frozenset[int]], y: frozenset[frozenset[int]]
    ) -> None:
        result1 = sort_iterable([x, y])
        result2 = sort_iterable([y, x])
        assert result1 == result2

    @given(data=data(), x=lists(none()))
    def test_nones(self, *, data: DataObject, x: list[None]) -> None:
        result1 = sort_iterable(x)
        result2 = sort_iterable(data.draw(permutations(result1)))
        assert result1 == result2

    def test_error(self) -> None:
        with raises(SortIterableError, match=r"Unable to sort .* and .*"):
            _ = sort_iterable([sentinel, sentinel])


class TestSortIterablesCmpFloats:
    @given(x=floats(), y=floats())
    def test_main(self, *, x: float, y: float) -> None:
        result1 = _sort_iterable_cmp_floats(x, y)
        result2 = _sort_iterable_cmp_floats(y, x)
        assert result1 == -result2
        if isfinite(x) is not isfinite(y):
            assert result1 != result2
        if isinf(x) is not isinf(y):
            assert result1 != result2
        if isnan(x) is not isnan(y):
            assert result1 != result2

    @given(x=floats(allow_nan=False))
    def test_num_vs_nan(self, *, x: float) -> None:
        result = _sort_iterable_cmp_floats(x, nan)
        assert result == -1

    @given(x=floats(allow_nan=False))
    def test_nan_vs_num(self, *, x: float) -> None:
        result = _sort_iterable_cmp_floats(nan, x)
        assert result == 1

    def test_nan_vs_nan(self) -> None:
        result = _sort_iterable_cmp_floats(nan, nan)
        assert result == 0
