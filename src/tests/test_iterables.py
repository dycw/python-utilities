from __future__ import annotations

import re
from dataclasses import dataclass, replace
from enum import Enum, auto
from functools import cmp_to_key
from itertools import chain, repeat
from math import isfinite, isinf, isnan, nan
from operator import add, neg, sub
from re import DOTALL
from typing import TYPE_CHECKING, Any, ClassVar, override

from hypothesis import given
from hypothesis.strategies import (
    DataObject,
    binary,
    booleans,
    data,
    datetimes,
    dictionaries,
    floats,
    frozensets,
    integers,
    just,
    lists,
    none,
    permutations,
    sampled_from,
    sets,
    text,
    tuples,
)
from pytest import mark, param, raises

from tests.test_operator import make_objects
from utilities.functions import is_sequence_of
from utilities.hypothesis import (
    sentinels,
    sets_fixed_length,
    text_ascii,
    zoned_datetimes,
)
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
    Collection,
    EnsureIterableError,
    EnsureIterableNotStrError,
    MergeStrMappingsError,
    OneEmptyError,
    OneMaybeEmptyError,
    OneMaybeNonUniqueError,
    OneNonUniqueError,
    OneStrEmptyError,
    OneStrNonUniqueError,
    OneUniqueEmptyError,
    OneUniqueNonUniqueError,
    ResolveIncludeAndExcludeError,
    SortIterableError,
    _ApplyBijectionDuplicateKeysError,
    _ApplyBijectionDuplicateValuesError,
    _CheckUniqueModuloCaseDuplicateLowerCaseStringsError,
    _CheckUniqueModuloCaseDuplicateStringsError,
    _sort_iterable_cmp_datetimes,
    _sort_iterable_cmp_floats,
    always_iterable,
    always_iterable_hashable,
    apply_bijection,
    apply_to_tuple,
    apply_to_varargs,
    chain_mappings,
    chain_maybe_iterables,
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
    chunked,
    cmp_nullable,
    ensure_hashables,
    ensure_iterable,
    ensure_iterable_not_str,
    expanding_window,
    filter_include_and_exclude,
    group_consecutive_integers,
    groupby_lists,
    hashable_to_iterable,
    is_iterable,
    is_iterable_not_enum,
    is_iterable_not_str,
    map_mapping,
    merge_mappings,
    merge_sets,
    merge_str_mappings,
    one,
    one_maybe,
    one_str,
    one_unique,
    pairwise_tail,
    product_dicts,
    reduce_mappings,
    resolve_include_and_exclude,
    sort_iterable,
    sum_mappings,
    take,
    transpose,
    ungroup_consecutive_integers,
    unique_everseen,
)
from utilities.sentinel import Sentinel, sentinel

if TYPE_CHECKING:
    import datetime as dt
    from collections.abc import Iterable, Iterator, Mapping, Sequence

    from utilities.types import MaybeIterable, StrMapping


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


class TestAlwaysIterableHashable:
    def test_none(self) -> None:
        assert always_iterable_hashable(None) is None

    @given(x=integers())
    def test_integer(self, *, x: int) -> None:
        assert always_iterable_hashable(x) == (x,)

    @given(x=text())
    def test_string(self, *, x: str) -> None:
        assert always_iterable_hashable(x) == (x,)

    @given(x=lists(integers()))
    def test_list_of_integers(self, *, x: list[int]) -> None:
        assert always_iterable_hashable(x) == tuple(x)

    @given(x=lists(text()))
    def test_list_of_strings(self, *, x: list[str]) -> None:
        assert always_iterable_hashable(x) == tuple(x)


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


class TestChainMaybeIterables:
    @given(values=lists(integers() | lists(integers())))
    def test_main(self, *, values: list[int | list[int]]) -> None:
        result = list(chain_maybe_iterables(*values))
        expected = []
        for val in values:
            if isinstance(val, int):
                expected.append(val)
            else:
                expected.extend(v for v in val)
        assert result == expected


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
            match="Mapping .* must be a bijection; got duplicates {None: 2}",
        ):
            check_bijection({True: None, False: None})


class TestCheckDuplicates:
    @given(x=sets(integers()))
    def test_main(self, *, x: set[int]) -> None:
        check_duplicates(x)

    def test_error(self) -> None:
        with raises(
            CheckDuplicatesError,
            match="Iterable .* must not contain duplicates; got {None: 2}",
        ):
            check_duplicates([None, None])


class TestCheckIterablesEqual:
    def test_pass(self) -> None:
        check_iterables_equal([], [])

    def test_error_differing_items_and_left_longer(self) -> None:
        with raises(
            CheckIterablesEqualError,
            match="Iterables .* and .* must be equal; differing items were .* and left was longer",
        ):
            check_iterables_equal([1, 2, 3], [9])

    def test_error_differing_items_and_right_longer(self) -> None:
        with raises(
            CheckIterablesEqualError,
            match="Iterables .* and .* must be equal; differing items were .* and right was longer",
        ):
            check_iterables_equal([9], [1, 2, 3])

    def test_error_differing_items_and_same_length(self) -> None:
        with raises(
            CheckIterablesEqualError,
            match="Iterables .* and .* must be equal; differing items were .*",
        ):
            check_iterables_equal([1, 2, 3], [1, 2, 9])

    def test_error_no_differing_items_just_left_longer(self) -> None:
        with raises(
            CheckIterablesEqualError,
            match="Iterables .* and .* must be equal; left was longer",
        ):
            check_iterables_equal([1, 2, 3], [1])

    def test_error_no_differing_items_just_right_longer(self) -> None:
        with raises(
            CheckIterablesEqualError,
            match="Iterables .* and .* must be equal; right was longer",
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
            CheckLengthError, match="Object .* must have minimum length .*; got .*"
        ):
            check_length([], min=1)

    def test_max_pass(self) -> None:
        check_length([], max=0)

    def test_max_error(self) -> None:
        with raises(
            CheckLengthError, match="Object .* must have maximum length .*; got .*"
        ):
            check_length([1], max=0)


class TestCheckLengthsEqual:
    def test_pass(self) -> None:
        check_lengths_equal([], [])

    def test_error(self) -> None:
        with raises(
            CheckLengthsEqualError,
            match="Sized objects .* and .* must have the same length; got .* and .*",
        ):
            check_lengths_equal([], [1, 2, 3])


class TestCheckMappingsEqual:
    def test_pass(self) -> None:
        check_mappings_equal({}, {})

    def test_error_extra_and_missing_and_differing_values(self) -> None:
        with raises(
            CheckMappingsEqualError,
            match="Mappings .* and .* must be equal; left had extra keys .*, right had extra keys .* and differing values were .*",
        ):
            check_mappings_equal({"a": 1, "b": 2, "c": 3}, {"b": 2, "c": 9, "d": 4})

    def test_error_extra_and_missing(self) -> None:
        with raises(
            CheckMappingsEqualError,
            match="Mappings .* and .* must be equal; left had extra keys .* and right had extra keys .*",
        ):
            check_mappings_equal({"a": 1, "b": 2, "c": 3}, {"b": 2, "c": 3, "d": 4})

    def test_error_extra_and_differing_values(self) -> None:
        with raises(
            CheckMappingsEqualError,
            match="Mappings .* and .* must be equal; left had extra keys .* and differing values were .*",
        ):
            check_mappings_equal({"a": 1, "b": 2, "c": 3}, {"a": 9})

    def test_error_missing_and_differing_values(self) -> None:
        with raises(
            CheckMappingsEqualError,
            match="Mappings .* and .* must be equal; right had extra keys .* and differing values were .*",
        ):
            check_mappings_equal({"a": 1}, {"a": 9, "b": 2, "c": 3})

    def test_error_extra_only(self) -> None:
        with raises(
            CheckMappingsEqualError,
            match="Mappings .* and .* must be equal; left had extra keys .*",
        ):
            check_mappings_equal({"a": 1, "b": 2, "c": 3}, {"a": 1})

    def test_error_missing_only(self) -> None:
        with raises(
            CheckMappingsEqualError,
            match="Mappings .* and .* must be equal; right had extra keys .*",
        ):
            check_mappings_equal({"a": 1}, {"a": 1, "b": 2, "c": 3})

    def test_error_differing_values_only(self) -> None:
        with raises(
            CheckMappingsEqualError,
            match="Mappings .* and .* must be equal; differing values were .*",
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
            match="Sets .* and .* must be equal; left had extra items .* and right had extra items .*",
        ):
            check_sets_equal({1, 2, 3}, {2, 3, 4})

    def test_error_extra(self) -> None:
        with raises(
            CheckSetsEqualError,
            match="Sets .* and .* must be equal; left had extra items .*",
        ):
            check_sets_equal({1, 2, 3}, set())

    def test_error_missing(self) -> None:
        with raises(
            CheckSetsEqualError,
            match="Sets .* and .* must be equal; right had extra items .*",
        ):
            check_sets_equal(set(), {1, 2, 3})


class TestCheckSubMapping:
    def test_pass(self) -> None:
        check_submapping({}, {})

    def test_error_extra_and_differing_values(self) -> None:
        with raises(
            CheckSubMappingError,
            match="Mapping .* must be a submapping of .*; left had extra keys .* and differing values were .*",
        ):
            check_submapping({"a": 1, "b": 2, "c": 3}, {"a": 9})

    def test_error_extra_only(self) -> None:
        with raises(
            CheckSubMappingError,
            match="Mapping .* must be a submapping of .*; left had extra keys .*",
        ):
            check_submapping({"a": 1, "b": 2, "c": 3}, {"a": 1})

    def test_error_differing_values_only(self) -> None:
        with raises(
            CheckSubMappingError,
            match="Mapping .* must be a submapping of .*; differing values were .*",
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
            match="Set .* must be a subset of .*; left had extra items .*",
        ):
            check_subset({1, 2, 3}, {1})


class TestCheckSuperMapping:
    def test_pass(self) -> None:
        check_supermapping({}, {})

    def test_error_missing_and_differing_values(self) -> None:
        with raises(
            CheckSuperMappingError,
            match="Mapping .* must be a supermapping of .*; right had extra keys .* and differing values were .*",
        ):
            check_supermapping({"a": 1}, {"a": 9, "b": 2, "c": 3})

    def test_error_extra_only(self) -> None:
        with raises(
            CheckSuperMappingError,
            match="Mapping .* must be a supermapping of .*; right had extra keys .*",
        ):
            check_supermapping({"a": 1}, {"a": 1, "b": 2, "c": 3})

    def test_error_differing_values_only(self) -> None:
        with raises(
            CheckSuperMappingError,
            match="Mapping .* must be a supermapping of .*; differing values were .*",
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


@dataclass(unsafe_hash=True, slots=True)
class _Item:
    n: int


class TestCollection:
    def test_and_singleton(self) -> None:
        collection = Collection(map(_Item, range(3)))
        result = collection & _Item(1)
        assert isinstance(result, Collection)
        expected = Collection(_Item(1))
        assert result == expected

    def test_and_collection(self) -> None:
        collection = Collection(map(_Item, range(3)))
        result = collection & Collection(_Item(1))
        assert isinstance(result, Collection)
        expected = Collection(_Item(1))
        assert result == expected

    def test_and_iterable(self) -> None:
        collection = Collection(map(_Item, range(3)))
        result = collection & [_Item(1)]
        assert isinstance(result, Collection)
        expected = Collection(_Item(1))
        assert result == expected

    def test_filter(self) -> None:
        collection = Collection(map(_Item, range(4)))
        result = collection.filter(lambda item: item.n % 2 == 0)
        assert isinstance(result, Collection)
        expected = Collection(_Item(0), _Item(2))
        assert result == expected

    def test_hash(self) -> None:
        collection = Collection(map(_Item, range(3)))
        _ = hash(collection)

    def test_init(self) -> None:
        class SubCollection(Collection[_Item]):
            @override
            def __init__(self, *item_or_items: MaybeIterable[_Item]) -> None:
                super().__init__(*item_or_items)
                if any(item.n >= 1 for item in self):
                    msg = "n >= 1 is not permitted"
                    raise ValueError(msg)

        with raises(ValueError, match="n >= 1 is not permitted"):
            _ = SubCollection(map(_Item, range(3)))

    def test_map_return_same_type(self) -> None:
        collection = Collection(map(_Item, range(3)))
        result = collection.map(lambda item: replace(item, n=item.n + 1))
        assert isinstance(result, Collection)
        expected = Collection(map(_Item, range(1, 4)))
        assert result == expected

    def test_map_return_different_type(self) -> None:
        collection = Collection(map(_Item, range(3)))
        result = collection.map(lambda item: item.n)
        assert isinstance(result, Collection)
        expected = Collection(range(3))
        assert result == expected

    def test_new_one_singleton(self) -> None:
        collection = Collection(_Item(1))
        assert isinstance(collection, Collection)
        assert len(collection) == 1
        assert one(collection) == _Item(1)

    def test_new_one_iterable(self) -> None:
        collection = Collection(map(_Item, range(3)))
        assert isinstance(collection, Collection)
        assert len(collection) == 3

    def test_new_many_singletons(self) -> None:
        collection = Collection(_Item(1), _Item(2), _Item(3))
        assert isinstance(collection, Collection)
        assert len(collection) == 3

    def test_new_many_iterables(self) -> None:
        collection = Collection(map(_Item, range(3)), map(_Item, range(3)))
        assert isinstance(collection, Collection)
        assert len(collection) == 3

    def test_new_check_items(self) -> None:
        class SubCollection(Collection[_Item]):
            @classmethod
            @override
            def check_items(cls, items: Iterable[_Item]) -> None:
                if any(item.n >= 1 for item in items):
                    msg = "n >= 1 is not permitted"
                    raise ValueError(msg)

        with raises(ValueError, match="n >= 1 is not permitted"):
            _ = SubCollection(map(_Item, range(3)))

    def test_or_singleton(self) -> None:
        collection = Collection(map(_Item, range(3)))
        result = collection | _Item(3)
        assert isinstance(result, Collection)
        expected = Collection(map(_Item, range(4)))
        assert result == expected

    def test_or_collection(self) -> None:
        collection = Collection(map(_Item, range(3)))
        result = collection | Collection(map(_Item, range(1, 4)))
        assert isinstance(result, Collection)
        expected = Collection(map(_Item, range(4)))
        assert result == expected

    def test_or_iterable(self) -> None:
        collection = Collection(map(_Item, range(3)))
        result = collection | map(_Item, range(1, 4))
        assert isinstance(result, Collection)
        expected = Collection(map(_Item, range(4)))
        assert result == expected

    def test_partition(self) -> None:
        collection = Collection(map(_Item, range(4)))
        result_false, result_true = collection.partition(lambda item: item.n % 2 == 0)
        assert isinstance(result_false, Collection)
        expected_false = Collection(_Item(1), _Item(3))
        assert result_false == expected_false
        assert isinstance(result_true, Collection)
        expected_true = Collection(_Item(0), _Item(2))
        assert result_true == expected_true

    def test_repr(self) -> None:
        collection = Collection(map(_Item, range(3)))
        result = repr(collection)
        expected = "Collection({_Item(n=0), _Item(n=1), _Item(n=2)})"
        assert result == expected

    def test_str(self) -> None:
        collection = Collection(map(_Item, range(3)))
        result = str(collection)
        expected = "Collection({_Item(n=0), _Item(n=1), _Item(n=2)})"
        assert result == expected

    def test_sub_single_item(self) -> None:
        collection = Collection(map(_Item, range(3)))
        result = collection - _Item(1)
        assert isinstance(result, Collection)
        expected = Collection(_Item(0), _Item(2))
        assert result == expected

    def test_sub_collection(self) -> None:
        collection = Collection(map(_Item, range(3)))
        result = collection - Collection(_Item(1))
        assert isinstance(result, Collection)
        expected = Collection(_Item(0), _Item(2))
        assert result == expected

    def test_sub_iterable(self) -> None:
        collection = Collection(map(_Item, range(3)))
        result = collection - [_Item(1)]
        assert isinstance(result, Collection)
        expected = Collection(_Item(0), _Item(2))
        assert result == expected


class TestEnsureHashables:
    def test_main(self) -> None:
        assert ensure_hashables(1, 2, a=3, b=4) == ([1, 2], {"a": 3, "b": 4})


class TestEnsureIterable:
    @mark.parametrize("obj", [param([]), param(()), param("")])
    def test_main(self, *, obj: Any) -> None:
        _ = ensure_iterable(obj)

    def test_error(self) -> None:
        with raises(EnsureIterableError, match="Object .* must be iterable"):
            _ = ensure_iterable(None)


class TestEnsureIterableNotStr:
    @mark.parametrize("obj", [param([]), param(())])
    def test_main(self, *, obj: Any) -> None:
        _ = ensure_iterable_not_str(obj)

    @mark.parametrize("obj", [param(None), param("")])
    def test_error(self, *, obj: Any) -> None:
        with raises(
            EnsureIterableNotStrError,
            match="Object .* must be iterable, but not a string",
        ):
            _ = ensure_iterable_not_str(obj)


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


class TestGroupAndUngroupConsecutiveIntegers:
    @given(xs=lists(integers(), unique=True).map(sorted))
    def test_main(self, *, xs: list[int]) -> None:
        result = list(ungroup_consecutive_integers(group_consecutive_integers(xs)))
        assert result == xs

    def test_example(self) -> None:
        result = list(group_consecutive_integers([1, 2, 3, 6, 7, 10, 11, 12]))
        expected = [(1, 3), (6, 7), (10, 12)]
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


class TestIsIterableNotEnum:
    def test_single(self) -> None:
        class Truth(Enum):
            true = auto()
            false = auto()

        assert not is_iterable_not_enum(Truth)

    def test_union(self) -> None:
        class Truth1(Enum):
            true = auto()
            false = auto()

        class Truth2(Enum):
            true = auto()
            false = auto()

        assert is_iterable_not_enum((Truth1, Truth2))

    @mark.parametrize(
        ("obj", "expected"),
        [param(None, False), param([], True), param((), True), param("", True)],
    )
    def test_others(self, *, obj: Any, expected: bool) -> None:
        result = is_iterable_not_enum(obj)
        assert result is expected


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


class TestOne:
    @given(args=sampled_from([([None],), ([None], []), ([None], [], [])]))
    def test_main(self, *, args: tuple[Iterable[Any], ...]) -> None:
        assert one(*args) is None

    @given(args=sampled_from([([],), ([], []), ([], [], [])]))
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


class TestOneMaybe:
    @given(
        args=sampled_from([
            (None,),
            ([None],),
            (None, []),
            ([None], []),
            (None, []),
            ([None], [], []),
        ])
    )
    def test_main(self, *, args: tuple[MaybeIterable[Any], ...]) -> None:
        assert one_maybe(*args) is None

    @given(args=sampled_from([([],), ([], []), ([], [], [])]))
    def test_error_empty(self, *, args: tuple[MaybeIterable[Any], ...]) -> None:
        with raises(OneMaybeEmptyError, match=r"Object\(s\) must not be empty"):
            _ = one_maybe(*args)

    @given(iterable=sets(integers(), min_size=2))
    def test_error_non_unique(self, *, iterable: set[int]) -> None:
        with raises(
            OneMaybeNonUniqueError,
            match=re.compile(
                r"Object\(s\) .* must contain exactly one item; got .*, .* and perhaps more",
                flags=DOTALL,
            ),
        ):
            _ = one_maybe(iterable)


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


class TestOneUnique:
    @given(args=sampled_from([([None],), ([None], [None]), ([None], [None], [None])]))
    def test_main(self, *, args: tuple[Iterable[Any], ...]) -> None:
        assert one_unique(*args) is None

    @given(args=sampled_from([([],), ([], []), ([], [], [])]))
    def test_error_empty(self, *, args: tuple[Iterable[Any], ...]) -> None:
        with raises(OneUniqueEmptyError, match=r"Iterable\(s\) must not be empty"):
            _ = one_unique(*args)

    @given(iterable=sets(integers(), min_size=2))
    def test_error_non_unique(self, *, iterable: set[int]) -> None:
        with raises(
            OneUniqueNonUniqueError,
            match=re.compile(
                r"Iterable\(s\) .* must contain exactly one item; got .*, .* and perhaps more",
                flags=DOTALL,
            ),
        ):
            _ = one_unique(iterable)


class TestPairwiseTail:
    def test_main(self) -> None:
        iterable = range(5)
        result = list(pairwise_tail(iterable))
        expected = [(0, 1), (1, 2), (2, 3), (3, 4), (4, sentinel)]
        assert result == expected


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


class TestReduceMappings:
    @given(
        mappings=lists(dictionaries(text_ascii(), integers())),
        initial=just(0) | sentinels(),
    )
    def test_main(
        self, *, mappings: Sequence[Mapping[str, int]], initial: int | Sentinel
    ) -> None:
        result = reduce_mappings(add, mappings, initial=initial)
        expected = {}
        for mapping in mappings:
            for key, value in mapping.items():
                expected[key] = expected.get(key, 0) + value
        assert result == expected


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
            match="Iterables .* and .* must not overlap; got .*",
        ):
            _ = resolve_include_and_exclude(include=[1, 2, 3], exclude=[3, 4, 5])


class TestSortIterable:
    @given(
        x=make_objects(floats_allow_nan=False), y=make_objects(floats_allow_nan=False)
    )
    def test_main(self, *, x: Any, y: Any) -> None:
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
        with raises(SortIterableError, match="Unable to sort .* and .*"):
            _ = sort_iterable([sentinel, sentinel])


class TestSortIterablesCmpDateTimes:
    @given(x=datetimes() | zoned_datetimes(), y=datetimes() | zoned_datetimes())
    def test_main(self, *, x: dt.datetime, y: dt.datetime) -> None:
        result1 = _sort_iterable_cmp_datetimes(x, y)
        result2 = _sort_iterable_cmp_datetimes(y, x)
        assert result1 == -result2


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

    def test_nan_vs_nan(self) -> None:
        result = _sort_iterable_cmp_floats(nan, nan)
        assert result == 0


class TestSumMappings:
    @given(mappings=lists(dictionaries(text_ascii(), integers())))
    def test_main(self, *, mappings: Sequence[Mapping[str, int]]) -> None:
        result = sum_mappings(*mappings)
        expected = {}
        for mapping in mappings:
            for key, value in mapping.items():
                expected[key] = expected.get(key, 0) + value
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
