from __future__ import annotations

from dataclasses import dataclass
from math import nan
from typing import TYPE_CHECKING, Any

from hypothesis import example, given, settings
from hypothesis.strategies import (
    SearchStrategy,
    booleans,
    builds,
    dictionaries,
    floats,
    integers,
    just,
    lists,
    none,
    recursive,
    sampled_from,
    times,
    tuples,
    uuids,
)
from polars import DataFrame, Int64
from pytest import raises

import utilities.math
import utilities.operator
from tests.conftest import IS_CI
from tests.test_typing_funcs.with_future import (
    DataClassFutureCustomEquality,
    DataClassFutureDefaultInInitChild,
    DataClassFutureInt,
    DataClassFutureIntDefault,
    DataClassFutureLiteral,
    DataClassFutureLiteralNullable,
    DataClassFutureNestedInnerFirstOuter,
    DataClassFutureNestedOuterFirstOuter,
    DataClassFutureNone,
    DataClassFutureTypeLiteral,
    DataClassFutureTypeLiteralNullable,
)
from utilities.hypothesis import (
    assume_does_not_raise,
    date_deltas,
    dates,
    int64s,
    pairs,
    paths,
    plain_datetimes,
    text_ascii,
    text_printable,
    versions,
    zoned_datetimes,
)
from utilities.operator import IsEqualError
from utilities.polars import are_frames_equal
from utilities.zoneinfo import UTC

if TYPE_CHECKING:
    from collections.abc import Sequence

    from utilities.types import Number
    from utilities.typing import StrMapping


def base_objects(
    *,
    dataclass_custom_equality: bool = False,
    dataclass_default_in_init_child: bool = False,
    dataclass_int: bool = False,
    dataclass_int_default: bool = False,
    dataclass_literal: bool = False,
    dataclass_literal_nullable: bool = False,
    dataclass_nested: bool = False,
    dataclass_none: bool = False,
    dataclass_type_literal: bool = False,
    dataclass_type_literal_nullable: bool = False,
    enum: bool = False,
    exception_class: bool = False,
    exception_instance: bool = False,
    floats_min_value: Number | None = None,
    floats_max_value: Number | None = None,
    floats_allow_nan: bool | None = None,
    floats_allow_infinity: bool | None = None,
) -> SearchStrategy[Any]:
    base = (
        booleans()
        | floats(
            min_value=floats_min_value,
            max_value=floats_max_value,
            allow_nan=floats_allow_nan,
            allow_infinity=floats_allow_infinity,
        )
        | dates()
        | int64s()
        | none()
        | paths()
        | plain_datetimes()
        | text_printable().filter(lambda x: not x.startswith("["))
        | date_deltas(parsable=True)
        | times()
        | uuids()
        | versions()
        | zoned_datetimes(time_zone=just(UTC) if IS_CI else timezones())
    )
    if dataclass_custom_equality:
        base |= builds(DataClassFutureCustomEquality)
    if dataclass_default_in_init_child:
        base |= builds(DataClassFutureDefaultInInitChild)
    if dataclass_int:
        base |= builds(DataClassFutureInt).filter(lambda obj: _is_int64(obj.int_))
    if dataclass_int_default:
        base |= builds(DataClassFutureIntDefault).filter(
            lambda obj: _is_int64(obj.int_)
        )
    if dataclass_literal:
        base |= builds(DataClassFutureLiteral, truth=sampled_from(["true", "false"]))
    if dataclass_literal_nullable:
        base |= builds(
            DataClassFutureLiteralNullable,
            truth=sampled_from(["true", "false"]) | none(),
        )
    if dataclass_nested:
        base |= builds(DataClassFutureNestedInnerFirstOuter).filter(
            lambda outer: _is_int64(outer.inner.int_)
        ) | builds(DataClassFutureNestedOuterFirstOuter).filter(
            lambda outer: _is_int64(outer.inner.int_)
        )
    if dataclass_none:
        base |= builds(DataClassFutureNone)
    if dataclass_type_literal:
        base |= builds(
            DataClassFutureTypeLiteral, truth=sampled_from(["true", "false"])
        )
    if dataclass_type_literal_nullable:
        base |= builds(
            DataClassFutureTypeLiteralNullable,
            truth=sampled_from(["true", "false"]) | none(),
        )
    if enum:
        base |= sampled_from(TruthEnum)
    if exception_class:
        base |= just(CustomError)
    if exception_instance:
        base |= builds(CustomError, int64s())
    return base


def make_objects(
    *,
    dataclass_custom_equality: bool = False,
    dataclass_default_in_init_child: bool = False,
    dataclass_int: bool = False,
    dataclass_int_default: bool = False,
    dataclass_literal: bool = False,
    dataclass_literal_nullable: bool = False,
    dataclass_nested: bool = False,
    dataclass_none: bool = False,
    dataclass_type_literal: bool = False,
    dataclass_type_literal_nullable: bool = False,
    enum: bool = False,
    exception_class: bool = False,
    exception_instance: bool = False,
    floats_min_value: Number | None = None,
    floats_max_value: Number | None = None,
    floats_allow_nan: bool | None = None,
    floats_allow_infinity: bool | None = None,
    extra_base: SearchStrategy[Any] | None = None,
    sub_frozenset: bool = False,
    sub_list: bool = False,
    sub_set: bool = False,
    sub_tuple: bool = False,
) -> SearchStrategy[Any]:
    base = base_objects(
        dataclass_custom_equality=dataclass_custom_equality,
        dataclass_default_in_init_child=dataclass_default_in_init_child,
        dataclass_int=dataclass_int,
        dataclass_int_default=dataclass_int_default,
        dataclass_literal=dataclass_literal,
        dataclass_literal_nullable=dataclass_literal_nullable,
        dataclass_nested=dataclass_nested,
        dataclass_none=dataclass_none,
        dataclass_type_literal=dataclass_type_literal,
        dataclass_type_literal_nullable=dataclass_type_literal_nullable,
        enum=enum,
        exception_class=exception_class,
        exception_instance=exception_instance,
        floats_min_value=floats_min_value,
        floats_max_value=floats_max_value,
        floats_allow_nan=floats_allow_nan,
        floats_allow_infinity=floats_allow_infinity,
    )
    if extra_base is not None:
        base |= extra_base
    return recursive(
        base,
        partial(
            _extend,
            sub_frozenset=sub_frozenset,
            sub_list=sub_list,
            sub_set=sub_set,
            sub_tuple=sub_tuple,
        ),
    )


def _extend(
    strategy: SearchStrategy[Any],
    /,
    *,
    sub_frozenset: bool = False,
    sub_list: bool = False,
    sub_set: bool = False,
    sub_tuple: bool = False,
) -> SearchStrategy[Any]:
    lsts = lists(strategy)
    sets = lsts.map(_into_set)
    frozensets = lists(strategy).map(_into_set).map(frozenset)
    extension = (
        dictionaries(text_ascii(), strategy)
        | frozensets
        | lsts
        | sets
        | tuples(strategy)
    )
    if sub_frozenset:
        extension |= frozensets.map(SubFrozenSet)
    if sub_list:
        extension |= lists(strategy).map(SubList)
    if sub_set:
        extension |= sets.map(SubSet)
    if sub_tuple:
        extension |= tuples(strategy).map(SubTuple)
    return extension


def _is_int64(n: int, /) -> bool:
    return MIN_INT64 <= n <= MAX_INT64


def _into_set(elements: list[Any], /) -> set[Any]:
    with assume_does_not_raise(TypeError, match="unhashable type"):
        return set(elements)


class CustomError(Exception): ...


class SubFrozenSet(frozenset):
    pass


class SubList(list):
    pass


class SubSet(set):
    pass


class SubTuple(tuple):  # noqa: SLOT001
    pass


class TruthEnum(Enum):
    true = auto()
    false = auto()


# tests


class TestIsEqual:
    @given(obj=objects(all_=True))
    def test_one(self, *, obj: Any) -> None:
        with assume_does_not_raise(IsEqualError):
            assert utilities.operator.is_equal(obj, obj)

    @given(objs=pairs(objects(all_=True)))
    def test_two_objects(self, *, objs: tuple[Any, Any]) -> None:
        x, y = objs
        with assume_does_not_raise(IsEqualError):
            _ = utilities.operator.is_equal(x, y)

    @given(int_=integers())
    def test_dataclass_custom_equality(self, *, int_: int) -> None:
        x, y = (
            DataClassFutureCustomEquality(int_=int_),
            DataClassFutureCustomEquality(int_=int_),
        )
        assert x != y
        assert utilities.operator.is_equal(x, y)

    @given(deltas=pairs(date_deltas_whenever()))
    def test_date_deltas(self, *, deltas: tuple[DateDelta, DateDelta]) -> None:
        x, y = deltas
        result = utilities.operator.is_equal(x, y)
        expected = x == y
        assert result is expected

    def test_dataclass_of_numbers(self) -> None:
        @dataclass
        class Example:
            x: Number

        first, second = Example(x=0), Example(x=1e-16)
        assert not utilities.operator.is_equal(first, second)
        assert utilities.operator.is_equal(first, second, abs_tol=1e-8)

    def test_exception_class(self) -> None:
        assert utilities.operator.is_equal(CustomError, CustomError)

    @given(x=lists(integers()), y=lists(integers()))
    def test_exception_instance(self, *, x: Sequence[int], y: Sequence[int]) -> None:
        result = utilities.operator.is_equal(CustomError(*x), CustomError(*y))
        expected = x == y
        assert result is expected

    def test_float_vs_int(self) -> None:
        x, y = 0, 1e-16
        assert not utilities.math.is_equal(x, y)
        assert utilities.math.is_equal(x, y, abs_tol=1e-8)
        assert not utilities.operator.is_equal(x, y)
        assert utilities.operator.is_equal(x, y, abs_tol=1e-8)

    @given(
        x=dictionaries(text_ascii(), objects()), y=dictionaries(text_ascii(), objects())
    )
    def test_mappings(self, *, x: StrMapping, y: StrMapping) -> None:
        result = utilities.operator.is_equal(x, y)
        assert isinstance(result, bool)

    @given(x=date_deltas_whenever(), y=date_deltas_whenever())
    def test_sets_of_date_deltas(self, *, x: float, y: float) -> None:
        assert utilities.operator.is_equal({x, y}, {y, x})

    def test_sets_of_enums(self) -> None:
        obj = set(TruthEnum)
        assert utilities.operator.is_equal(obj, obj)

    def test_sets_of_errors(self) -> None:
        obj = {CustomError(), CustomError()}
        obj2 = {CustomError(), CustomError()}
        assert utilities.operator.is_equal(obj, obj2)

    @given(x=floats(), y=floats())
    @example(x=-4.233805663404397, y=nan)
    def test_sets_of_floats(self, *, x: float, y: float) -> None:
        assert utilities.operator.is_equal({x, y}, {y, x})

    @mark.parametrize(
        ("x", "y", "expected"),
        [
            param(DataFrame(), DataFrame(), True),
            param(DataFrame([()]), DataFrame([()]), True),
            param(DataFrame(), DataFrame(schema={"value": Int64}), False),
            param(DataFrame([()]), DataFrame([(0,)], schema={"value": Int64}), False),
        ],
    )
    def test_extra(self, *, x: DataFrame, y: DataFrame, expected: bool) -> None:
        result = utilities.operator.is_equal(x, y, extra={DataFrame: are_frames_equal})
        assert result is expected

    def test_extra_but_no_match(self) -> None:
        with raises(ValueError, match="DataFrame columns do not match"):
            _ = utilities.operator.is_equal(
                DataFrame(), DataFrame(schema={"value": Int64}), extra={}
            )
