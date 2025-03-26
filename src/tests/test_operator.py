from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from enum import Enum, auto
from functools import partial
from math import nan
from typing import TYPE_CHECKING, Any

from hypothesis import example, given
from hypothesis.strategies import (
    SearchStrategy,
    booleans,
    builds,
    dates,
    datetimes,
    dictionaries,
    floats,
    integers,
    just,
    lists,
    none,
    recursive,
    sampled_from,
    times,
    timezones,
    tuples,
    uuids,
)
from polars import DataFrame, Int64
from pytest import raises

import utilities.math
import utilities.operator
from tests.conftest import IS_CI_AND_WINDOWS
from tests.test_typing_funcs.with_future import (
    DataClassNestedWithFutureInnerThenOuterOuter,
    DataClassNestedWithFutureOuterThenInnerOuter,
    DataClassWithCustomEquality,
    DataClassWithInt,
    DataClassWithIntDefault,
    DataClassWithLiteral,
    DataClassWithLiteralNullable,
    DataClassWithNone,
)
from utilities.hypothesis import (
    assume_does_not_raise,
    int64s,
    pairs,
    paths,
    text_ascii,
    text_printable,
    timedeltas_2w,
    versions,
    zoned_datetimes,
)
from utilities.math import MAX_INT64, MIN_INT64
from utilities.operator import IsEqualError
from utilities.polars import are_frames_equal

if TYPE_CHECKING:
    from utilities.types import DateOrDateTime, Number
    from utilities.typing import StrMapping


def base_objects(
    *,
    dataclass_nested: bool = False,
    dataclass_with_custom_equality: bool = False,
    dataclass_with_int: bool = False,
    dataclass_with_int_default: bool = False,
    dataclass_with_literal: bool = False,
    dataclass_with_literal_nullable: bool = False,
    dataclass_with_none: bool = False,
    enum: bool = False,
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
        | datetimes()
        | int64s()
        | none()
        | paths()
        | text_printable().filter(lambda x: not x.startswith("["))
        | times()
        | timedeltas_2w()
        | uuids()
        | versions()
    )
    if IS_CI_AND_WINDOWS:
        base |= zoned_datetimes()
    else:
        base |= zoned_datetimes(time_zone=timezones() | just(dt.UTC), valid=True)
    if dataclass_nested:
        base |= builds(DataClassNestedWithFutureInnerThenOuterOuter).filter(
            lambda outer: _is_int64(outer.inner.int_)
        ) | builds(DataClassNestedWithFutureOuterThenInnerOuter).filter(
            lambda outer: _is_int64(outer.inner.int_)
        )
    if dataclass_with_custom_equality:
        base |= builds(DataClassWithCustomEquality)
    if dataclass_with_int:
        base |= builds(DataClassWithInt).filter(lambda obj: _is_int64(obj.int_))
    if dataclass_with_int_default:
        base |= builds(DataClassWithIntDefault).filter(lambda obj: _is_int64(obj.int_))
    if dataclass_with_literal:
        base |= builds(DataClassWithLiteral, truth=sampled_from(["true", "false"]))
    if dataclass_with_literal_nullable:
        base |= builds(
            DataClassWithLiteralNullable, truth=sampled_from(["true", "false"]) | none()
        )
    if dataclass_with_none:
        base |= builds(DataClassWithNone)
    if enum:
        base |= sampled_from(TruthEnum)
    return base


def make_objects(
    *,
    dataclass_nested: bool = False,
    dataclass_with_custom_equality: bool = False,
    dataclass_with_int: bool = False,
    dataclass_with_int_default: bool = False,
    dataclass_with_literal: bool = False,
    dataclass_with_literal_nullable: bool = False,
    dataclass_with_none: bool = False,
    enum: bool = False,
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
        dataclass_nested=dataclass_nested,
        dataclass_with_custom_equality=dataclass_with_custom_equality,
        dataclass_with_int=dataclass_with_int,
        dataclass_with_int_default=dataclass_with_int_default,
        dataclass_with_literal=dataclass_with_literal,
        dataclass_with_literal_nullable=dataclass_with_literal_nullable,
        dataclass_with_none=dataclass_with_none,
        enum=enum,
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
    @given(
        obj=make_objects(
            dataclass_nested=True,
            dataclass_with_custom_equality=True,
            dataclass_with_int=True,
            dataclass_with_int_default=True,
            dataclass_with_literal=True,
            dataclass_with_literal_nullable=True,
            dataclass_with_none=True,
            sub_frozenset=True,
            sub_list=True,
            sub_set=True,
            sub_tuple=True,
        )
    )
    def test_one(self, *, obj: Any) -> None:
        with assume_does_not_raise(IsEqualError):
            assert utilities.operator.is_equal(obj, obj)

    @given(
        objs=pairs(
            make_objects(
                dataclass_nested=True,
                dataclass_with_custom_equality=True,
                dataclass_with_int=True,
                dataclass_with_int_default=True,
                dataclass_with_literal=True,
                dataclass_with_literal_nullable=True,
                dataclass_with_none=True,
                sub_frozenset=True,
                sub_list=True,
                sub_set=True,
                sub_tuple=True,
            )
        )
    )
    def test_two_objects(self, *, objs: tuple[Any, Any]) -> None:
        first, second = objs
        with assume_does_not_raise(IsEqualError):
            result = utilities.operator.is_equal(first, second)
        assert isinstance(result, bool)

    @given(x=integers())
    def test_dataclass_with_custom_equality(self, *, x: int) -> None:
        first, second = (
            DataClassWithCustomEquality(int_=x),
            DataClassWithCustomEquality(int_=x),
        )
        assert first != second
        assert utilities.operator.is_equal(first, second)

    def test_dataclass_of_numbers(self) -> None:
        @dataclass
        class Example:
            x: Number

        first, second = Example(x=0), Example(x=1e-16)
        assert not utilities.operator.is_equal(first, second)
        assert utilities.operator.is_equal(first, second, abs_tol=1e-8)

    @given(
        x=dates() | datetimes() | zoned_datetimes(time_zone=timezones()),
        y=dates() | datetimes() | zoned_datetimes(time_zone=timezones()),
    )
    def test_dates_or_datetimes(self, *, x: DateOrDateTime, y: DateOrDateTime) -> None:
        result = utilities.operator.is_equal(x, y)
        assert isinstance(result, bool)

    def test_float_vs_int(self) -> None:
        x, y = 0, 1e-16
        assert not utilities.math.is_equal(x, y)
        assert utilities.math.is_equal(x, y, abs_tol=1e-8)
        assert not utilities.operator.is_equal(x, y)
        assert utilities.operator.is_equal(x, y, abs_tol=1e-8)

    @given(
        x=dictionaries(text_ascii(), make_objects(), max_size=10),
        y=dictionaries(text_ascii(), make_objects(), max_size=10),
    )
    def test_mappings(self, *, x: StrMapping, y: StrMapping) -> None:
        with assume_does_not_raise(IsEqualError):
            result = utilities.operator.is_equal(x, y)
        assert isinstance(result, bool)

    @given(x=floats(), y=floats())
    @example(x=-4.233805663404397, y=nan)
    def test_sets_of_floats(self, *, x: float, y: float) -> None:
        assert utilities.operator.is_equal({x, y}, {y, x})

    @given(
        case=sampled_from([
            (DataFrame(), DataFrame(), True),
            (DataFrame([()]), DataFrame([()]), True),
            (DataFrame(), DataFrame(schema={"value": Int64}), False),
            (DataFrame([()]), DataFrame([(0,)], schema={"value": Int64}), False),
        ])
    )
    def test_extra(self, *, case: tuple[DataFrame, DataFrame, bool]) -> None:
        x, y, expected = case
        result = utilities.operator.is_equal(x, y, extra={DataFrame: are_frames_equal})
        assert result is expected

    def test_extra_but_no_match(self) -> None:
        with raises(ValueError, match="DataFrame columns do not match"):
            _ = utilities.operator.is_equal(
                DataFrame(), DataFrame(schema={"value": Int64}), extra={}
            )
