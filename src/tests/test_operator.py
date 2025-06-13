from __future__ import annotations

from dataclasses import dataclass
from math import nan
from typing import TYPE_CHECKING, Any

from hypothesis import example, given
from hypothesis.strategies import dictionaries, floats, integers, lists
from polars import DataFrame, Int64
from pytest import mark, param, raises

import utilities.math
import utilities.operator
from tests.test_objects.objects import CustomError, TruthEnum, objects
from tests.test_typing_funcs.with_future import DataClassFutureCustomEquality
from utilities.hypothesis import assume_does_not_raise, date_deltas, pairs, text_ascii
from utilities.operator import IsEqualError
from utilities.polars import are_frames_equal

if TYPE_CHECKING:
    from collections.abc import Sequence

    from whenever import DateDelta

    from utilities.types import Number
    from utilities.typing import StrMapping


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

    @given(deltas=pairs(date_deltas()))
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

    @given(ints=pairs(lists(integers())))
    def test_exception_instance(
        self, *, ints: tuple[Sequence[int], Sequence[int]]
    ) -> None:
        x, y = ints
        result = utilities.operator.is_equal(CustomError(*x), CustomError(*y))
        expected = x == y
        assert result is expected

    def test_float_vs_int(self) -> None:
        x, y = 0, 1e-16
        assert not utilities.math.is_equal(x, y)
        assert utilities.math.is_equal(x, y, abs_tol=1e-8)
        assert not utilities.operator.is_equal(x, y)
        assert utilities.operator.is_equal(x, y, abs_tol=1e-8)

    @given(mappings=pairs(dictionaries(text_ascii(), objects())))
    def test_mappings(self, *, mappings: tuple[StrMapping, StrMapping]) -> None:
        x, y = mappings
        result = utilities.operator.is_equal(x, y)
        assert isinstance(result, bool)

    @given(deltas=pairs(date_deltas()))
    def test_sets_of_date_deltas(self, *, deltas: tuple[DateDelta, DateDelta]) -> None:
        x, y = deltas
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
