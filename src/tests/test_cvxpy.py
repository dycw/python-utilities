from __future__ import annotations

import re
from re import DOTALL
from typing import TYPE_CHECKING, Any, cast

import cvxpy
import numpy as np
from cvxpy import CLARABEL, Expression, Maximize, Minimize, Problem, Variable
from numpy import array, isclose
from numpy.testing import assert_equal
from pandas import DataFrame, Series
from pandas.testing import assert_frame_equal, assert_series_equal
from pytest import mark, param, raises

from utilities.cvxpy import (
    MaximumError,
    MinimumError,
    MultiplyError,
    ScalarProductError,
    SolveInfeasibleError,
    SolveUnboundedError,
    _check_series_and_dataframe,
    _CheckSeriesAndDataFrameError,
    abs_,
    add,
    divide,
    max_,
    maximum,
    min_,
    minimum,
    multiply,
    negate,
    negative,
    norm,
    positive,
    power,
    quad_form,
    scalar_product,
    solve,
    sqrt,
    subtract,
    sum_,
    sum_axis0,
    sum_axis1,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from utilities.numpy import NDArrayF
    from utilities.pandas import SeriesF


def _get_variable(
    objective: type[Maximize | Minimize], /, *, shape: tuple[int, ...] | None = None
) -> Variable:
    if shape is None:
        var = Variable()
        scalar = var
    else:
        var = Variable(shape=shape)
        scalar = cvxpy.sum(var)
    threshold = 10.0
    problem = Problem(
        objective(scalar), [cast(Any, var) >= -threshold, cast(Any, var) <= threshold]
    )
    _ = problem.solve(solver=CLARABEL)
    return var


class TestAbs:
    @mark.parametrize(
        ("x", "expected"), [param(0.0, 0.0), param(1.0, 1.0), param(-1.0, 1.0)]
    )
    def test_float(self, *, x: float, expected: float) -> None:
        assert isclose(abs_(x), expected)

    @mark.parametrize(
        ("x", "expected"),
        [
            param(array([0.0]), array([0.0])),
            param(array([1.0]), array([1.0])),
            param(array([-1.0]), array([1.0])),
        ],
    )
    def test_array(self, *, x: NDArrayF, expected: NDArrayF) -> None:
        assert_equal(abs_(x), expected)

    @mark.parametrize(
        ("x", "expected"),
        [
            param(Series([0.0]), Series([0.0])),
            param(Series([1.0]), Series([1.0])),
            param(Series([-1.0]), Series([1.0])),
        ],
    )
    def test_series(self, *, x: SeriesF, expected: SeriesF) -> None:
        assert_series_equal(abs_(x), expected)

    @mark.parametrize(
        ("x", "expected"),
        [
            param(DataFrame([0.0]), DataFrame([0.0])),
            param(DataFrame([1.0]), DataFrame([1.0])),
            param(DataFrame([-1.0]), DataFrame([1.0])),
        ],
    )
    def test_dataframe(self, *, x: DataFrame, expected: DataFrame) -> None:
        assert_frame_equal(abs_(x), expected)

    @mark.parametrize("objective", [param(Maximize), param(Minimize)])
    def test_expression(self, *, objective: type[Maximize | Minimize]) -> None:
        var = _get_variable(objective)
        assert_equal(abs_(var).value, abs_(var.value))


class TestAdd:
    @mark.parametrize(
        ("x", "y", "expected"),
        [
            param(1.0, 2.0, 3.0),
            param(1.0, array([2.0]), array([3.0])),
            param(array([1.0]), 2.0, array([3.0])),
            param(array([1.0]), array([2.0]), array([3.0])),
        ],
    )
    def test_float_and_array(
        self, *, x: float | NDArrayF, y: float | NDArrayF, expected: float | NDArrayF
    ) -> None:
        assert_equal(add(x, y), expected)

    @mark.parametrize("x", [param(1.0), param(array([1.0]))])
    @mark.parametrize("objective", [param(Maximize), param(Minimize)])
    def test_one_expression(
        self, *, x: float | NDArrayF | Expression, objective: type[Maximize | Minimize]
    ) -> None:
        var = _get_variable(objective)
        assert isclose(add(x, var).value, add(x, var.value))  # type: ignore[]
        assert isclose(add(var, x).value, add(var.value, x))  # type: ignore[]

    @mark.parametrize("objective1", [param(Maximize), param(Minimize)])
    @mark.parametrize("objective2", [param(Maximize), param(Minimize)])
    def test_two_expressions(
        self,
        *,
        objective1: type[Maximize | Minimize],
        objective2: type[Maximize | Minimize],
    ) -> None:
        var1 = _get_variable(objective1)
        var2 = _get_variable(objective2)
        assert_equal(add(var1, var2).value, add(var1.value, var2.value))


class TestCheckSeriesAndDataFrame:
    def test_main(self) -> None:
        x, y = Series([2.0]), DataFrame([3.0])
        match = re.compile(
            r"Function must not be between a Series and DataFrame; got .* and .*\.",
            flags=DOTALL,
        )
        with raises(_CheckSeriesAndDataFrameError, match=match):
            _ = _check_series_and_dataframe(cast(Any, x), cast(Any, y))
        with raises(_CheckSeriesAndDataFrameError, match=match):
            _ = _check_series_and_dataframe(cast(Any, y), cast(Any, x))


class TestDivide:
    @mark.parametrize(
        ("x", "y", "expected"),
        [
            param(1.0, 2.0, 0.5),
            param(1.0, array([2.0]), array([0.5])),
            param(array([1.0]), 2.0, array([0.5])),
            param(array([1.0]), array([2.0]), array([0.5])),
        ],
    )
    def test_float_and_array(
        self, *, x: float | NDArrayF, y: float | NDArrayF, expected: float | NDArrayF
    ) -> None:
        assert_equal(divide(x, y), expected)

    @mark.parametrize("x", [param(1.0), param(array([1.0]))])
    @mark.parametrize("objective", [param(Maximize), param(Minimize)])
    def test_one_expression(
        self, *, x: float | NDArrayF | Expression, objective: type[Maximize | Minimize]
    ) -> None:
        var = _get_variable(objective)
        assert_equal(divide(x, var).value, divide(x, var.value))
        assert_equal(divide(var, x).value, divide(var.value, x))

    @mark.parametrize("objective1", [param(Maximize), param(Minimize)])
    @mark.parametrize("objective2", [param(Maximize), param(Minimize)])
    def test_two_expressions(
        self,
        *,
        objective1: type[Maximize | Minimize],
        objective2: type[Maximize | Minimize],
    ) -> None:
        var1 = _get_variable(objective1)
        var2 = _get_variable(objective2)
        assert_equal(divide(var1, var2).value, divide(var1.value, var2.value))


class TestMax:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(0.0, 0.0),
            param(array([1.0, 2.0]), 2.0),
            param(array([-1.0, -2.0]), -1.0),
            param(Series([1.0, 2.0]), 2.0),
            param(Series([-1.0, -2.0]), -1.0),
            param(DataFrame([1.0, 2.0]), 2.0),
            param(DataFrame([-1.0, -2.0]), -1.0),
        ],
    )
    def test_float_array_and_ndframe(
        self, *, x: float | NDArrayF | SeriesF | DataFrame, expected: float
    ) -> None:
        assert_equal(max_(x), expected)

    @mark.parametrize("objective", [param(Maximize), param(Minimize)])
    def test_expression(self, *, objective: type[Maximize | Minimize]) -> None:
        var = _get_variable(objective)
        assert isclose(max_(var).value, max_(var.value))


class TestMaximumAndMinimum:
    @mark.parametrize(("func", "expected"), [param(maximum, 3.0), param(minimum, 2.0)])
    def test_two_floats(self, *, func: Callable[..., Any], expected: float) -> None:
        assert isclose(func(2.0, 3.0), expected)

    @mark.parametrize(("func", "expected"), [param(maximum, 3.0), param(minimum, 2.0)])
    def test_two_arrays(self, *, func: Callable[..., Any], expected: float) -> None:
        assert_equal(func(array([2.0]), array([3.0])), array([expected]))

    @mark.parametrize(("func", "expected"), [param(maximum, 3.0), param(minimum, 2.0)])
    def test_two_series(self, *, func: Callable[..., Any], expected: float) -> None:
        res = func(Series([2.0]), Series([3.0]))
        exp_series = Series([expected])
        assert_series_equal(res, exp_series)

    @mark.parametrize(("func", "expected"), [param(maximum, 3.0), param(minimum, 2.0)])
    def test_two_dataframes(self, *, func: Callable[..., Any], expected: float) -> None:
        res = func(DataFrame([2.0]), DataFrame([3.0]))
        exp_df = DataFrame([expected])
        assert_frame_equal(res, exp_df)

    @mark.parametrize("func", [param(maximum), param(minimum)])
    @mark.parametrize("objective1", [param(Maximize), param(Minimize)])
    @mark.parametrize("objective2", [param(Maximize), param(Minimize)])
    def test_two_expressions(
        self,
        *,
        func: Callable[..., Any],
        objective1: type[Maximize | Minimize],
        objective2: type[Maximize | Minimize],
    ) -> None:
        var1 = _get_variable(objective1)
        var2 = _get_variable(objective2)
        assert isclose(func(var1, var2).value, func(var1.value, var2.value))

    @mark.parametrize("func", [param(maximum), param(minimum)])
    @mark.parametrize("objective", [param(Maximize), param(Minimize)])
    @mark.parametrize("shape", [param(None), param((2, 2))])
    def test_float_and_expr(
        self,
        *,
        func: Callable[..., Any],
        objective: type[Maximize | Minimize],
        shape: tuple[int, ...] | None,
    ) -> None:
        x, y = 2.0, _get_variable(objective, shape=shape)
        assert_equal(func(x, y).value, func(x, y.value))
        assert_equal(func(y, x).value, func(y.value, x))

    @mark.parametrize(("func", "expected"), [param(maximum, 3.0), param(minimum, 2.0)])
    def test_array_and_series(
        self, *, func: Callable[..., Any], expected: float
    ) -> None:
        x, y, exp_series = array([2.0]), Series([3.0]), Series([expected])
        assert_series_equal(func(x, y), exp_series)
        assert_series_equal(func(y, x), exp_series)

    @mark.parametrize(("func", "expected"), [param(maximum, 3.0), param(minimum, 2.0)])
    def test_array_and_dataframe(
        self, *, func: Callable[..., Any], expected: float
    ) -> None:
        x, y, exp_df = array([2.0]), DataFrame([3.0]), DataFrame([expected])
        assert_frame_equal(func(x, y), exp_df)
        assert_frame_equal(func(y, x), exp_df)

    @mark.parametrize("func", [param(maximum), param(minimum)])
    @mark.parametrize("objective", [param(Maximize), param(Minimize)])
    def test_array_and_expr(
        self, *, func: Callable[..., Any], objective: type[Maximize | Minimize]
    ) -> None:
        x, y = array([2.0]), _get_variable(objective)
        assert isclose(func(x, y).value, func(x, y.value))
        assert isclose(func(y, x).value, func(y.value, x))

    @mark.parametrize(
        ("func", "error"), [param(maximum, MaximumError), param(minimum, MinimumError)]
    )
    def test_series_and_dataframe(
        self, func: Callable[..., Any], error: type[Exception]
    ) -> None:
        x, y = Series([2.0]), DataFrame([3.0])
        match = re.compile(
            r".* must not be between a Series and DataFrame; got .* and .*\.",
            flags=DOTALL,
        )
        with raises(error, match=match):
            _ = func(cast(Any, x), cast(Any, y))
        with raises(error, match=match):
            _ = func(cast(Any, y), cast(Any, x))

    @mark.parametrize("func", [param(maximum), param(minimum)])
    @mark.parametrize("objective", [param(Maximize), param(Minimize)])
    def test_series_and_expr(
        self, *, func: Callable[..., Any], objective: type[Maximize | Minimize]
    ) -> None:
        x, y = Series([2.0]), _get_variable(objective)
        assert isclose(func(x, y).value, func(x, y.value))
        assert isclose(func(y, x).value, func(y.value, x))

    @mark.parametrize("func", [param(maximum), param(minimum)])
    @mark.parametrize("objective", [param(Maximize), param(Minimize)])
    def test_dataframe_and_expr(
        self, *, func: Callable[..., Any], objective: type[Maximize | Minimize]
    ) -> None:
        x, y = DataFrame([2.0]), _get_variable(objective)
        assert isclose(func(x, y).value, func(x, y.value))
        assert isclose(func(y, x).value, func(y.value, x))


class TestMin:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(0.0, 0.0),
            param(array([1.0, 2.0]), 1.0),
            param(array([-1.0, -2.0]), -2.0),
            param(Series([1.0, 2.0]), 1.0),
            param(Series([-1.0, -2.0]), -2.0),
            param(DataFrame([1.0, 2.0]), 1.0),
            param(DataFrame([-1.0, -2.0]), -2.0),
        ],
    )
    def test_float_array_and_ndframe(
        self, *, x: float | NDArrayF | SeriesF | DataFrame, expected: float
    ) -> None:
        assert isclose(min_(x), expected)

    @mark.parametrize("objective", [param(Maximize), param(Minimize)])
    def test_expression(self, *, objective: type[Maximize | Minimize]) -> None:
        var = _get_variable(objective, shape=(2,))
        assert isclose(min_(var).value, min_(var.value))


class TestMultiply:
    def test_two_floats(self) -> None:
        assert isclose(multiply(2.0, 3.0), 6.0)

    def test_two_arrays(self) -> None:
        assert_equal(multiply(array([2.0]), array([3.0])), array([6.0]))

    def test_two_series(self) -> None:
        res = multiply(Series([2.0]), Series([3.0]))
        expected = Series([6.0])
        assert_series_equal(res, expected)

    def test_two_dataframes(self) -> None:
        res = multiply(DataFrame([2.0]), DataFrame([3.0]))
        expected = DataFrame([6.0])
        assert_frame_equal(res, expected)

    @mark.parametrize("objective1", [param(Maximize), param(Minimize)])
    @mark.parametrize("objective2", [param(Maximize), param(Minimize)])
    def test_two_expressions(
        self,
        *,
        objective1: type[Maximize | Minimize],
        objective2: type[Maximize | Minimize],
    ) -> None:
        var1 = _get_variable(objective1)
        var2 = _get_variable(objective2)
        assert isclose(multiply(var1, var2).value, multiply(var1.value, var2.value))

    def test_float_and_array(self) -> None:
        x, y, expected = 2.0, array([3.0]), array([6.0])
        assert_equal(multiply(x, y), expected)
        assert_equal(multiply(y, x), expected)

    def test_float_and_series(self) -> None:
        x, y, expected = 2.0, Series([3.0]), Series([6.0])
        assert_series_equal(multiply(x, y), expected)
        assert_series_equal(multiply(y, x), expected)

    def test_float_and_dataframe(self) -> None:
        x, y, expected = 2.0, DataFrame([3.0]), DataFrame([6.0])
        assert_frame_equal(multiply(x, y), expected)
        assert_frame_equal(multiply(y, x), expected)

    @mark.parametrize("objective", [param(Maximize), param(Minimize)])
    @mark.parametrize("shape", [param(None), param((2, 2))])
    def test_float_and_expr(
        self, *, objective: type[Maximize | Minimize], shape: tuple[int, ...] | None
    ) -> None:
        x, y = 2.0, _get_variable(objective, shape=shape)
        assert_equal(multiply(x, y).value, multiply(x, y.value))
        assert_equal(multiply(y, x).value, multiply(y.value, x))

    def test_array_and_series(self) -> None:
        x, y, expected = array([2.0]), Series([3.0]), Series([6.0])
        assert_series_equal(multiply(x, y), expected)
        assert_series_equal(multiply(y, x), expected)

    def test_array_and_dataframe(self) -> None:
        x, y, expected = array([2.0]), DataFrame([3.0]), DataFrame([6.0])
        assert_frame_equal(multiply(x, y), expected)
        assert_frame_equal(multiply(y, x), expected)

    @mark.parametrize("objective", [param(Maximize), param(Minimize)])
    def test_array_and_expr(self, *, objective: type[Maximize | Minimize]) -> None:
        x, y = array([2.0]), _get_variable(objective)
        assert isclose(multiply(x, y).value, multiply(x, y.value))
        assert isclose(multiply(y, x).value, multiply(y.value, x))

    def test_series_and_dataframe(self) -> None:
        x, y = Series([2.0]), DataFrame([3.0])
        match = re.compile(
            r"Multiply must not be between a Series and DataFrame; got .* and .*\.",
            flags=DOTALL,
        )
        with raises(MultiplyError, match=match):
            _ = multiply(cast(Any, x), cast(Any, y))
        with raises(MultiplyError, match=match):
            _ = multiply(cast(Any, y), cast(Any, x))

    @mark.parametrize("objective", [param(Maximize), param(Minimize)])
    def test_series_and_expr(self, *, objective: type[Maximize | Minimize]) -> None:
        x, y = Series([2.0]), _get_variable(objective)
        assert isclose(multiply(x, y).value, multiply(x, y.value))
        assert isclose(multiply(y, x).value, multiply(y.value, x))

    @mark.parametrize("objective", [param(Maximize), param(Minimize)])
    def test_dataframe_and_expr(self, *, objective: type[Maximize | Minimize]) -> None:
        x, y = DataFrame([2.0]), _get_variable(objective)
        assert isclose(multiply(x, y).value, multiply(x, y.value))
        assert isclose(multiply(y, x).value, multiply(y.value, x))


class TestNegate:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(0.0, -0.0),
            param(1.0, -1.0),
            param(-1.0, 1.0),
            param(array([0.0]), array([-0.0])),
            param(array([1.0]), array([-1.0])),
            param(array([-1.0]), array([1.0])),
        ],
    )
    def test_float_and_array(
        self, *, x: float | NDArrayF, expected: float | NDArrayF
    ) -> None:
        assert_equal(negate(x), expected)

    @mark.parametrize(
        ("x", "expected"),
        [
            param(Series([0.0]), Series([0.0])),
            param(Series([1.0]), Series([-1.0])),
            param(Series([-1.0]), Series([1.0])),
        ],
    )
    def test_series(self, *, x: SeriesF, expected: SeriesF) -> None:
        assert_series_equal(negate(x), expected)

    @mark.parametrize(
        ("x", "expected"),
        [
            param(DataFrame([0.0]), DataFrame([0.0])),
            param(DataFrame([1.0]), DataFrame([-1.0])),
            param(DataFrame([-1.0]), DataFrame([1.0])),
        ],
    )
    def test_dataframe(self, *, x: DataFrame, expected: DataFrame) -> None:
        assert_frame_equal(negate(x), expected)

    @mark.parametrize("objective", [param(Maximize), param(Minimize)])
    def test_expression(self, *, objective: type[Maximize | Minimize]) -> None:
        var = _get_variable(objective)
        assert_equal(negate(var).value, negate(var.value))


class TestNegative:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(0.0, 0.0),
            param(1.0, 0.0),
            param(-1.0, 1.0),
            param(array([0.0]), array([0.0])),
            param(array([1.0]), array([0.0])),
            param(array([-1.0]), array([1.0])),
        ],
    )
    def test_float_and_array(
        self, *, x: float | NDArrayF, expected: float | NDArrayF
    ) -> None:
        assert_equal(negative(x), expected)

    @mark.parametrize(
        ("x", "expected"),
        [
            param(Series([0.0]), Series([0.0])),
            param(Series([1.0]), Series([0.0])),
            param(Series([-1.0]), Series([1.0])),
        ],
    )
    def test_series(self, *, x: SeriesF, expected: SeriesF) -> None:
        assert_series_equal(negative(x), expected)

    @mark.parametrize(
        ("x", "expected"),
        [
            param(DataFrame([0.0]), DataFrame([0.0])),
            param(DataFrame([1.0]), DataFrame([0.0])),
            param(DataFrame([-1.0]), DataFrame([1.0])),
        ],
    )
    def test_dataframe(self, *, x: DataFrame, expected: DataFrame) -> None:
        assert_frame_equal(negative(x), expected)

    @mark.parametrize("objective", [param(Maximize), param(Minimize)])
    def test_expression(self, *, objective: type[Maximize | Minimize]) -> None:
        var = _get_variable(objective)
        assert isclose(negative(var).value, negative(var.value))


class TestNorm:
    @mark.parametrize("x", [param(array([2.0, 3.0])), param(Series([2.0, 3.0]))])
    def test_array_and_series(self, *, x: NDArrayF | SeriesF) -> None:
        assert isclose(norm(x), np.sqrt(13))

    @mark.parametrize("objective", [param(Maximize), param(Minimize)])
    @mark.parametrize("shape", [param((2,)), param((2, 2))])
    def test_expression(
        self, *, objective: type[Maximize | Minimize], shape: tuple[int, ...]
    ) -> None:
        var = _get_variable(objective, shape=shape)
        assert isclose(norm(var).value, norm(var.value))


class TestPositive:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(0.0, 0.0),
            param(1.0, 1.0),
            param(-1.0, 0.0),
            param(array([0.0]), array([0.0])),
            param(array([1.0]), array([1.0])),
            param(array([-1.0]), array([0.0])),
        ],
    )
    def test_float_and_array(
        self, *, x: float | NDArrayF, expected: float | NDArrayF
    ) -> None:
        assert_equal(positive(x), expected)

    @mark.parametrize(
        ("x", "expected"),
        [
            param(Series([0.0]), Series([0.0])),
            param(Series([1.0]), Series([1.0])),
            param(Series([-1.0]), Series([0.0])),
        ],
    )
    def test_series(self, *, x: SeriesF, expected: SeriesF) -> None:
        assert_series_equal(positive(x), expected)

    @mark.parametrize(
        ("x", "expected"),
        [
            param(DataFrame([0.0]), DataFrame([0.0])),
            param(DataFrame([1.0]), DataFrame([1.0])),
            param(DataFrame([-1.0]), DataFrame([0.0])),
        ],
    )
    def test_dataframe(self, *, x: DataFrame, expected: DataFrame) -> None:
        assert_frame_equal(positive(x), expected)

    @mark.parametrize("objective", [param(Maximize), param(Minimize)])
    def test_expression(self, *, objective: type[Maximize | Minimize]) -> None:
        var = _get_variable(objective)
        assert_equal(positive(var).value, positive(var.value))


class TestPower:
    @mark.parametrize(
        ("x", "p", "expected"),
        [
            param(0.0, 0.0, 1.0),
            param(2.0, 3.0, 8.0),
            param(2.0, array([3.0]), array([8.0])),
            param(array([2.0]), 3.0, array([8.0])),
            param(array([2.0]), array([3.0]), array([8.0])),
        ],
    )
    def test_float_and_array(
        self, *, x: float | NDArrayF, p: float | NDArrayF, expected: float | NDArrayF
    ) -> None:
        assert_equal(power(x, p), expected)

    @mark.parametrize("objective", [param(Maximize), param(Minimize)])
    def test_one_expression(self, *, objective: type[Maximize | Minimize]) -> None:
        var = _get_variable(objective)
        assert_equal(power(var, 2.0).value, power(var.value, 2.0))


class TestQuadForm:
    def test_array(self) -> None:
        assert_equal(
            quad_form(array([2.0, 3.0]), array([[4.0, 5.0], [5.0, 4.0]])), 112.0
        )

    @mark.parametrize("objective", [param(Maximize), param(Minimize)])
    def test_expression(self, *, objective: type[Maximize | Minimize]) -> None:
        var = _get_variable(objective, shape=(2,))
        P = array([[2.0, 3.0], [3.0, 2.0]])  # noqa: N806
        assert_equal(quad_form(var, P).value, quad_form(var.value, P))


class TestScalarProduct:
    @mark.parametrize(
        "x",
        [
            param(2.0),
            param(array([2.0])),
            param(Series([2.0])),
            param(DataFrame([2.0])),
        ],
    )
    @mark.parametrize(
        "y",
        [
            param(3.0),
            param(array([3.0])),
            param(Series([3.0])),
            param(DataFrame([3.0])),
        ],
    )
    def test_two_floats_arrays_and_ndframes(
        self,
        *,
        x: float | NDArrayF | SeriesF | DataFrame,
        y: float | NDArrayF | SeriesF | DataFrame,
    ) -> None:
        if (isinstance(x, Series) and isinstance(y, DataFrame)) or (
            isinstance(x, DataFrame) and isinstance(y, Series)
        ):
            match = re.compile(
                r".* must not be between a Series and DataFrame; got .* and .*\.",
                flags=DOTALL,
            )
            with raises(ScalarProductError, match=match):
                _ = scalar_product(cast(Any, x), cast(Any, y))
            with raises(ScalarProductError, match=match):
                _ = scalar_product(cast(Any, y), cast(Any, x))
        else:
            assert isclose(scalar_product(cast(Any, y), cast(Any, x)), 6.0)
            assert isclose(scalar_product(cast(Any, x), cast(Any, y)), 6.0)

    @mark.parametrize(
        ("x", "shape"),
        [
            param(2.0, None),
            param(2.0, (2,)),
            param(2.0, (2, 2)),
            param(array([2.0]), None),
            param(array([2.0]), (1,)),
            param(array([2.0]), (2,)),
            param(array([2.0]), (1, 2)),
            param(array([2.0]), (2, 2)),
            param(Series([2.0]), None),
            param(Series([2.0]), (1,)),
            param(DataFrame([2.0]), None),
            param(DataFrame([2.0]), (1,)),
            param(DataFrame([2.0]), (1, 1)),
        ],
    )
    @mark.parametrize("objective", [param(Maximize), param(Minimize)])
    def test_one_float_array_and_ndframe_and_one_expression(
        self,
        *,
        x: float | NDArrayF,
        objective: type[Maximize | Minimize],
        shape: tuple[int, ...] | None,
    ) -> None:
        y = _get_variable(objective, shape=shape)
        assert isclose(scalar_product(x, y).value, scalar_product(x, y.value))
        assert isclose(scalar_product(y, x).value, scalar_product(y.value, x))

    @mark.parametrize("objective1", [param(Maximize), param(Minimize)])
    @mark.parametrize("objective2", [param(Maximize), param(Minimize)])
    def test_two_expressions(
        self,
        *,
        objective1: type[Maximize | Minimize],
        objective2: type[Maximize | Minimize],
    ) -> None:
        var1 = _get_variable(objective1)
        var2 = _get_variable(objective2)
        assert isclose(
            scalar_product(var1, var2).value, scalar_product(var1.value, var2.value)
        )


class TestSolve:
    def test_main(self) -> None:
        var = Variable()
        problem = Problem(Minimize(sum_(abs_(var))), [])
        _ = solve(problem, solver=CLARABEL)

    def test_infeasible_problem(self) -> None:
        var = Variable()
        threshold = 1.0
        problem = Problem(
            Minimize(sum_(abs_(var))),
            [cast(Any, var) >= threshold, cast(Any, var) <= -threshold],
        )
        with raises(SolveInfeasibleError):
            _ = solve(problem, solver=CLARABEL)

    def test_unbounded_problem(self) -> None:
        var = Variable()
        problem = Problem(Maximize(sum_(var)), [])
        with raises(SolveUnboundedError):
            _ = solve(problem, solver=CLARABEL)


class TestSqrt:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(0.0, 0.0),
            param(1.0, 1.0),
            param(array([0.0]), array([0.0])),
            param(array([1.0]), array([1.0])),
        ],
    )
    def test_float_and_array(
        self, *, x: float | NDArrayF, expected: float | NDArrayF
    ) -> None:
        assert_equal(sqrt(x), expected)

    @mark.parametrize(
        ("x", "expected"),
        [param(Series([0.0]), Series([0.0])), param(Series([1.0]), Series([1.0]))],
    )
    def test_series(self, *, x: SeriesF, expected: SeriesF) -> None:
        assert_series_equal(sqrt(x), expected)

    @mark.parametrize(
        ("x", "expected"),
        [
            param(DataFrame([0.0]), DataFrame([0.0])),
            param(DataFrame([1.0]), DataFrame([1.0])),
        ],
    )
    def test_dataframe(self, *, x: DataFrame, expected: DataFrame) -> None:
        assert_frame_equal(sqrt(x), expected)

    def test_expression(self) -> None:
        var = _get_variable(Maximize)
        assert isclose(sqrt(var).value, sqrt(var.value))


class TestSubtract:
    @mark.parametrize(
        ("x", "y", "expected"),
        [
            param(1.0, 2.0, -1.0),
            param(1.0, array([2.0]), array([-1.0])),
            param(array([1.0]), 2.0, array([-1.0])),
            param(array([1.0]), array([2.0]), array([-1.0])),
        ],
    )
    def test_float_and_array(
        self, *, x: float | NDArrayF, y: float | NDArrayF, expected: float | NDArrayF
    ) -> None:
        assert_equal(subtract(x, y), expected)

    @mark.parametrize("x", [param(1.0), param(array([1.0]))])
    @mark.parametrize("objective", [param(Maximize), param(Minimize)])
    def test_one_expression(
        self, *, x: float | NDArrayF | Expression, objective: type[Maximize | Minimize]
    ) -> None:
        var = _get_variable(objective)
        assert_equal(subtract(x, var).value, subtract(x, var.value))
        assert_equal(subtract(var, x).value, subtract(var.value, x))

    @mark.parametrize("objective1", [param(Maximize), param(Minimize)])
    @mark.parametrize("objective2", [param(Maximize), param(Minimize)])
    def test_two_expressions(
        self,
        *,
        objective1: type[Maximize | Minimize],
        objective2: type[Maximize | Minimize],
    ) -> None:
        var1 = _get_variable(objective1)
        var2 = _get_variable(objective2)
        assert_equal(subtract(var1, var2).value, subtract(var1.value, var2.value))


class TestSum:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(0.0, 0.0),
            param(1.0, 1.0),
            param(-1.0, -1.0),
            param(array([0.0]), 0.0),
            param(array([1.0]), 1.0),
            param(array([-1.0]), -1.0),
            param(array([[0.0, 0.0]]), 0.0),
            param(array([[1.0, 1.0]]), 2.0),
            param(array([[-1.0, -1.0]]), -2.0),
            param(Series([0.0]), 0.0),
            param(Series([1.0]), 1.0),
            param(Series([-1.0]), -1.0),
            param(DataFrame([0.0]), 0.0),
            param(DataFrame([1.0]), 1.0),
            param(DataFrame([-1.0]), -1.0),
        ],
    )
    def test_float_array_and_ndframe(
        self, *, x: float | NDArrayF | SeriesF | DataFrame, expected: float
    ) -> None:
        assert isclose(sum_(x), expected)

    def test_expression(self) -> None:
        var = _get_variable(Maximize)
        assert_equal(sum_(var).value, sum_(var.value))


class TestSum0And1:
    def test_array(self) -> None:
        x = array([[1.0, 2.0], [3.0, 4.0]])
        assert_equal(sum_axis0(x), array([4.0, 6.0]))
        assert_equal(sum_axis1(x), array([3.0, 7.0]))

    def test_dataframe(self) -> None:
        x = DataFrame([[1.0, 2.0], [3.0, 4.0]])
        assert_series_equal(sum_axis0(x), Series([4.0, 6.0]))
        assert_series_equal(sum_axis1(x), Series([3.0, 7.0]))

    def test_expression(self) -> None:
        var = _get_variable(Maximize, shape=(2, 2))
        assert_equal(sum_axis0(var).value, sum_axis0(var.value))
        assert_equal(sum_axis1(var).value, sum_axis1(var.value))
