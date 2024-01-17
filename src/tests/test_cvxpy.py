from __future__ import annotations

from functools import cache
from typing import Any, cast

import cvxpy
import numpy as np
from cvxpy import Expression, Maximize, Minimize, Problem, Variable
from numpy import array, isclose
from numpy.testing import assert_equal
from pandas import DataFrame, Series
from pandas.testing import assert_frame_equal, assert_series_equal
from pytest import mark, param, raises

from utilities.cvxpy import (
    MultiplyError,
    SolveInfeasibleError,
    SolveUnboundedError,
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
)
from utilities.numpy import NDArrayF
from utilities.pandas import SeriesF


@cache
def _get_variable(
    objective: type[Maximize] | type[Minimize],  # noqa: PYI055
    /,
    *,
    array: bool = False,
) -> Variable:
    if array:
        var = Variable(2)
        scalar = cvxpy.sum(var)
    else:
        var = Variable()
        scalar = var
    threshold = 10.0
    problem = Problem(
        objective(scalar), [cast(Any, var) >= -threshold, cast(Any, var) <= threshold]
    )
    _ = problem.solve()
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
        assert isclose(add(x, var).value, add(x, var.value))
        assert isclose(add(var, x).value, add(var.value, x))

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


class TestMaximum:
    @mark.parametrize(
        ("x", "y", "expected"),
        [
            param(2.0, 3.0, 3.0),
            param(2.0, array([3.0]), array([3.0])),
            param(array([2.0]), 3.0, array([3.0])),
            param(array([2.0]), array([3.0]), array([3.0])),
        ],
    )
    def test_float_and_array(
        self, *, x: float | NDArrayF, y: float | NDArrayF, expected: float | NDArrayF
    ) -> None:
        assert_equal(maximum(x, y), expected)

    @mark.parametrize("x", [param(2.0), param(array([2.0]))])
    @mark.parametrize("objective", [param(Maximize), param(Minimize)])
    def test_one_expression(
        self, *, x: float | NDArrayF | Expression, objective: type[Maximize | Minimize]
    ) -> None:
        var = _get_variable(objective)
        assert_equal(maximum(x, var).value, maximum(x, var.value))
        assert_equal(maximum(var, x).value, maximum(var.value, x))

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
        assert_equal(maximum(var1, var2).value, maximum(var1.value, var2.value))


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
        var = _get_variable(objective, array=True)
        assert isclose(min_(var).value, min_(var.value))


class TestMinimum:
    @mark.parametrize(
        ("x", "y", "expected"),
        [
            param(2.0, 3.0, 2.0),
            param(2.0, array([3.0]), array([2.0])),
            param(array([2.0]), 3.0, array([2.0])),
            param(array([2.0]), array([3.0]), array([2.0])),
        ],
    )
    def test_float_and_array(
        self, *, x: float | NDArrayF, y: float | NDArrayF, expected: float | NDArrayF
    ) -> None:
        assert_equal(minimum(x, y), expected)

    @mark.parametrize("x", [param(2.0), param(array([2.0]))])
    @mark.parametrize("objective", [param(Maximize), param(Minimize)])
    def test_one_expression(
        self, *, x: float | NDArrayF | Expression, objective: type[Maximize | Minimize]
    ) -> None:
        var = _get_variable(objective)
        assert_equal(minimum(x, var).value, minimum(x, var.value))
        assert_equal(minimum(var, x).value, minimum(var.value, x))

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
        assert_equal(minimum(var1, var2).value, minimum(var1.value, var2.value))


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
    @mark.parametrize("array1", [param(True), param(False)])
    @mark.parametrize("objective2", [param(Maximize), param(Minimize)])
    @mark.parametrize("array2", [param(True), param(False)])
    def test_two_expressions(
        self,
        *,
        objective1: type[Maximize | Minimize],
        array1: bool,
        objective2: type[Maximize | Minimize],
        array2: bool,
    ) -> None:
        var1 = _get_variable(objective1, array=array1)
        var2 = _get_variable(objective2, array=array2)
        assert_equal(multiply(var1, var2).value, multiply(var1.value, var2.value))

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
    @mark.parametrize("array", [param(True), param(False)])
    def test_float_and_expr(
        self, *, objective: type[Maximize | Minimize], array: bool
    ) -> None:
        x, y = 2.0, _get_variable(objective, array=array)
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
        with raises(MultiplyError):
            _ = multiply(cast(Any, x), cast(Any, y))
        with raises(MultiplyError):
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
    def test_expression(
        self,
        *,
        objective: type[Maximize] | type[Minimize],  # noqa: PYI055
    ) -> None:
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

    @mark.parametrize("objective", [param(Maximize), param(Minimize)])
    def test_expression(
        self,
        *,
        objective: type[Maximize] | type[Minimize],  # noqa: PYI055
    ) -> None:
        var = _get_variable(objective)
        assert isclose(negative(var).value, negative(var.value))


class TestNorm:
    @mark.parametrize("x", [param(array([2.0, 3.0])), param(Series([2.0, 3.0]))])
    def test_array_and_series(self, *, x: NDArrayF | SeriesF) -> None:
        assert isclose(norm(x), np.sqrt(13))

    @mark.parametrize("objective", [param(Maximize), param(Minimize)])
    def test_expression(self, *, objective: type[Maximize] | type[Minimize]) -> None:  # noqa: PYI055
        var = _get_variable(objective, array=True)
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
        var = _get_variable(objective, array=True)
        P = array([[2.0, 3.0], [3.0, 2.0]])  # noqa: N806
        assert_equal(quad_form(var, P).value, quad_form(var.value, P))


class TestScalarProduct:
    @mark.parametrize(
        ("x", "y", "expected"),
        [
            param(1.0, 2.0, 2.0),
            param(1.0, array([2.0]), 2.0),
            param(array([1.0]), 2.0, 2.0),
            param(array([1.0]), array([2.0]), 2.0),
        ],
    )
    def test_float_and_array(
        self, *, x: float | NDArrayF, y: float | NDArrayF, expected: float | NDArrayF
    ) -> None:
        assert_equal(scalar_product(x, y), expected)

    @mark.parametrize("objective1", [param(Maximize), param(Minimize)])
    @mark.parametrize("objective2", [param(Maximize), param(Minimize)])
    def test_two_expressions(
        self,
        *,
        objective1: type[Maximize | Minimize],
        objective2: type[Maximize | Minimize],
    ) -> None:
        var1 = _get_variable(objective1, array=True)
        var2 = _get_variable(objective2, array=True)
        assert_equal(
            scalar_product(var1, var2).value, scalar_product(var1.value, var2.value)
        )


class TestSolve:
    def test_main(self) -> None:
        var = Variable()
        problem = Problem(Minimize(sum_(abs_(var))), [])
        _ = solve(problem)

    def test_infeasible_problem(self) -> None:
        var = Variable()
        threshold = 1.0
        problem = Problem(
            Minimize(sum_(abs_(var))),
            [cast(Any, var) >= threshold, cast(Any, var) <= -threshold],
        )
        with raises(SolveInfeasibleError):
            _ = solve(problem)

    def test_unbounded_problem(self) -> None:
        var = Variable()
        problem = Problem(Maximize(sum_(var)), [])
        with raises(SolveUnboundedError):
            _ = solve(problem)


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
        assert_equal(sum_(x), expected)

    def test_expression(self) -> None:
        var = _get_variable(Maximize)
        assert_equal(sum_(var).value, sum_(var.value))
