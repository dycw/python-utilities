from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import cvxpy
import numpy as np
from cvxpy import CLARABEL, Expression, Maximize, Minimize, Problem, Variable
from hypothesis import given
from hypothesis.strategies import just, none, sampled_from
from numpy import array, float64, isclose, ndarray
from numpy.testing import assert_equal
from pytest import raises

from utilities.cvxpy import (
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
    sum_axis0,
    sum_axis1,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from utilities.numpy import NDArrayF


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
        objective(scalar),
        [cast("Any", var) >= -threshold, cast("Any", var) <= threshold],
    )
    _ = problem.solve(solver=CLARABEL)
    return var


class TestAbs:
    @given(case=sampled_from([(0.0, 0.0), (1.0, 1.0), (-1.0, 1.0)]))
    def test_float(self, *, case: tuple[float, float]) -> None:
        x, expected = case
        assert isclose(abs_(x), expected)

    @given(
        case=sampled_from([
            (array([0.0]), array([0.0])),
            (array([1.0]), array([1.0])),
            (array([-1.0]), array([1.0])),
        ])
    )
    def test_array(self, *, case: tuple[NDArrayF, NDArrayF]) -> None:
        x, expected = case
        assert_equal(abs_(x), expected)

    @given(objective=sampled_from([Maximize, Minimize]))
    def test_expression(self, *, objective: type[Maximize | Minimize]) -> None:
        var = _get_variable(objective)
        assert var.value is not None
        assert_equal(abs_(var).value, abs_(var.value))


class TestAdd:
    @given(
        case=sampled_from([
            (1.0, 2.0, 3.0),
            (1.0, array([2.0]), array([3.0])),
            (array([1.0]), 2.0, array([3.0])),
            (array([1.0]), array([2.0]), array([3.0])),
        ])
    )
    def test_float_and_array(
        self, *, case: tuple[float | NDArrayF, float | NDArrayF, float | NDArrayF]
    ) -> None:
        x, y, expected = case
        assert_equal(add(x, y), expected)

    @given(
        x=sampled_from([1.0, array([1.0])]),
        objective=sampled_from([Maximize, Minimize]),
    )
    def test_one_expression(
        self, *, x: float | NDArrayF | Expression, objective: type[Maximize | Minimize]
    ) -> None:
        var = _get_variable(objective)
        result1 = add(x, var).value
        assert result1 is not None
        assert var.value is not None
        result2 = add(x, var.value)
        assert isinstance(result2, float64 | ndarray)
        assert isclose(result1, result2)

    @given(
        objective1=sampled_from([Maximize, Minimize]),
        objective2=sampled_from([Maximize, Minimize]),
    )
    def test_two_expressions(
        self,
        *,
        objective1: type[Maximize | Minimize],
        objective2: type[Maximize | Minimize],
    ) -> None:
        var1 = _get_variable(objective1)
        var2 = _get_variable(objective2)
        assert var1.value is not None
        assert var2.value is not None
        assert_equal(add(var1, var2).value, add(var1.value, var2.value))


class TestDivide:
    @given(
        case=sampled_from([
            (1.0, 2.0, 0.5),
            (1.0, array([2.0]), array([0.5])),
            (array([1.0]), 2.0, array([0.5])),
            (array([1.0]), array([2.0]), array([0.5])),
        ])
    )
    def test_float_and_array(
        self, *, case: tuple[float | NDArrayF, float | NDArrayF, float | NDArrayF]
    ) -> None:
        x, y, expected = case
        assert_equal(divide(x, y), expected)

    @given(
        x=sampled_from([1.0, array([1.0])]),
        objective=sampled_from([Maximize, Minimize]),
    )
    def test_one_expression(
        self, *, x: float | NDArrayF | Expression, objective: type[Maximize | Minimize]
    ) -> None:
        var = _get_variable(objective)
        assert var.value is not None
        assert_equal(divide(x, var).value, divide(x, var.value))
        assert_equal(divide(var, x).value, divide(var.value, x))

    @given(
        objective1=sampled_from([Maximize, Minimize]),
        objective2=sampled_from([Maximize, Minimize]),
    )
    def test_two_expressions(
        self,
        *,
        objective1: type[Maximize | Minimize],
        objective2: type[Maximize | Minimize],
    ) -> None:
        var1 = _get_variable(objective1)
        var2 = _get_variable(objective2)
        assert var1.value is not None
        assert var2.value is not None
        assert_equal(divide(var1, var2).value, divide(var1.value, var2.value))


class TestMax:
    @given(
        case=sampled_from([
            (0.0, 0.0),
            (array([1.0, 2.0]), 2.0),
            (array([-1.0, -2.0]), -1.0),
        ])
    )
    def test_float_or_array(self, *, case: tuple[float | NDArrayF, float]) -> None:
        x, expected = case
        assert_equal(max_(x), expected)

    @given(objective=sampled_from([Maximize, Minimize]))
    def test_expression(self, *, objective: type[Maximize | Minimize]) -> None:
        var = _get_variable(objective)
        result1 = max_(var).value
        assert result1 is not None
        assert var.value is not None
        result2 = max_(var.value)
        assert result2 is not None
        assert isclose(result1, result2)


class TestMaximumAndMinimum:
    @given(case=sampled_from([(maximum, 3.0), (minimum, 2.0)]))
    def test_two_floats(self, *, case: tuple[Callable[..., Any], float]) -> None:
        func, expected = case
        assert isclose(func(2.0, 3.0), expected)

    @given(case=sampled_from([(maximum, 3.0), (minimum, 2.0)]))
    def test_two_arrays(self, *, case: tuple[Callable[..., Any], float]) -> None:
        func, expected = case
        assert_equal(func(array([2.0]), array([3.0])), array([expected]))

    @given(
        func=sampled_from([maximum, minimum]),
        objective1=sampled_from([Maximize, Minimize]),
        objective2=sampled_from([Maximize, Minimize]),
    )
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

    @given(
        func=sampled_from([maximum, minimum]),
        objective=sampled_from([Maximize, Minimize]),
        shape=just((2, 2)) | none(),
    )
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

    @given(
        func=sampled_from([maximum, minimum]),
        objective=sampled_from([Maximize, Minimize]),
    )
    def test_array_and_expr(
        self, *, func: Callable[..., Any], objective: type[Maximize | Minimize]
    ) -> None:
        x, y = array([2.0]), _get_variable(objective)
        assert isclose(func(x, y).value, func(x, y.value))
        assert isclose(func(y, x).value, func(y.value, x))


class TestMin:
    @given(
        case=sampled_from([
            (0.0, 0.0),
            (array([1.0, 2.0]), 1.0),
            (array([-1.0, -2.0]), -2.0),
        ])
    )
    def test_float_or_array(self, *, case: tuple[float | NDArrayF, float]) -> None:
        x, expected = case
        assert isclose(min_(x), expected)

    @given(objective=sampled_from([Maximize, Minimize]))
    def test_expression(self, *, objective: type[Maximize | Minimize]) -> None:
        var = _get_variable(objective, shape=(2,))
        result1 = min_(var).value
        assert result1 is not None
        assert var.value is not None
        result2 = min_(var.value)
        assert result2 is not None
        assert isclose(result1, result2)


class TestMultiply:
    def test_two_floats(self) -> None:
        assert isclose(multiply(2.0, 3.0), 6.0)

    def test_two_arrays(self) -> None:
        assert_equal(multiply(array([2.0]), array([3.0])), array([6.0]))

    @given(
        objective1=sampled_from([Maximize, Minimize]),
        objective2=sampled_from([Maximize, Minimize]),
    )
    def test_two_expressions(
        self,
        *,
        objective1: type[Maximize | Minimize],
        objective2: type[Maximize | Minimize],
    ) -> None:
        var1 = _get_variable(objective1)
        var2 = _get_variable(objective2)
        result1 = multiply(var1, var2).value
        assert result1 is not None
        assert var1.value is not None
        assert var2.value is not None
        result2 = multiply(var1.value, var2.value)
        assert result2 is not None
        assert isclose(result1, result2)

    def test_float_and_array(self) -> None:
        x, y, expected = 2.0, array([3.0]), array([6.0])
        assert_equal(multiply(x, y), expected)
        assert_equal(multiply(y, x), expected)

    @given(objective=sampled_from([Maximize, Minimize]), shape=just((2, 2)) | none())
    def test_float_and_expr(
        self, *, objective: type[Maximize | Minimize], shape: tuple[int, ...] | None
    ) -> None:
        x, y = 2.0, _get_variable(objective, shape=shape)
        assert y.value is not None
        assert_equal(multiply(x, y).value, multiply(x, y.value))
        assert_equal(multiply(y, x).value, multiply(y.value, x))

    @given(objective=sampled_from([Maximize, Minimize]))
    def test_array_and_expr(self, *, objective: type[Maximize | Minimize]) -> None:
        x, y = array([2.0]), _get_variable(objective)
        assert y.value is not None
        result1 = multiply(x, y).value
        assert result1 is not None
        result2 = multiply(x, y.value)
        assert result2 is not None
        assert isclose(result1, result2)
        result3 = multiply(x, y.value)
        assert result3 is not None
        assert isclose(result1, result3)


class TestNegate:
    @given(
        case=sampled_from([
            (0.0, -0.0),
            (1.0, -1.0),
            (-1.0, 1.0),
            (array([0.0]), array([-0.0])),
            (array([1.0]), array([-1.0])),
            (array([-1.0]), array([1.0])),
        ])
    )
    def test_float_and_array(
        self, *, case: tuple[float | NDArrayF, float | NDArrayF]
    ) -> None:
        x, expected = case
        assert_equal(negate(x), expected)

    @given(objective=sampled_from([Maximize, Minimize]))
    def test_expression(self, *, objective: type[Maximize | Minimize]) -> None:
        var = _get_variable(objective)
        assert var.value is not None
        assert_equal(negate(var).value, negate(var.value))


class TestNegative:
    @given(
        case=sampled_from([
            (0.0, 0.0),
            (1.0, 0.0),
            (-1.0, 1.0),
            (array([0.0]), array([0.0])),
            (array([1.0]), array([0.0])),
            (array([-1.0]), array([1.0])),
        ])
    )
    def test_float_and_array(
        self, *, case: tuple[float | NDArrayF, float | NDArrayF]
    ) -> None:
        x, expected = case
        assert_equal(negative(x), expected)

    @given(objective=sampled_from([Maximize, Minimize]))
    def test_expression(self, *, objective: type[Maximize | Minimize]) -> None:
        var = _get_variable(objective)
        result1 = negative(var).value
        assert result1 is not None
        assert var.value is not None
        result2 = negative(var.value)
        assert result2 is not None
        assert isclose(result1, result2)


class TestNorm:
    def test_array(self) -> None:
        assert isclose(norm(array([2.0, 3.0])), np.sqrt(13))

    @given(
        objective=sampled_from([Maximize, Minimize]), shape=sampled_from([(2,), (2, 2)])
    )
    def test_expression(
        self, *, objective: type[Maximize | Minimize], shape: tuple[int, ...]
    ) -> None:
        var = _get_variable(objective, shape=shape)
        result1 = norm(var).value
        assert result1 is not None
        assert var.value is not None
        result2 = norm(var.value)
        assert result2 is not None
        assert isclose(result1, result2)


class TestPositive:
    @given(
        case=sampled_from([
            (0.0, 0.0),
            (1.0, 1.0),
            (-1.0, 0.0),
            (array([0.0]), array([0.0])),
            (array([1.0]), array([1.0])),
            (array([-1.0]), array([0.0])),
        ])
    )
    def test_float_and_array(
        self, *, case: tuple[float | NDArrayF, float | NDArrayF]
    ) -> None:
        x, expected = case
        assert_equal(positive(x), expected)

    @given(objective=sampled_from([Maximize, Minimize]))
    def test_expression(self, *, objective: type[Maximize | Minimize]) -> None:
        var = _get_variable(objective)
        assert var.value is not None
        assert_equal(positive(var).value, positive(var.value))


class TestPower:
    @given(
        case=sampled_from([
            (0.0, 0.0, 1.0),
            (2.0, 3.0, 8.0),
            (2.0, array([3.0]), array([8.0])),
            (array([2.0]), 3.0, array([8.0])),
            (array([2.0]), array([3.0]), array([8.0])),
        ])
    )
    def test_float_and_array(
        self, *, case: tuple[float | NDArrayF, float | NDArrayF, float | NDArrayF]
    ) -> None:
        x, p, expected = case
        assert_equal(power(x, p), expected)

    @given(objective=sampled_from([Maximize, Minimize]))
    def test_one_expression(self, *, objective: type[Maximize | Minimize]) -> None:
        var = _get_variable(objective)
        assert var.value is not None
        assert_equal(power(var, 2.0).value, power(var.value, 2.0))


class TestQuadForm:
    def test_array(self) -> None:
        assert_equal(
            quad_form(array([2.0, 3.0]), array([[4.0, 5.0], [5.0, 4.0]])), 112.0
        )

    @given(objective=sampled_from([Maximize, Minimize]))
    def test_expression(self, *, objective: type[Maximize | Minimize]) -> None:
        var = _get_variable(objective, shape=(2,))
        P = array([[2.0, 3.0], [3.0, 2.0]])  # noqa: N806
        assert var.value is not None
        assert_equal(quad_form(var, P).value, quad_form(var.value, P))


class TestScalarProduct:
    @given(x=sampled_from([2.0, array([2.0])]), y=sampled_from([3.0, array([3.0])]))
    def test_two_floats_or_arrays(
        self, *, x: float | NDArrayF, y: float | NDArrayF
    ) -> None:
        assert isclose(scalar_product(x, y), 6.0)
        assert isclose(scalar_product(y, x), 6.0)

    @given(
        case=sampled_from([
            (2.0, None),
            (2.0, (2,)),
            (2.0, (2, 2)),
            (array([2.0]), None),
            (array([2.0]), (1,)),
            (array([2.0]), (2,)),
            (array([2.0]), (1, 2)),
            (array([2.0]), (2, 2)),
        ]),
        objective=sampled_from([Maximize, Minimize]),
    )
    def test_one_float_array_and_one_expression(
        self,
        *,
        case: tuple[float | NDArrayF, tuple[int, ...] | None],
        objective: type[Maximize | Minimize],
    ) -> None:
        x, shape = case
        y = _get_variable(objective, shape=shape)
        result1 = scalar_product(x, y).value
        assert result1 is not None
        assert y.value is not None
        result2 = scalar_product(x, y.value)
        assert result2 is not None
        assert isclose(result1, result2)

    @given(
        objective1=sampled_from([Maximize, Minimize]),
        objective2=sampled_from([Maximize, Minimize]),
    )
    def test_two_expressions(
        self,
        *,
        objective1: type[Maximize | Minimize],
        objective2: type[Maximize | Minimize],
    ) -> None:
        var1 = _get_variable(objective1)
        var2 = _get_variable(objective2)
        result1 = scalar_product(var1, var2).value
        assert result1 is not None
        assert var1.value is not None
        assert var2.value is not None
        result2 = scalar_product(var1.value, var2.value)
        assert isclose(result1, result2)


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
            [cast("Any", var) >= threshold, cast("Any", var) <= -threshold],
        )
        with raises(SolveInfeasibleError):
            _ = solve(problem, solver=CLARABEL)

    def test_unbounded_problem(self) -> None:
        var = Variable()
        problem = Problem(Maximize(sum_(var)), [])
        with raises(SolveUnboundedError):
            _ = solve(problem, solver=CLARABEL)


class TestSqrt:
    @given(
        case=sampled_from([
            (0.0, 0.0),
            (1.0, 1.0),
            (array([0.0]), array([0.0])),
            (array([1.0]), array([1.0])),
        ])
    )
    def test_float_and_array(
        self, *, case: tuple[float | NDArrayF, float | NDArrayF]
    ) -> None:
        x, expected = case
        assert_equal(sqrt(x), expected)

    def test_expression(self) -> None:
        var = _get_variable(Maximize)
        result1 = sqrt(var).value
        assert result1 is not None
        assert var.value is not None
        result2 = sqrt(var.value)
        assert result2 is not None
        assert isclose(result1, result2)


class TestSubtract:
    @given(
        case=sampled_from([
            (1.0, 2.0, -1.0),
            (1.0, array([2.0]), array([-1.0])),
            (array([1.0]), 2.0, array([-1.0])),
            (array([1.0]), array([2.0]), array([-1.0])),
        ])
    )
    def test_float_and_array(
        self, *, case: tuple[float | NDArrayF, float | NDArrayF, float | NDArrayF]
    ) -> None:
        x, y, expected = case
        assert_equal(subtract(x, y), expected)

    @given(
        x=sampled_from([1.0, array([1.0])]),
        objective=sampled_from([Maximize, Minimize]),
    )
    def test_one_expression(
        self, *, x: float | NDArrayF | Expression, objective: type[Maximize | Minimize]
    ) -> None:
        var = _get_variable(objective)
        assert var.value is not None
        assert_equal(subtract(x, var).value, subtract(x, var.value))
        assert_equal(subtract(var, x).value, subtract(var.value, x))

    @given(
        objective1=sampled_from([Maximize, Minimize]),
        objective2=sampled_from([Maximize, Minimize]),
    )
    def test_two_expressions(
        self,
        *,
        objective1: type[Maximize | Minimize],
        objective2: type[Maximize | Minimize],
    ) -> None:
        var1 = _get_variable(objective1)
        var2 = _get_variable(objective2)
        assert var1.value is not None
        assert var2.value is not None
        assert_equal(subtract(var1, var2).value, subtract(var1.value, var2.value))


class TestSum:
    @given(
        case=sampled_from([
            (0.0, 0.0),
            (1.0, 1.0),
            (-1.0, -1.0),
            (array([0.0]), 0.0),
            (array([1.0]), 1.0),
            (array([-1.0]), -1.0),
            (array([[0.0, 0.0]]), 0.0),
            (array([[1.0, 1.0]]), 2.0),
            (array([[-1.0, -1.0]]), -2.0),
        ])
    )
    def test_float_or_array(self, *, case: tuple[float | NDArrayF, float]) -> None:
        x, expected = case
        assert isclose(sum_(x), expected)

    def test_expression(self) -> None:
        var = _get_variable(Maximize)
        assert var.value is not None
        assert_equal(sum_(var).value, sum_(var.value))


class TestSum0And1:
    def test_array(self) -> None:
        x = array([[1.0, 2.0], [3.0, 4.0]])
        assert_equal(sum_axis0(x), array([4.0, 6.0]))
        assert_equal(sum_axis1(x), array([3.0, 7.0]))

    def test_expression(self) -> None:
        var = _get_variable(Maximize, shape=(2, 2))
        assert var.value is not None
        assert_equal(sum_axis0(var).value, sum_axis0(var.value))
        assert_equal(sum_axis1(var).value, sum_axis1(var.value))
