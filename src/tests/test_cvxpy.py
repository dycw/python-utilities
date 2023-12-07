from __future__ import annotations

from functools import cache
from typing import Any, cast

import cvxpy
import numpy as np
from cvxpy import Expression, Maximize, Minimize, Problem, Variable
from numpy import array
from numpy.testing import assert_equal
from pytest import mark, param, raises

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
    solve,
    sqrt,
    subtract,
    sum_,
)
from utilities.numpy import NDArrayF


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
        ("x", "expected"),
        [
            param(0.0, 0.0),
            param(1.0, 1.0),
            param(-1.0, 1.0),
            param(array([0.0]), array([0.0])),
            param(array([1.0]), array([1.0])),
            param(array([-1.0]), array([1.0])),
        ],
    )
    def test_float_and_array(
        self, *, x: float | NDArrayF, expected: float | NDArrayF
    ) -> None:
        assert_equal(abs_(x), expected)

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
        assert_equal(add(x, var).value, add(x, var.value))
        assert_equal(add(var, x).value, add(var.value, x))

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
            param(1.0, 1.0),
            param(-1.0, -1.0),
            param(array([0.0]), 0.0),
            param(array([1.0]), 1.0),
            param(array([-1.0]), -1.0),
        ],
    )
    def test_float_and_array(self, *, x: float | NDArrayF, expected: float) -> None:
        assert_equal(max_(x), expected)

    @mark.parametrize("objective", [param(Maximize), param(Minimize)])
    def test_expression(self, *, objective: type[Maximize | Minimize]) -> None:
        var = _get_variable(objective)
        assert_equal(max_(var).value, max_(var.value))


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
            param(1.0, 1.0),
            param(-1.0, -1.0),
            param(array([0.0]), 0.0),
            param(array([1.0]), 1.0),
            param(array([-1.0]), -1.0),
        ],
    )
    def test_float_and_array(self, *, x: float | NDArrayF, expected: float) -> None:
        assert_equal(min_(x), expected)

    @mark.parametrize("objective", [param(Maximize), param(Minimize)])
    def test_expression(self, *, objective: type[Maximize | Minimize]) -> None:
        var = _get_variable(objective)
        assert_equal(min_(var).value, min_(var.value))


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
    @mark.parametrize(
        ("x", "y", "expected"),
        [
            param(2.0, 3.0, 6.0),
            param(2.0, array([3.0]), array([6.0])),
            param(array([2.0]), 3.0, array([6.0])),
            param(array([2.0]), array([3.0]), array([6.0])),
        ],
    )
    def test_float_and_array(
        self, *, x: float | NDArrayF, y: float | NDArrayF, expected: float | NDArrayF
    ) -> None:
        assert_equal(multiply(x, y), expected)

    @mark.parametrize("x", [param(2.0), param(array([2.0]))])
    @mark.parametrize("objective", [param(Maximize), param(Minimize)])
    def test_one_expression(
        self, *, x: float | NDArrayF | Expression, objective: type[Maximize | Minimize]
    ) -> None:
        var = _get_variable(objective)
        assert_equal(multiply(x, var).value, multiply(x, var.value))
        assert_equal(multiply(var, x).value, multiply(var.value, x))

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
        assert_equal(multiply(var1, var2).value, multiply(var1.value, var2.value))


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
        assert_equal(negative(var).value, negative(var.value))


class TestNorm:
    def test_array(self) -> None:
        assert_equal(norm(array([2.0, 3.0])), np.sqrt(13))

    @mark.parametrize("objective", [param(Maximize), param(Minimize)])
    def test_expression(self, *, objective: type[Maximize] | type[Minimize]) -> None:  # noqa: PYI055
        var = _get_variable(objective, array=True)
        assert_equal(norm(var).value, norm(var.value))


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

    def test_expression(self) -> None:
        var = _get_variable(Maximize)
        assert_equal(sqrt(var).value, sqrt(var.value))


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
        ],
    )
    def test_float_and_array(self, *, x: float | NDArrayF, expected: float) -> None:
        assert_equal(sum_(x), expected)

    def test_expression(self) -> None:
        var = _get_variable(Maximize)
        assert_equal(sum_(var).value, sum_(var.value))
