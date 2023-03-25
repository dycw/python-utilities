from functools import cache
from typing import Any, Union, cast

import cvxpy
import numpy as np
from cvxpy import Expression, Maximize, Minimize, Problem, Variable
from numpy import array
from numpy.testing import assert_equal
from pytest import mark, param

from utilities.cvxpy import abs_, multiply, neg, norm, pos, power, quad_form, sqrt, sum_
from utilities.numpy.typing import NDArrayF


@cache
def _get_variable(
    objective: Union[type[Maximize], type[Minimize]], /, *, array: bool = False
) -> Variable:
    if array:
        var = Variable(2)
        scalar = cvxpy.sum(var)
    else:
        var = Variable()
        scalar = var
    problem = Problem(
        objective(scalar), [cast(Any, var) >= -10.0, cast(Any, var) <= 10.0]
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
        self, x: Union[float, NDArrayF], expected: Union[float, NDArrayF]
    ) -> None:
        assert_equal(abs_(x), expected)

    @mark.parametrize("objective", [param(Maximize), param(Minimize)])
    def test_expression(self, objective: Union[type[Maximize], type[Minimize]]) -> None:
        var = _get_variable(objective)
        assert_equal(abs_(var).value, abs_(var.value))


class TestMultiply:
    @mark.parametrize(
        ("x", "y", "expected"),
        [
            param(0.0, 0.0, 0.0),
            param(2.0, 3.0, 6.0),
            param(2.0, array([3.0]), array([6.0])),
            param(array([2.0]), 3.0, array([6.0])),
            param(array([2.0]), array([3.0]), array([6.0])),
        ],
    )
    def test_float_and_array(
        self,
        x: Union[float, NDArrayF],
        y: Union[float, NDArrayF],
        expected: Union[float, NDArrayF],
    ) -> None:
        assert_equal(multiply(x, y), expected)

    @mark.parametrize("x", [param(2.0), param(array([2.0]))])
    @mark.parametrize("objective", [param(Maximize), param(Minimize)])
    def test_one_expression(
        self,
        x: Union[float, NDArrayF, Expression],
        objective: Union[type[Maximize], type[Minimize]],
    ) -> None:
        var = _get_variable(objective)
        assert_equal(multiply(x, var).value, multiply(x, var.value))
        assert_equal(multiply(var, x).value, multiply(var.value, x))

    @mark.parametrize("objective1", [param(Maximize), param(Minimize)])
    @mark.parametrize("objective2", [param(Maximize), param(Minimize)])
    def test_two_expressions(
        self,
        objective1: Union[type[Maximize], type[Minimize]],
        objective2: Union[type[Maximize], type[Minimize]],
    ) -> None:
        var1 = _get_variable(objective1)
        var2 = _get_variable(objective2)
        assert_equal(multiply(var1, var2).value, multiply(var1.value, var2.value))


class TestNeg:
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
        self, x: Union[float, NDArrayF], expected: Union[float, NDArrayF]
    ) -> None:
        assert_equal(neg(x), expected)

    @mark.parametrize("objective", [param(Maximize), param(Minimize)])
    def test_expression(self, objective: Union[type[Maximize], type[Minimize]]) -> None:
        var = _get_variable(objective)
        assert_equal(neg(var).value, neg(var.value))


class TestNorm:
    def test_array(self) -> None:
        assert_equal(norm(array([2.0, 3.0])), np.sqrt(13))

    @mark.parametrize("objective", [param(Maximize), param(Minimize)])
    def test_expression(self, objective: Union[type[Maximize], type[Minimize]]) -> None:
        var = _get_variable(objective, array=True)
        assert_equal(norm(var).value, norm(var.value))


class TestPos:
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
        self, x: Union[float, NDArrayF], expected: Union[float, NDArrayF]
    ) -> None:
        assert_equal(pos(x), expected)

    @mark.parametrize("objective", [param(Maximize), param(Minimize)])
    def test_expression(self, objective: Union[type[Maximize], type[Minimize]]) -> None:
        var = _get_variable(objective)
        assert_equal(pos(var).value, pos(var.value))


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
        self,
        x: Union[float, NDArrayF],
        p: Union[float, NDArrayF],
        expected: Union[float, NDArrayF],
    ) -> None:
        assert_equal(power(x, p), expected)

    @mark.parametrize("objective", [param(Maximize), param(Minimize)])
    def test_one_expression(
        self, objective: Union[type[Maximize], type[Minimize]]
    ) -> None:
        var = _get_variable(objective)
        assert_equal(power(var, 2.0).value, power(var.value, 2.0))


class TestQuadForm:
    def test_array(self) -> None:
        assert_equal(
            quad_form(array([2.0, 3.0]), array([[4.0, 5.0], [5.0, 4.0]])), 112.0
        )

    @mark.parametrize("objective", [param(Maximize), param(Minimize)])
    def test_expression(self, objective: Union[type[Maximize], type[Minimize]]) -> None:
        var = _get_variable(objective, array=True)
        P = array([[2.0, 3.0], [3.0, 2.0]])  # noqa: N806
        assert_equal(quad_form(var, P).value, quad_form(var.value, P))


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
        self, x: Union[float, NDArrayF], expected: Union[float, NDArrayF]
    ) -> None:
        assert_equal(sqrt(x), expected)

    def test_expression(self) -> None:
        var = _get_variable(Maximize)
        assert_equal(sqrt(var).value, sqrt(var.value))


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
    def test_float_and_array(self, x: Union[float, NDArrayF], expected: float) -> None:
        assert_equal(sum_(x), expected)

    def test_expression(self) -> None:
        var = _get_variable(Maximize)
        assert_equal(sum_(var).value, sum_(var.value))
