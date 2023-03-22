from typing import Union

from cvxpy import Expression, Variable
from numpy import array
from numpy.testing import assert_equal
from pytest import mark, param

import utilities.cvxpy
from utilities.cvxpy import multiply, neg, pos
from utilities.numpy.typing import NDArrayF


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
    def test_main(
        self, x: Union[float, NDArrayF], expected: Union[float, NDArrayF]
    ) -> None:
        assert_equal(utilities.cvxpy.abs(x), expected)

    def test_expression(self) -> None:
        _ = utilities.cvxpy.abs(Variable())


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
    def test_main(
        self,
        x: Union[float, NDArrayF],
        y: Union[float, NDArrayF],
        expected: Union[float, NDArrayF],
    ) -> None:
        assert_equal(multiply(x, y), expected)

    @mark.parametrize(
        ("x", "y"),
        [
            param(0.0, Variable()),
            param(array([0.0]), Variable()),
            param(Variable(), Variable()),
            param(Variable(), 0.0),
            param(Variable(), array([0.0])),
        ],
    )
    def test_expression(
        self,
        x: Union[float, NDArrayF, Expression],
        y: Union[float, NDArrayF, Expression],
    ) -> None:
        _ = multiply(x, y)


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
    def test_main(
        self, x: Union[float, NDArrayF], expected: Union[float, NDArrayF]
    ) -> None:
        assert_equal(neg(x), expected)

    def test_expression(self) -> None:
        _ = neg(Variable())


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
    def test_main(
        self, x: Union[float, NDArrayF], expected: Union[float, NDArrayF]
    ) -> None:
        assert_equal(pos(x), expected)

    def test_expression(self) -> None:
        _ = pos(Variable())
