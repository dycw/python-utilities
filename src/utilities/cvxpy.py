from typing import Union, overload

import cvxpy
import numpy as np
from beartype import beartype
from cvxpy import Expression
from numpy import maximum, minimum, ndarray, where

from utilities.numpy import is_zero
from utilities.numpy.typing import NDArrayF


@overload
def abs(x: float, /) -> float:  # noqa: A001
    ...


@overload
def abs(x: NDArrayF, /) -> NDArrayF:  # noqa: A001
    ...


@overload
def abs(x: Expression, /) -> Expression:  # noqa: A001
    ...


@beartype
def abs(  # noqa: A001
    x: Union[float, NDArrayF, Expression], /
) -> Union[float, NDArrayF, Expression]:
    """Compute the absolute value."""
    if isinstance(x, (float, ndarray)):
        return np.abs(x)
    return cvxpy.abs(x)


@overload
def multiply(x: float, y: float, /) -> float:
    ...


@overload
def multiply(x: float, y: NDArrayF, /) -> NDArrayF:
    ...


@overload
def multiply(x: NDArrayF, y: float, /) -> NDArrayF:
    ...


@overload
def multiply(x: NDArrayF, y: NDArrayF, /) -> NDArrayF:
    ...


@overload
def multiply(x: float, y: Expression, /) -> Expression:
    ...


@overload
def multiply(x: NDArrayF, y: Expression, /) -> Expression:
    ...


@overload
def multiply(x: Expression, y: Expression, /) -> Expression:
    ...


@overload
def multiply(x: Expression, y: float, /) -> Expression:
    ...


@overload
def multiply(x: Expression, y: NDArrayF, /) -> Expression:
    ...


@beartype
def multiply(
    x: Union[float, NDArrayF, Expression], y: Union[float, NDArrayF, Expression], /
) -> Union[float, NDArrayF, Expression]:
    """Compute the product of two quantities."""
    if isinstance(x, (float, ndarray)) and isinstance(y, (float, ndarray)):
        return np.multiply(x, y)
    return cvxpy.multiply(x, y)


@overload
def neg(x: float, /) -> float:
    ...


@overload
def neg(x: NDArrayF, /) -> NDArrayF:
    ...


@overload
def neg(x: Expression, /) -> Expression:
    ...


@beartype
def neg(x: Union[float, NDArrayF, Expression], /) -> Union[float, NDArrayF, Expression]:
    """Compute the negative parts of a quantity."""
    if isinstance(x, (float, ndarray)):
        result = -minimum(x, 0.0)
        return where(is_zero(result), 0.0, result)
    return cvxpy.neg(x)


@overload
def pos(x: float, /) -> float:
    ...


@overload
def pos(x: NDArrayF, /) -> NDArrayF:
    ...


@overload
def pos(x: Expression, /) -> Expression:
    ...


@beartype
def pos(x: Union[float, NDArrayF, Expression], /) -> Union[float, NDArrayF, Expression]:
    """Compute the posative parts of a quantity."""
    if isinstance(x, (float, ndarray)):
        result = maximum(x, 0.0)
        return where(is_zero(result), 0.0, result)
    return cvxpy.pos(x)
