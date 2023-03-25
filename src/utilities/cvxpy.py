from typing import Any, Union, cast, overload

import cvxpy
import numpy as np
import numpy.linalg
from beartype import beartype
from cvxpy import Expression
from numpy import maximum, minimum, ndarray, where

from utilities.numpy import is_zero
from utilities.numpy.typing import NDArrayF, NDArrayF1, NDArrayF2


@overload
def abs_(x: float, /) -> float:
    ...


@overload
def abs_(x: NDArrayF, /) -> NDArrayF:
    ...


@overload
def abs_(x: Expression, /) -> Expression:
    ...


@beartype
def abs_(
    x: Union[float, NDArrayF, Expression], /
) -> Union[float, NDArrayF, Expression]:
    """Compute the absolute value."""
    if isinstance(x, (float, ndarray)):
        return np.abs(x)
    return cvxpy.abs(x)


@overload
def add(x: float, y: float, /) -> float:
    ...


@overload
def add(x: NDArrayF, y: float, /) -> NDArrayF:
    ...


@overload
def add(x: Expression, y: float, /) -> Expression:
    ...


@overload
def add(x: float, y: NDArrayF, /) -> NDArrayF:
    ...


@overload
def add(x: NDArrayF, y: NDArrayF, /) -> NDArrayF:
    ...


@overload
def add(x: Expression, y: NDArrayF, /) -> Expression:
    ...


@overload
def add(x: float, y: Expression, /) -> Expression:
    ...


@overload
def add(x: NDArrayF, y: Expression, /) -> Expression:
    ...


@overload
def add(x: Expression, y: Expression, /) -> Expression:
    ...


@beartype
def add(
    x: Union[float, NDArrayF, Expression], y: Union[float, NDArrayF, Expression], /
) -> Union[float, NDArrayF, Expression]:
    """Compute the sum of two quantities."""
    if isinstance(x, (float, ndarray)) and isinstance(y, (float, ndarray)):
        return np.add(x, y)
    return cast(Any, x) + cast(Any, y)


@overload
def multiply(x: float, y: float, /) -> float:
    ...


@overload
def multiply(x: NDArrayF, y: float, /) -> NDArrayF:
    ...


@overload
def multiply(x: Expression, y: float, /) -> Expression:
    ...


@overload
def multiply(x: float, y: NDArrayF, /) -> NDArrayF:
    ...


@overload
def multiply(x: NDArrayF, y: NDArrayF, /) -> NDArrayF:
    ...


@overload
def multiply(x: Expression, y: NDArrayF, /) -> Expression:
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
def norm(x: NDArrayF1, /) -> float:
    ...


@overload
def norm(x: Expression, /) -> Expression:
    ...


@beartype
def norm(x: Union[NDArrayF1, Expression], /) -> Union[float, Expression]:
    """Compute the negative parts of a quantity."""
    if isinstance(x, ndarray):
        return cast(float, numpy.linalg.norm(x))
    return cvxpy.norm(x)


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
    """Compute the positive parts of a quantity."""
    if isinstance(x, (float, ndarray)):
        result = maximum(x, 0.0)
        return where(is_zero(result), 0.0, result)
    return cvxpy.pos(x)


@overload
def power(x: float, p: float, /) -> float:
    ...


@overload
def power(x: NDArrayF, p: float, /) -> NDArrayF:
    ...


@overload
def power(x: Expression, p: float, /) -> Expression:
    ...


@overload
def power(x: float, p: NDArrayF, /) -> NDArrayF:
    ...


@overload
def power(x: NDArrayF, p: NDArrayF, /) -> NDArrayF:
    ...


@overload
def power(x: Expression, p: NDArrayF, /) -> Expression:
    ...


@beartype
def power(
    x: Union[float, NDArrayF, Expression], p: Union[float, NDArrayF], /
) -> Union[float, NDArrayF, Expression]:
    """Compute the power of a quantity."""
    if isinstance(x, (float, ndarray)):
        return np.power(x, p)
    return cvxpy.power(x, p)


@overload
def quad_form(x: NDArrayF1, P: NDArrayF2, /) -> float:  # noqa: N803
    ...


@overload
def quad_form(x: Expression, P: NDArrayF2, /) -> Expression:  # noqa: N803
    ...


@beartype
def quad_form(
    x: Union[NDArrayF1, Expression], P: NDArrayF2, /  # noqa: N803
) -> Union[float, Expression]:
    """Compute the quadratic form of a vector & matrix."""
    if isinstance(x, ndarray):
        return cast(float, x.T @ P @ x)
    return cvxpy.quad_form(x, P)


@overload
def sqrt(x: float, /) -> float:
    ...


@overload
def sqrt(x: NDArrayF, /) -> NDArrayF:
    ...


@overload
def sqrt(x: Expression, /) -> Expression:
    ...


@beartype
def sqrt(
    x: Union[float, NDArrayF, Expression], /
) -> Union[float, NDArrayF, Expression]:
    """Compute the square root of a quantity."""
    if isinstance(x, (float, ndarray)):
        return np.sqrt(x)
    return cvxpy.sqrt(x)


@overload
def subtract(x: float, y: float, /) -> float:
    ...


@overload
def subtract(x: NDArrayF, y: float, /) -> NDArrayF:
    ...


@overload
def subtract(x: Expression, y: float, /) -> Expression:
    ...


@overload
def subtract(x: float, y: NDArrayF, /) -> NDArrayF:
    ...


@overload
def subtract(x: NDArrayF, y: NDArrayF, /) -> NDArrayF:
    ...


@overload
def subtract(x: Expression, y: NDArrayF, /) -> Expression:
    ...


@overload
def subtract(x: float, y: Expression, /) -> Expression:
    ...


@overload
def subtract(x: NDArrayF, y: Expression, /) -> Expression:
    ...


@overload
def subtract(x: Expression, y: Expression, /) -> Expression:
    ...


@beartype
def subtract(
    x: Union[float, NDArrayF, Expression], y: Union[float, NDArrayF, Expression], /
) -> Union[float, NDArrayF, Expression]:
    """Compute the difference of two quantities."""
    if isinstance(x, (float, ndarray)) and isinstance(y, (float, ndarray)):
        return np.subtract(x, y)
    return cast(Any, x) - cast(Any, y)


@overload
def sum_(x: Union[float, NDArrayF], /) -> float:
    ...


@overload
def sum_(x: Expression, /) -> Expression:
    ...


@beartype
def sum_(x: Union[float, NDArrayF, Expression], /) -> Union[float, Expression]:
    """Compute the sum of a quantity."""
    if isinstance(x, float):
        return x
    if isinstance(x, ndarray):
        return float(np.sum(x))
    return cvxpy.sum(x)
