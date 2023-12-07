from __future__ import annotations

from typing import Any, Literal, cast, overload

import cvxpy
import numpy as np
import numpy.linalg
from cvxpy import CLARABEL, Expression, Problem
from numpy import ndarray, where

from utilities.numpy import NDArrayF, NDArrayF1, NDArrayF2, is_zero


@overload
def abs_(x: float, /) -> float:
    ...


@overload
def abs_(x: NDArrayF, /) -> NDArrayF:
    ...


@overload
def abs_(x: Expression, /) -> Expression:
    ...


def abs_(x: float | NDArrayF | Expression, /) -> float | NDArrayF | Expression:
    """Compute the absolute value."""
    if isinstance(x, float | ndarray):
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


def add(
    x: float | NDArrayF | Expression, y: float | NDArrayF | Expression, /
) -> float | NDArrayF | Expression:
    """Compute the sum of two quantities."""
    if isinstance(x, float | ndarray) and isinstance(y, float | ndarray):
        return np.add(x, y)
    return cast(Any, x) + cast(Any, y)


@overload
def divide(x: float, y: float, /) -> float:
    ...


@overload
def divide(x: NDArrayF, y: float, /) -> NDArrayF:
    ...


@overload
def divide(x: Expression, y: float, /) -> Expression:
    ...


@overload
def divide(x: float, y: NDArrayF, /) -> NDArrayF:
    ...


@overload
def divide(x: NDArrayF, y: NDArrayF, /) -> NDArrayF:
    ...


@overload
def divide(x: Expression, y: NDArrayF, /) -> Expression:
    ...


@overload
def divide(x: float, y: Expression, /) -> Expression:
    ...


@overload
def divide(x: NDArrayF, y: Expression, /) -> Expression:
    ...


@overload
def divide(x: Expression, y: Expression, /) -> Expression:
    ...


def divide(
    x: float | NDArrayF | Expression, y: float | NDArrayF | Expression, /
) -> float | NDArrayF | Expression:
    """Compute the quotient of two quantities."""
    if isinstance(x, float | ndarray) and isinstance(y, float | ndarray):
        return np.divide(x, y)
    return cast(Any, x) / cast(Any, y)


@overload
def max_(x: float | NDArrayF, /) -> float:
    ...


@overload
def max_(x: Expression, /) -> Expression:
    ...


def max_(x: float | NDArrayF | Expression, /) -> float | Expression:
    """Compute the maximum of a quantity."""
    if isinstance(x, float | ndarray):
        return np.max(x)
    return cvxpy.max(x)


@overload
def maximum(x: float, y: float, /) -> float:
    ...


@overload
def maximum(x: NDArrayF, y: float, /) -> NDArrayF:
    ...


@overload
def maximum(x: Expression, y: float, /) -> Expression:
    ...


@overload
def maximum(x: float, y: NDArrayF, /) -> NDArrayF:
    ...


@overload
def maximum(x: NDArrayF, y: NDArrayF, /) -> NDArrayF:
    ...


@overload
def maximum(x: Expression, y: NDArrayF, /) -> Expression:
    ...


@overload
def maximum(x: float, y: Expression, /) -> Expression:
    ...


@overload
def maximum(x: NDArrayF, y: Expression, /) -> Expression:
    ...


@overload
def maximum(x: Expression, y: Expression, /) -> Expression:
    ...


def maximum(
    x: float | NDArrayF | Expression, y: float | NDArrayF | Expression, /
) -> float | NDArrayF | Expression:
    """Compute the maximum of two quantities."""
    if isinstance(x, float | ndarray) and isinstance(y, float | ndarray):
        return np.maximum(x, y)
    return cvxpy.maximum(x, y)


@overload
def min_(x: float | NDArrayF, /) -> float:
    ...


@overload
def min_(x: Expression, /) -> Expression:
    ...


def min_(x: float | NDArrayF | Expression, /) -> float | Expression:
    """Compute the minimum of a quantity."""
    if isinstance(x, float | ndarray):
        return np.min(x)
    return cvxpy.min(x)


@overload
def minimum(x: float, y: float, /) -> float:
    ...


@overload
def minimum(x: NDArrayF, y: float, /) -> NDArrayF:
    ...


@overload
def minimum(x: Expression, y: float, /) -> Expression:
    ...


@overload
def minimum(x: float, y: NDArrayF, /) -> NDArrayF:
    ...


@overload
def minimum(x: NDArrayF, y: NDArrayF, /) -> NDArrayF:
    ...


@overload
def minimum(x: Expression, y: NDArrayF, /) -> Expression:
    ...


@overload
def minimum(x: float, y: Expression, /) -> Expression:
    ...


@overload
def minimum(x: NDArrayF, y: Expression, /) -> Expression:
    ...


@overload
def minimum(x: Expression, y: Expression, /) -> Expression:
    ...


def minimum(
    x: float | NDArrayF | Expression, y: float | NDArrayF | Expression, /
) -> float | NDArrayF | Expression:
    """Compute the minimum of two quantities."""
    if isinstance(x, float | ndarray) and isinstance(y, float | ndarray):
        return np.minimum(x, y)
    return cvxpy.minimum(x, y)


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


def multiply(
    x: float | NDArrayF | Expression, y: float | NDArrayF | Expression, /
) -> float | NDArrayF | Expression:
    """Compute the product of two quantities."""
    if isinstance(x, float | ndarray) and isinstance(y, float | ndarray):
        return np.multiply(x, y)
    return cvxpy.multiply(x, y)


@overload
def negate(x: float, /) -> float:
    ...


@overload
def negate(x: NDArrayF, /) -> NDArrayF:
    ...


@overload
def negate(x: Expression, /) -> Expression:
    ...


def negate(x: float | NDArrayF | Expression, /) -> float | NDArrayF | Expression:
    """Negate a quantity."""
    return -x


@overload
def negative(x: float, /) -> float:
    ...


@overload
def negative(x: NDArrayF, /) -> NDArrayF:
    ...


@overload
def negative(x: Expression, /) -> Expression:
    ...


def negative(x: float | NDArrayF | Expression, /) -> float | NDArrayF | Expression:
    """Compute the negative parts of a quantity."""
    if isinstance(x, float | ndarray):
        result = -minimum(x, 0.0)
        return where(is_zero(result), 0.0, result)
    return cvxpy.neg(x)


@overload
def norm(x: NDArrayF1, /) -> float:
    ...


@overload
def norm(x: Expression, /) -> Expression:
    ...


def norm(x: NDArrayF1 | Expression, /) -> float | Expression:
    """Compute the negative parts of a quantity."""
    if isinstance(x, ndarray):
        return cast(float, numpy.linalg.norm(x))
    return cvxpy.norm(x)


@overload
def positive(x: float, /) -> float:
    ...


@overload
def positive(x: NDArrayF, /) -> NDArrayF:
    ...


@overload
def positive(x: Expression, /) -> Expression:
    ...


def positive(x: float | NDArrayF | Expression, /) -> float | NDArrayF | Expression:
    """Compute the positive parts of a quantity."""
    if isinstance(x, float | ndarray):
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


def power(
    x: float | NDArrayF | Expression, p: float | NDArrayF, /
) -> float | NDArrayF | Expression:
    """Compute the power of a quantity."""
    if isinstance(x, float | ndarray):
        return np.power(x, p)
    return cvxpy.power(x, p)


@overload
def quad_form(x: NDArrayF1, P: NDArrayF2, /) -> float:  # noqa: N803
    ...


@overload
def quad_form(x: Expression, P: NDArrayF2, /) -> Expression:  # noqa: N803
    ...


def quad_form(
    x: NDArrayF1 | Expression,
    P: NDArrayF2,  # noqa: N803
    /,
) -> float | Expression:
    """Compute the quadratic form of a vector & matrix."""
    if isinstance(x, ndarray):
        return cast(float, x.T @ P @ x)
    return cvxpy.quad_form(x, P)


def solve(
    problem: Problem,
    /,
    *,
    solver: Literal[
        "CBC",
        "CLARABEL",
        "COPT",
        "CVXOPT",
        "ECOS",
        "GLOP",
        "GLPK_MI",
        "GLPK",
        "GUROBI",
        "MOSEK",
        "NAG",
        "OSQP",
        "PDLPCPLEX",
        "PIQP",
        "PROXQP",
        "SCIP",
        "SCIPY",
        "SCS",
        "SDPA",
        "XPRESS",
    ] = CLARABEL,
    verbose: bool = False,
    **kwargs: Any,
) -> float:
    """Solve a problem."""
    match solver:
        case "MOSEK":  # pragma: no cover
            specific = {"mosek_params": {"MSK_IPAR_LICENSE_WAIT": True}}
        case _:
            specific = {}
    obj = cast(
        float, problem.solve(solver=solver, verbose=verbose, **kwargs, **specific)
    )
    if (status := problem.status) in {"optimal", "optimal_inaccurate"}:
        return obj
    if status in {"infeasible", "infeasible_inaccurate"}:
        msg = f"{problem=}"
        raise SolveInfeasibleError(msg)
    if status == "unbounded":
        msg = f"{problem=}"
        raise SolveUnboundedError(msg)
    msg = f"{status=}"  # pragma: no cover
    raise SolveError(msg)  # pragma: no cover


class SolveError(Exception):
    ...


class SolveInfeasibleError(SolveError):
    ...


class SolveUnboundedError(SolveError):
    ...


@overload
def sqrt(x: float, /) -> float:
    ...


@overload
def sqrt(x: NDArrayF, /) -> NDArrayF:
    ...


@overload
def sqrt(x: Expression, /) -> Expression:
    ...


def sqrt(x: float | NDArrayF | Expression, /) -> float | NDArrayF | Expression:
    """Compute the square root of a quantity."""
    if isinstance(x, float | ndarray):
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


def subtract(
    x: float | NDArrayF | Expression, y: float | NDArrayF | Expression, /
) -> float | NDArrayF | Expression:
    """Compute the difference of two quantities."""
    if isinstance(x, float | ndarray) and isinstance(y, float | ndarray):
        return np.subtract(x, y)
    return cast(Any, x) - cast(Any, y)


@overload
def sum_(x: float | NDArrayF, /) -> float:
    ...


@overload
def sum_(x: Expression, /) -> Expression:
    ...


def sum_(x: float | NDArrayF | Expression, /) -> float | Expression:
    """Compute the sum of a quantity."""
    if isinstance(x, float):
        return x
    if isinstance(x, ndarray):
        return float(np.sum(x))
    return cvxpy.sum(x)


__all__ = [
    "SolveError",
    "SolveInfeasibleError",
    "SolveUnboundedError",
    "abs_",
    "add",
    "divide",
    "max_",
    "maximum",
    "min_",
    "multiply",
    "negate",
    "negative",
    "norm",
    "positive",
    "power",
    "quad_form",
    "solve",
    "sqrt",
    "subtract",
    "sum_",
]
