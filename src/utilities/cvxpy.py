from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, cast, overload

import cvxpy
import numpy as np
import numpy.linalg
from cvxpy import CLARABEL, Expression, Problem
from numpy import ndarray, where
from typing_extensions import override

from utilities.numpy import NDArrayF, NDArrayF1, NDArrayF2, is_non_zero, is_zero

if TYPE_CHECKING:
    from pandas import DataFrame

    from utilities.pandas import SeriesF


@overload
def abs_(x: float, /) -> float: ...


@overload
def abs_(x: NDArrayF, /) -> NDArrayF: ...


@overload
def abs_(x: SeriesF, /) -> SeriesF: ...


@overload
def abs_(x: DataFrame, /) -> DataFrame: ...


@overload
def abs_(x: Expression, /) -> Expression: ...


def abs_(
    x: float | NDArrayF | SeriesF | DataFrame | Expression, /
) -> float | NDArrayF | SeriesF | DataFrame | Expression:
    """Compute the absolute value."""
    if isinstance(x, int | float | ndarray):
        return np.abs(x)
    try:
        from pandas import DataFrame, Series
    except ModuleNotFoundError:  # pragma: no cover
        pass
    else:
        if isinstance(x, Series | DataFrame):
            return np.abs(x)
    return cvxpy.abs(x)


@overload
def add(x: float, y: float, /) -> float: ...


@overload
def add(x: NDArrayF, y: float, /) -> NDArrayF: ...


@overload
def add(x: Expression, y: float, /) -> Expression: ...


@overload
def add(x: float, y: NDArrayF, /) -> NDArrayF: ...


@overload
def add(x: NDArrayF, y: NDArrayF, /) -> NDArrayF: ...


@overload
def add(x: Expression, y: NDArrayF, /) -> Expression: ...


@overload
def add(x: float, y: Expression, /) -> Expression: ...


@overload
def add(x: NDArrayF, y: Expression, /) -> Expression: ...


@overload
def add(x: Expression, y: Expression, /) -> Expression: ...


def add(
    x: float | NDArrayF | Expression, y: float | NDArrayF | Expression, /
) -> float | NDArrayF | Expression:
    """Compute the sum of two quantities."""
    if isinstance(x, int | float | ndarray) and isinstance(y, int | float | ndarray):
        return np.add(x, y)
    return cast(Any, x) + cast(Any, y)


@overload
def divide(x: float, y: float, /) -> float: ...


@overload
def divide(x: NDArrayF, y: float, /) -> NDArrayF: ...


@overload
def divide(x: Expression, y: float, /) -> Expression: ...


@overload
def divide(x: float, y: NDArrayF, /) -> NDArrayF: ...


@overload
def divide(x: NDArrayF, y: NDArrayF, /) -> NDArrayF: ...


@overload
def divide(x: Expression, y: NDArrayF, /) -> Expression: ...


@overload
def divide(x: float, y: Expression, /) -> Expression: ...


@overload
def divide(x: NDArrayF, y: Expression, /) -> Expression: ...


@overload
def divide(x: Expression, y: Expression, /) -> Expression: ...


def divide(
    x: float | NDArrayF | Expression, y: float | NDArrayF | Expression, /
) -> float | NDArrayF | Expression:
    """Compute the quotient of two quantities."""
    if isinstance(x, int | float | ndarray) and isinstance(y, int | float | ndarray):
        return np.divide(x, y)
    return cast(Any, x) / cast(Any, y)


@overload
def max_(x: float | NDArrayF | SeriesF | DataFrame, /) -> float: ...


@overload
def max_(x: Expression, /) -> Expression: ...


def max_(
    x: float | NDArrayF | SeriesF | DataFrame | Expression, /
) -> float | Expression:
    """Compute the maximum of a quantity."""
    if isinstance(x, int | float | ndarray):
        return np.max(x)
    try:
        from pandas import DataFrame, Series
    except ModuleNotFoundError:  # pragma: no cover
        pass
    else:
        if isinstance(x, Series | DataFrame):
            return max_(x.to_numpy())
    return cvxpy.max(x)


@overload
def maximum(x: float, y: float, /) -> float: ...


@overload
def maximum(x: NDArrayF, y: float, /) -> NDArrayF: ...


@overload
def maximum(x: SeriesF, y: float, /) -> SeriesF: ...


@overload
def maximum(x: DataFrame, y: float, /) -> DataFrame: ...


@overload
def maximum(x: Expression, y: float, /) -> Expression: ...


@overload
def maximum(x: float, y: NDArrayF, /) -> NDArrayF: ...


@overload
def maximum(x: NDArrayF, y: NDArrayF, /) -> NDArrayF: ...


@overload
def maximum(x: SeriesF, y: NDArrayF, /) -> SeriesF: ...


@overload
def maximum(x: DataFrame, y: NDArrayF, /) -> DataFrame: ...


@overload
def maximum(x: Expression, y: NDArrayF, /) -> Expression: ...


@overload
def maximum(x: float, y: SeriesF, /) -> SeriesF: ...


@overload
def maximum(x: NDArrayF, y: SeriesF, /) -> SeriesF: ...


@overload
def maximum(x: SeriesF, y: SeriesF, /) -> SeriesF: ...


@overload
def maximum(x: Expression, y: SeriesF, /) -> Expression: ...


@overload
def maximum(x: float, y: DataFrame, /) -> DataFrame: ...


@overload
def maximum(x: NDArrayF, y: DataFrame, /) -> DataFrame: ...


@overload
def maximum(x: DataFrame, y: DataFrame, /) -> DataFrame: ...


@overload
def maximum(x: Expression, y: DataFrame, /) -> Expression: ...


@overload
def maximum(x: float, y: Expression, /) -> Expression: ...


@overload
def maximum(x: NDArrayF, y: Expression, /) -> Expression: ...


@overload
def maximum(x: SeriesF, y: Expression, /) -> Expression: ...


@overload
def maximum(x: DataFrame, y: Expression, /) -> Expression: ...


@overload
def maximum(x: Expression, y: Expression, /) -> Expression: ...


def maximum(
    x: float | NDArrayF | SeriesF | DataFrame | Expression,
    y: float | NDArrayF | SeriesF | DataFrame | Expression,
    /,
) -> float | NDArrayF | SeriesF | DataFrame | Expression:
    """Compute the elementwise maximum of two quantities."""
    try:
        _check_series_and_dataframe(x, y)
    except _CheckSeriesAndDataFrameError as error:
        raise MaximumError(x=error.x, y=error.y) from None
    try:
        from pandas import DataFrame, Series
    except ModuleNotFoundError:  # pragma: no cover
        if isinstance(x, int | float | ndarray) and isinstance(
            y, int | float | ndarray
        ):
            return np.maximum(x, y)
    else:
        if isinstance(x, int | float | ndarray | Series | DataFrame) and isinstance(
            y, int | float | ndarray | Series | DataFrame
        ):
            return np.maximum(x, y)
    return cvxpy.maximum(x, y)


@dataclass(kw_only=True)
class MaximumError(Exception):
    x: SeriesF | DataFrame
    y: SeriesF | DataFrame

    @override
    def __str__(self) -> str:
        return f"Maximum must not be between a Series and DataFrame; got {self.x} and {self.y}."


@overload
def min_(x: float | NDArrayF | SeriesF | DataFrame, /) -> float: ...


@overload
def min_(x: Expression, /) -> Expression: ...


def min_(
    x: float | NDArrayF | SeriesF | DataFrame | Expression, /
) -> float | Expression:
    """Compute the minimum of a quantity."""
    if isinstance(x, int | float | ndarray):
        return np.min(x)
    try:
        from pandas import DataFrame, Series
    except ModuleNotFoundError:  # pragma: no cover
        pass
    else:
        if isinstance(x, Series | DataFrame):
            return min_(x.to_numpy())
    return cvxpy.min(x)


@overload
def minimum(x: float, y: float, /) -> float: ...


@overload
def minimum(x: NDArrayF, y: float, /) -> NDArrayF: ...


@overload
def minimum(x: SeriesF, y: float, /) -> SeriesF: ...


@overload
def minimum(x: DataFrame, y: float, /) -> DataFrame: ...


@overload
def minimum(x: Expression, y: float, /) -> Expression: ...


@overload
def minimum(x: float, y: NDArrayF, /) -> NDArrayF: ...


@overload
def minimum(x: NDArrayF, y: NDArrayF, /) -> NDArrayF: ...


@overload
def minimum(x: SeriesF, y: NDArrayF, /) -> SeriesF: ...


@overload
def minimum(x: DataFrame, y: NDArrayF, /) -> DataFrame: ...


@overload
def minimum(x: Expression, y: NDArrayF, /) -> Expression: ...


@overload
def minimum(x: float, y: SeriesF, /) -> SeriesF: ...


@overload
def minimum(x: NDArrayF, y: SeriesF, /) -> SeriesF: ...


@overload
def minimum(x: SeriesF, y: SeriesF, /) -> SeriesF: ...


@overload
def minimum(x: Expression, y: SeriesF, /) -> Expression: ...


@overload
def minimum(x: float, y: DataFrame, /) -> DataFrame: ...


@overload
def minimum(x: NDArrayF, y: DataFrame, /) -> DataFrame: ...


@overload
def minimum(x: DataFrame, y: DataFrame, /) -> DataFrame: ...


@overload
def minimum(x: Expression, y: DataFrame, /) -> Expression: ...


@overload
def minimum(x: float, y: Expression, /) -> Expression: ...


@overload
def minimum(x: NDArrayF, y: Expression, /) -> Expression: ...


@overload
def minimum(x: SeriesF, y: Expression, /) -> Expression: ...


@overload
def minimum(x: DataFrame, y: Expression, /) -> Expression: ...


@overload
def minimum(x: Expression, y: Expression, /) -> Expression: ...


def minimum(
    x: float | NDArrayF | SeriesF | DataFrame | Expression,
    y: float | NDArrayF | SeriesF | DataFrame | Expression,
    /,
) -> float | NDArrayF | SeriesF | DataFrame | Expression:
    """Compute the elementwise minimum of two quantities."""
    try:
        _check_series_and_dataframe(x, y)
    except _CheckSeriesAndDataFrameError as error:
        raise MinimumError(x=error.x, y=error.y) from None
    try:
        from pandas import DataFrame, Series
    except ModuleNotFoundError:  # pragma: no cover
        if isinstance(x, int | float | ndarray) and isinstance(
            y, int | float | ndarray
        ):
            return np.minimum(x, y)
    else:
        if isinstance(x, int | float | ndarray | Series | DataFrame) and isinstance(
            y, int | float | ndarray | Series | DataFrame
        ):
            return np.minimum(x, y)
    return cvxpy.minimum(x, y)


@dataclass(kw_only=True)
class MinimumError(Exception):
    x: SeriesF | DataFrame
    y: SeriesF | DataFrame

    @override
    def __str__(self) -> str:
        return f"Minimum must not be between a Series and DataFrame; got {self.x} and {self.y}."


@overload
def multiply(x: float, y: float, /) -> float: ...


@overload
def multiply(x: NDArrayF, y: float, /) -> NDArrayF: ...


@overload
def multiply(x: SeriesF, y: float, /) -> SeriesF: ...


@overload
def multiply(x: DataFrame, y: float, /) -> DataFrame: ...


@overload
def multiply(x: Expression, y: float, /) -> Expression: ...


@overload
def multiply(x: float, y: NDArrayF, /) -> NDArrayF: ...


@overload
def multiply(x: NDArrayF, y: NDArrayF, /) -> NDArrayF: ...


@overload
def multiply(x: SeriesF, y: NDArrayF, /) -> SeriesF: ...


@overload
def multiply(x: DataFrame, y: NDArrayF, /) -> DataFrame: ...


@overload
def multiply(x: Expression, y: NDArrayF, /) -> Expression: ...


@overload
def multiply(x: float, y: SeriesF, /) -> SeriesF: ...


@overload
def multiply(x: NDArrayF, y: SeriesF, /) -> SeriesF: ...


@overload
def multiply(x: SeriesF, y: SeriesF, /) -> SeriesF: ...


@overload
def multiply(x: Expression, y: SeriesF, /) -> Expression: ...


@overload
def multiply(x: float, y: DataFrame, /) -> DataFrame: ...


@overload
def multiply(x: NDArrayF, y: DataFrame, /) -> DataFrame: ...


@overload
def multiply(x: DataFrame, y: DataFrame, /) -> DataFrame: ...


@overload
def multiply(x: Expression, y: DataFrame, /) -> Expression: ...


@overload
def multiply(x: float, y: Expression, /) -> Expression: ...


@overload
def multiply(x: NDArrayF, y: Expression, /) -> Expression: ...


@overload
def multiply(x: SeriesF, y: Expression, /) -> Expression: ...


@overload
def multiply(x: DataFrame, y: Expression, /) -> Expression: ...


@overload
def multiply(x: Expression, y: Expression, /) -> Expression: ...


def multiply(
    x: float | NDArrayF | SeriesF | DataFrame | Expression,
    y: float | NDArrayF | SeriesF | DataFrame | Expression,
    /,
) -> float | NDArrayF | SeriesF | DataFrame | Expression:
    """Compute the elementwise product of two quantities."""
    try:
        _check_series_and_dataframe(x, y)
    except _CheckSeriesAndDataFrameError as error:
        raise MultiplyError(x=error.x, y=error.y) from None
    try:
        from pandas import DataFrame, Series
    except ModuleNotFoundError:  # pragma: no cover
        if isinstance(x, int | float | ndarray) and isinstance(
            y, int | float | ndarray
        ):
            return np.multiply(x, y)
    else:
        if isinstance(x, int | float | ndarray | Series | DataFrame) and isinstance(
            y, int | float | ndarray | Series | DataFrame
        ):
            return np.multiply(x, y)
    return cvxpy.multiply(x, y)


@dataclass(kw_only=True)
class MultiplyError(Exception):
    x: SeriesF | DataFrame
    y: SeriesF | DataFrame

    @override
    def __str__(self) -> str:
        return f"Multiply must not be between a Series and DataFrame; got {self.x} and {self.y}."


@overload
def negate(x: float, /) -> float: ...


@overload
def negate(x: NDArrayF, /) -> NDArrayF: ...


@overload
def negate(x: SeriesF, /) -> SeriesF: ...


@overload
def negate(x: DataFrame, /) -> DataFrame: ...


@overload
def negate(x: Expression, /) -> Expression: ...


def negate(
    x: float | NDArrayF | SeriesF | DataFrame | Expression, /
) -> float | NDArrayF | SeriesF | DataFrame | Expression:
    """Negate a quantity."""
    return -x


@overload
def negative(x: float, /) -> float: ...


@overload
def negative(x: NDArrayF, /) -> NDArrayF: ...


@overload
def negative(x: SeriesF, /) -> SeriesF: ...


@overload
def negative(x: DataFrame, /) -> DataFrame: ...


@overload
def negative(x: Expression, /) -> Expression: ...


def negative(
    x: float | NDArrayF | SeriesF | DataFrame | Expression, /
) -> float | NDArrayF | SeriesF | DataFrame | Expression:
    """Compute the negative parts of a quantity."""
    if isinstance(x, int | float | ndarray):
        result = -minimum(x, 0.0)
        return where(is_zero(result), 0.0, result)
    try:
        from pandas import DataFrame, Series
    except ModuleNotFoundError:  # pragma: no cover
        pass
    else:
        if isinstance(x, Series | DataFrame):
            result = -minimum(x, 0.0)
            return result.where(is_non_zero(result), 0.0)
    return cvxpy.neg(x)


@overload
def norm(x: NDArrayF1 | SeriesF, /) -> float: ...


@overload
def norm(x: Expression, /) -> Expression: ...


def norm(x: NDArrayF1 | SeriesF | Expression, /) -> float | Expression:
    """Compute the norm of a quantity."""
    if isinstance(x, ndarray):
        return cast(float, numpy.linalg.norm(x))
    try:
        from pandas import Series
    except ModuleNotFoundError:  # pragma: no cover
        pass
    else:
        if isinstance(x, Series):
            return norm(x.to_numpy())
    return cvxpy.norm(x)


@overload
def positive(x: float, /) -> float: ...


@overload
def positive(x: NDArrayF, /) -> NDArrayF: ...


@overload
def positive(x: SeriesF, /) -> SeriesF: ...


@overload
def positive(x: DataFrame, /) -> DataFrame: ...


@overload
def positive(x: Expression, /) -> Expression: ...


def positive(
    x: float | NDArrayF | SeriesF | DataFrame | Expression, /
) -> float | NDArrayF | SeriesF | DataFrame | Expression:
    """Compute the positive parts of a quantity."""
    if isinstance(x, int | float | ndarray):
        result = maximum(x, 0.0)
        return where(is_zero(result), 0.0, result)
    try:
        from pandas import DataFrame, Series
    except ModuleNotFoundError:  # pragma: no cover
        pass
    else:
        if isinstance(x, Series | DataFrame):
            result = maximum(x, 0.0)
            return result.where(is_non_zero(result), 0.0)
    return cvxpy.pos(x)


@overload
def power(x: float, p: float, /) -> float: ...


@overload
def power(x: NDArrayF, p: float, /) -> NDArrayF: ...


@overload
def power(x: Expression, p: float, /) -> Expression: ...


@overload
def power(x: float, p: NDArrayF, /) -> NDArrayF: ...


@overload
def power(x: NDArrayF, p: NDArrayF, /) -> NDArrayF: ...


@overload
def power(x: Expression, p: NDArrayF, /) -> Expression: ...


def power(
    x: float | NDArrayF | Expression, p: float | NDArrayF, /
) -> float | NDArrayF | Expression:
    """Compute the power of a quantity."""
    if isinstance(x, int | float | ndarray):
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


@overload
def scalar_product(x: float, y: float, /) -> float: ...


@overload
def scalar_product(x: NDArrayF, y: float, /) -> NDArrayF: ...


@overload
def scalar_product(x: SeriesF, y: float, /) -> SeriesF: ...


@overload
def scalar_product(x: DataFrame, y: float, /) -> DataFrame: ...


@overload
def scalar_product(x: Expression, y: float, /) -> Expression: ...


@overload
def scalar_product(x: float, y: NDArrayF, /) -> NDArrayF: ...


@overload
def scalar_product(x: NDArrayF, y: NDArrayF, /) -> NDArrayF: ...


@overload
def scalar_product(x: SeriesF, y: NDArrayF, /) -> SeriesF: ...


@overload
def scalar_product(x: DataFrame, y: NDArrayF, /) -> DataFrame: ...


@overload
def scalar_product(x: Expression, y: NDArrayF, /) -> Expression: ...


@overload
def scalar_product(x: float, y: SeriesF, /) -> SeriesF: ...


@overload
def scalar_product(x: NDArrayF, y: SeriesF, /) -> SeriesF: ...


@overload
def scalar_product(x: SeriesF, y: SeriesF, /) -> SeriesF: ...


@overload
def scalar_product(x: Expression, y: SeriesF, /) -> Expression: ...


@overload
def scalar_product(x: float, y: DataFrame, /) -> DataFrame: ...


@overload
def scalar_product(x: NDArrayF, y: DataFrame, /) -> DataFrame: ...


@overload
def scalar_product(x: DataFrame, y: DataFrame, /) -> DataFrame: ...


@overload
def scalar_product(x: Expression, y: DataFrame, /) -> Expression: ...


@overload
def scalar_product(x: float, y: Expression, /) -> Expression: ...


@overload
def scalar_product(x: NDArrayF, y: Expression, /) -> Expression: ...


@overload
def scalar_product(x: SeriesF, y: Expression, /) -> Expression: ...


@overload
def scalar_product(x: DataFrame, y: Expression, /) -> Expression: ...


@overload
def scalar_product(x: Expression, y: Expression, /) -> Expression: ...


def scalar_product(
    x: float | NDArrayF | SeriesF | DataFrame | Expression,
    y: float | NDArrayF | SeriesF | DataFrame | Expression,
    /,
) -> float | NDArrayF | SeriesF | DataFrame | Expression:
    """Compute the scalar product of two quantities."""
    try:
        prod = multiply(cast(Any, x), cast(Any, y))
    except MultiplyError as error:
        raise ScalarProductError(x=error.x, y=error.y) from None
    return sum_(prod)


@dataclass(kw_only=True)
class ScalarProductError(Exception):
    x: SeriesF | DataFrame
    y: SeriesF | DataFrame

    @override
    def __str__(self) -> str:
        return f"Scalar product must not be between a Series and DataFrame; got {self.x} and {self.y}."


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
    if status in {"unbounded", "unbounded_inaccurate"}:
        msg = f"{problem=}"
        raise SolveUnboundedError(msg)
    msg = f"{status=}"  # pragma: no cover
    raise SolveError(msg)  # pragma: no cover


class SolveError(Exception): ...


class SolveInfeasibleError(SolveError): ...


class SolveUnboundedError(SolveError): ...


@overload
def sqrt(x: float, /) -> float: ...


@overload
def sqrt(x: NDArrayF, /) -> NDArrayF: ...


@overload
def sqrt(x: SeriesF, /) -> SeriesF: ...


@overload
def sqrt(x: DataFrame, /) -> DataFrame: ...


@overload
def sqrt(x: Expression, /) -> Expression: ...


def sqrt(
    x: float | NDArrayF | SeriesF | DataFrame | Expression, /
) -> float | NDArrayF | SeriesF | DataFrame | Expression:
    """Compute the square root of a quantity."""
    try:
        from pandas import DataFrame, Series
    except ModuleNotFoundError:  # pragma: no cover
        if isinstance(x, int | float | ndarray):
            return np.sqrt(x)
    else:
        if isinstance(x, int | float | ndarray | Series | DataFrame):
            return np.sqrt(x)
    return cvxpy.sqrt(x)


@overload
def subtract(x: float, y: float, /) -> float: ...


@overload
def subtract(x: NDArrayF, y: float, /) -> NDArrayF: ...


@overload
def subtract(x: Expression, y: float, /) -> Expression: ...


@overload
def subtract(x: float, y: NDArrayF, /) -> NDArrayF: ...


@overload
def subtract(x: NDArrayF, y: NDArrayF, /) -> NDArrayF: ...


@overload
def subtract(x: Expression, y: NDArrayF, /) -> Expression: ...


@overload
def subtract(x: float, y: Expression, /) -> Expression: ...


@overload
def subtract(x: NDArrayF, y: Expression, /) -> Expression: ...


@overload
def subtract(x: Expression, y: Expression, /) -> Expression: ...


def subtract(
    x: float | NDArrayF | Expression, y: float | NDArrayF | Expression, /
) -> float | NDArrayF | Expression:
    """Compute the difference of two quantities."""
    if isinstance(x, int | float | ndarray) and isinstance(y, int | float | ndarray):
        return np.subtract(x, y)
    return cast(Any, x) - cast(Any, y)


@overload
def sum_(x: float | NDArrayF | SeriesF | DataFrame, /) -> float: ...


@overload
def sum_(x: Expression, /) -> Expression: ...


def sum_(
    x: float | NDArrayF | SeriesF | DataFrame | Expression, /
) -> float | Expression:
    """Compute the sum of a quantity."""
    if isinstance(x, int | float):
        return x
    if isinstance(x, ndarray):
        return np.sum(x).item()
    try:
        from pandas import DataFrame, Series
    except ModuleNotFoundError:  # pragma: no cover
        pass
    else:
        if isinstance(x, Series | DataFrame):
            return sum_(x.to_numpy())
    return cvxpy.sum(x)


@overload
def sum_axis0(x: NDArrayF2, /) -> NDArrayF1: ...


@overload
def sum_axis0(x: DataFrame, /) -> SeriesF: ...


@overload
def sum_axis0(x: Expression, /) -> Expression: ...


def sum_axis0(
    x: NDArrayF2 | DataFrame | Expression, /
) -> NDArrayF1 | SeriesF | Expression:
    """Compute the sum along axis 0 of a quantity."""
    return _sum_axis_0_or_1(x, 0)


@overload
def sum_axis1(x: NDArrayF2, /) -> NDArrayF1: ...


@overload
def sum_axis1(x: DataFrame, /) -> SeriesF: ...


@overload
def sum_axis1(x: Expression, /) -> Expression: ...


def sum_axis1(
    x: NDArrayF2 | DataFrame | Expression, /
) -> NDArrayF1 | SeriesF | Expression:
    """Compute the sum along axis 1 of a quantity."""
    return _sum_axis_0_or_1(x, 1)


def _sum_axis_0_or_1(
    x: NDArrayF2 | DataFrame | Expression, axis: Literal[0, 1], /
) -> NDArrayF1 | SeriesF | Expression:
    try:
        from pandas import DataFrame
    except ModuleNotFoundError:  # pragma: no cover
        if isinstance(x, ndarray):
            return np.sum(x, axis=axis)
    else:
        if isinstance(x, ndarray | DataFrame):
            return np.sum(x, axis=axis)
    return cast(Expression, cvxpy.sum(x, axis=axis))


def _check_series_and_dataframe(
    x: float | NDArrayF | SeriesF | DataFrame | Expression,
    y: float | NDArrayF | SeriesF | DataFrame | Expression,
    /,
) -> None:
    try:
        from pandas import DataFrame, Series
    except ModuleNotFoundError:  # pragma: no cover
        return
    if (isinstance(x, Series) and isinstance(y, DataFrame)) or (
        isinstance(x, DataFrame) and isinstance(y, Series)
    ):
        raise _CheckSeriesAndDataFrameError(x=x, y=y)


@dataclass(kw_only=True)
class _CheckSeriesAndDataFrameError(Exception):
    x: SeriesF | DataFrame
    y: SeriesF | DataFrame

    @override
    def __str__(self) -> str:
        return f"Function must not be between a Series and DataFrame; got {self.x} and {self.y}."


__all__ = [
    "MaximumError",
    "MinimumError",
    "MultiplyError",
    "ScalarProductError",
    "SolveError",
    "SolveInfeasibleError",
    "SolveUnboundedError",
    "abs_",
    "add",
    "divide",
    "max_",
    "maximum",
    "min_",
    "minimum",
    "multiply",
    "negate",
    "negative",
    "norm",
    "positive",
    "power",
    "quad_form",
    "scalar_product",
    "solve",
    "sqrt",
    "subtract",
    "sum_",
    "sum_axis0",
    "sum_axis1",
]
