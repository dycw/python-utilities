from __future__ import annotations

from dataclasses import dataclass
from functools import reduce
from itertools import repeat
from typing import TYPE_CHECKING, Any, cast, overload

import numpy as np
from numpy import (
    array,
    bool_,
    digitize,
    dtype,
    errstate,
    exp,
    flatnonzero,
    flip,
    float64,
    full_like,
    inf,
    int64,
    isclose,
    isfinite,
    isinf,
    isnan,
    linspace,
    log,
    nan,
    nanquantile,
    object_,
    prod,
    rint,
    roll,
    where,
)
from numpy.linalg import det, eig
from numpy.random import default_rng
from numpy.typing import NDArray
from typing_extensions import override

from utilities.iterables import is_iterable_not_str

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator


# RNG
DEFAULT_RNG = default_rng()


# dtypes
datetime64Y = dtype("datetime64[Y]")  # noqa: N816
datetime64M = dtype("datetime64[M]")  # noqa: N816
datetime64W = dtype("datetime64[W]")  # noqa: N816
datetime64D = dtype("datetime64[D]")  # noqa: N816
datetime64h = dtype("datetime64[h]")
datetime64m = dtype("datetime64[m]")
datetime64s = dtype("datetime64[s]")
datetime64ms = dtype("datetime64[ms]")
datetime64us = dtype("datetime64[us]")
datetime64ns = dtype("datetime64[ns]")
datetime64ps = dtype("datetime64[ps]")
datetime64fs = dtype("datetime64[fs]")
datetime64as = dtype("datetime64[as]")


timedelta64Y = dtype("timedelta64[Y]")  # noqa: N816
timedelta64M = dtype("timedelta64[M]")  # noqa: N816
timedelta64W = dtype("timedelta64[W]")  # noqa: N816
timedelta64D = dtype("timedelta64[D]")  # noqa: N816
timedelta64h = dtype("timedelta64[h]")
timedelta64m = dtype("timedelta64[m]")
timedelta64s = dtype("timedelta64[s]")
timedelta64ms = dtype("timedelta64[ms]")
timedelta64us = dtype("timedelta64[us]")
timedelta64ns = dtype("timedelta64[ns]")
timedelta64ps = dtype("timedelta64[ps]")
timedelta64fs = dtype("timedelta64[fs]")
timedelta64as = dtype("timedelta64[as]")


# annotations - dtypes
NDArrayA = NDArray[Any]
NDArrayB = NDArray[bool_]
NDArrayF = NDArray[float64]
NDArrayI = NDArray[int64]
NDArrayO = NDArray[object_]


# functions


def array_indexer(i: int, ndim: int, /, *, axis: int = -1) -> tuple[int | slice, ...]:
    """Get the indexer which returns the `ith` slice of an array along an axis."""
    indexer: list[int | slice] = list(repeat(slice(None), times=ndim))
    indexer[axis] = i
    return tuple(indexer)


def as_int(
    array: NDArrayF, /, *, nan: int | None = None, inf: int | None = None
) -> NDArrayI:
    """Safely cast an array of floats into ints."""
    if (is_nan := isnan(array)).any():
        if nan is None:
            msg = f"{array=}"
            raise AsIntError(msg)
        return as_int(where(is_nan, nan, array).astype(float))
    if (is_inf := isinf(array)).any():
        if inf is None:
            msg = f"{array=}"
            raise AsIntError(msg)
        return as_int(where(is_inf, inf, array).astype(float))
    rounded = rint(array)
    if (isfinite(array) & (~isclose(array, rounded))).any():
        msg = f"{array=}"
        raise AsIntError(msg)
    return rounded.astype(int)


class AsIntError(Exception): ...


def discretize(x: NDArrayF, bins: int | Iterable[float], /) -> NDArrayF:
    """Discretize an array of floats.

    Finite values are mapped to {0, ..., bins-1}.
    """
    if len(x) == 0:
        return array([], dtype=float)
    if isinstance(bins, int):
        bins_use = linspace(0, 1, num=bins + 1)
    else:
        bins_use = array(list(bins), dtype=float)
    if (is_fin := isfinite(x)).all():
        edges = nanquantile(x, bins_use)
        edges[[0, -1]] = [-inf, inf]
        return digitize(x, edges[1:]).astype(float)
    out = full_like(x, nan, dtype=float)
    out[is_fin] = discretize(x[is_fin], bins)
    return out


def ewma(array: NDArrayF, halflife: float, /, *, axis: int = -1) -> NDArrayF:
    """Compute the EWMA of an array."""
    from numbagg import move_exp_nanmean

    alpha = _exp_weighted_alpha(halflife)
    return cast(Any, move_exp_nanmean)(array, axis=axis, alpha=alpha)


def exp_moving_sum(array: NDArrayF, halflife: float, /, *, axis: int = -1) -> NDArrayF:
    """Compute the exponentially-weighted moving sum of an array."""
    from numbagg import move_exp_nansum

    alpha = _exp_weighted_alpha(halflife)
    return cast(Any, move_exp_nansum)(array, axis=axis, alpha=alpha)


def _exp_weighted_alpha(halflife: float, /) -> float:
    """Get the alpha."""
    decay = 1.0 - exp(log(0.5) / halflife)
    com = 1.0 / decay - 1.0
    return 1.0 / (1.0 + com)


def ffill(array: NDArrayF, /, *, limit: int | None = None, axis: int = -1) -> NDArrayF:
    """Forward fill the elements in an array."""
    from bottleneck import push

    return push(array, n=limit, axis=axis)


def ffill_non_nan_slices(
    array: NDArrayF, /, *, limit: int | None = None, axis: int = -1
) -> NDArrayF:
    """Forward fill the slices in an array which contain non-nan values."""
    ndim = array.ndim
    arrays = (
        array[array_indexer(i, ndim, axis=axis)] for i in range(array.shape[axis])
    )
    out = array.copy()
    for i, repl_i in _ffill_non_nan_slices_helper(arrays, limit=limit):
        out[array_indexer(i, ndim, axis=axis)] = repl_i
    return out


def _ffill_non_nan_slices_helper(
    arrays: Iterator[NDArrayF], /, *, limit: int | None = None
) -> Iterator[tuple[int, NDArrayF]]:
    """Yield the slices to be pasted in."""
    last: tuple[int, NDArrayF] | None = None
    for i, arr_i in enumerate(arrays):
        if (~isnan(arr_i)).any():
            last = i, arr_i
        elif last is not None:
            last_i, last_sl = last
            if (limit is None) or ((i - last_i) <= limit):
                yield i, last_sl


def fillna(array: NDArrayF, /, *, value: float = 0.0) -> NDArrayF:
    """Fill the null elements in an array."""
    return where(isnan(array), value, array)


def flatn0(array: NDArrayB, /) -> int:
    """Return the index of the unique True element."""
    if not array.any():
        raise FlatN0EmptyError(array=array)
    flattened = flatnonzero(array)
    try:
        return flattened.item()
    except ValueError:
        raise FlatN0MultipleError(array=array) from None


@dataclass(kw_only=True)
class FlatN0Error(Exception):
    array: NDArrayB


@dataclass(kw_only=True)
class FlatN0EmptyError(FlatN0Error):
    @override
    def __str__(self) -> str:
        return f"Array {self.array} must contain a True."


@dataclass(kw_only=True)
class FlatN0MultipleError(FlatN0Error):
    @override
    def __str__(self) -> str:
        return f"Array {self.array} must contain at most one True."


def has_dtype(x: Any, dtype: Any, /) -> bool:
    """Check if an object has the required dtype."""
    if is_iterable_not_str(dtype):
        return any(has_dtype(x, d) for d in dtype)
    return x.dtype == dtype


def is_at_least(
    x: Any,
    y: Any,
    /,
    *,
    rtol: float | None = None,
    atol: float | None = None,
    equal_nan: bool = False,
) -> Any:
    """Check if x >= y."""
    return (x >= y) | _is_close(x, y, rtol=rtol, atol=atol, equal_nan=equal_nan)


def is_at_least_or_nan(
    x: Any,
    y: Any,
    /,
    *,
    rtol: float | None = None,
    atol: float | None = None,
    equal_nan: bool = False,
) -> Any:
    """Check if x >= y or x == nan."""
    return is_at_least(x, y, rtol=rtol, atol=atol, equal_nan=equal_nan) | isnan(x)


def is_at_most(
    x: Any,
    y: Any,
    /,
    *,
    rtol: float | None = None,
    atol: float | None = None,
    equal_nan: bool = False,
) -> Any:
    """Check if x <= y."""
    return (x <= y) | _is_close(x, y, rtol=rtol, atol=atol, equal_nan=equal_nan)


def is_at_most_or_nan(
    x: Any,
    y: Any,
    /,
    *,
    rtol: float | None = None,
    atol: float | None = None,
    equal_nan: bool = False,
) -> Any:
    """Check if x <= y or x == nan."""
    return is_at_most(x, y, rtol=rtol, atol=atol, equal_nan=equal_nan) | isnan(x)


def is_between(
    x: Any,
    low: Any,
    high: Any,
    /,
    *,
    rtol: float | None = None,
    atol: float | None = None,
    equal_nan: bool = False,
    low_equal_nan: bool = False,
    high_equal_nan: bool = False,
) -> Any:
    """Check if low <= x <= high."""
    return is_at_least(
        x, low, rtol=rtol, atol=atol, equal_nan=equal_nan or low_equal_nan
    ) & is_at_most(x, high, rtol=rtol, atol=atol, equal_nan=equal_nan or high_equal_nan)


def is_between_or_nan(
    x: Any,
    low: Any,
    high: Any,
    /,
    *,
    rtol: float | None = None,
    atol: float | None = None,
    equal_nan: bool = False,
    low_equal_nan: bool = False,
    high_equal_nan: bool = False,
) -> Any:
    """Check if low <= x <= high or x == nan."""
    return is_between(
        x,
        low,
        high,
        rtol=rtol,
        atol=atol,
        equal_nan=equal_nan,
        low_equal_nan=low_equal_nan,
        high_equal_nan=high_equal_nan,
    ) | isnan(x)


def _is_close(
    x: Any,
    y: Any,
    /,
    *,
    rtol: float | None = None,
    atol: float | None = None,
    equal_nan: bool = False,
) -> Any:
    """Check if x == y."""
    return np.isclose(
        x,
        y,
        **({} if rtol is None else {"rtol": rtol}),
        **({} if atol is None else {"atol": atol}),
        equal_nan=equal_nan,
    )


def is_empty(shape_or_array: int | tuple[int, ...] | NDArrayA, /) -> bool:
    """Check if an ndarray is empty."""
    if isinstance(shape_or_array, int):
        return shape_or_array == 0
    if isinstance(shape_or_array, tuple):
        return (len(shape_or_array) == 0) or (prod(shape_or_array).item() == 0)
    return is_empty(shape_or_array.shape)


def is_finite_and_integral(
    x: Any, /, *, rtol: float | None = None, atol: float | None = None
) -> Any:
    """Check if -inf < x < inf and x == int(x)."""
    return isfinite(x) & is_integral(x, rtol=rtol, atol=atol)


def is_finite_and_integral_or_nan(
    x: Any, /, *, rtol: float | None = None, atol: float | None = None
) -> Any:
    """Check if -inf < x < inf and x == int(x), or x == nan."""
    return is_finite_and_integral(x, rtol=rtol, atol=atol) | isnan(x)


def is_finite_and_negative(
    x: Any, /, *, rtol: float | None = None, atol: float | None = None
) -> Any:
    """Check if -inf < x < 0."""
    return isfinite(x) & is_negative(x, rtol=rtol, atol=atol)


def is_finite_and_negative_or_nan(
    x: Any, /, *, rtol: float | None = None, atol: float | None = None
) -> Any:
    """Check if -inf < x < 0 or x == nan."""
    return is_finite_and_negative(x, rtol=rtol, atol=atol) | isnan(x)


def is_finite_and_non_negative(
    x: Any, /, *, rtol: float | None = None, atol: float | None = None
) -> Any:
    """Check if 0 <= x < inf."""
    return isfinite(x) & is_non_negative(x, rtol=rtol, atol=atol)


def is_finite_and_non_negative_or_nan(
    x: Any, /, *, rtol: float | None = None, atol: float | None = None
) -> Any:
    """Check if 0 <= x < inf or x == nan."""
    return is_finite_and_non_negative(x, rtol=rtol, atol=atol) | isnan(x)


def is_finite_and_non_positive(
    x: Any, /, *, rtol: float | None = None, atol: float | None = None
) -> Any:
    """Check if -inf < x <= 0."""
    return isfinite(x) & is_non_positive(x, rtol=rtol, atol=atol)


def is_finite_and_non_positive_or_nan(
    x: Any, /, *, rtol: float | None = None, atol: float | None = None
) -> Any:
    """Check if -inf < x <= 0 or x == nan."""
    return is_finite_and_non_positive(x, rtol=rtol, atol=atol) | isnan(x)


def is_finite_and_non_zero(
    x: Any, /, *, rtol: float | None = None, atol: float | None = None
) -> Any:
    """Check if -inf < x < inf, x != 0."""
    return isfinite(x) & is_non_zero(x, rtol=rtol, atol=atol)


def is_finite_and_non_zero_or_nan(
    x: Any, /, *, rtol: float | None = None, atol: float | None = None
) -> Any:
    """Check if x != 0 or x == nan."""
    return is_finite_and_non_zero(x, rtol=rtol, atol=atol) | isnan(x)


def is_finite_and_positive(
    x: Any, /, *, rtol: float | None = None, atol: float | None = None
) -> Any:
    """Check if 0 < x < inf."""
    return isfinite(x) & is_positive(x, rtol=rtol, atol=atol)


def is_finite_and_positive_or_nan(
    x: Any, /, *, rtol: float | None = None, atol: float | None = None
) -> Any:
    """Check if 0 < x < inf or x == nan."""
    return is_finite_and_positive(x, rtol=rtol, atol=atol) | isnan(x)


def is_finite_or_nan(x: Any, /) -> Any:
    """Check if -inf < x < inf or x == nan."""
    return isfinite(x) | isnan(x)


def is_greater_than(
    x: Any,
    y: Any,
    /,
    *,
    rtol: float | None = None,
    atol: float | None = None,
    equal_nan: bool = False,
) -> Any:
    """Check if x > y."""
    return ((x > y) & ~_is_close(x, y, rtol=rtol, atol=atol)) | (
        equal_nan & isnan(x) & isnan(y)
    )


def is_greater_than_or_nan(
    x: Any,
    y: Any,
    /,
    *,
    rtol: float | None = None,
    atol: float | None = None,
    equal_nan: bool = False,
) -> Any:
    """Check if x > y or x == nan."""
    return is_greater_than(x, y, rtol=rtol, atol=atol, equal_nan=equal_nan) | isnan(x)


def is_integral(
    x: Any, /, *, rtol: float | None = None, atol: float | None = None
) -> Any:
    """Check if x == int(x)."""
    return _is_close(x, rint(x), rtol=rtol, atol=atol)


def is_integral_or_nan(
    x: Any, /, *, rtol: float | None = None, atol: float | None = None
) -> Any:
    """Check if x == int(x) or x == nan."""
    return is_integral(x, rtol=rtol, atol=atol) | isnan(x)


def is_less_than(
    x: Any,
    y: Any,
    /,
    *,
    rtol: float | None = None,
    atol: float | None = None,
    equal_nan: bool = False,
) -> Any:
    """Check if x < y."""
    return ((x < y) & ~_is_close(x, y, rtol=rtol, atol=atol)) | (
        equal_nan & isnan(x) & isnan(y)
    )


def is_less_than_or_nan(
    x: Any,
    y: Any,
    /,
    *,
    rtol: float | None = None,
    atol: float | None = None,
    equal_nan: bool = False,
) -> Any:
    """Check if x < y or x == nan."""
    return is_less_than(x, y, rtol=rtol, atol=atol, equal_nan=equal_nan) | isnan(x)


def is_negative(
    x: Any, /, *, rtol: float | None = None, atol: float | None = None
) -> Any:
    """Check if x < 0."""
    return is_less_than(x, 0.0, rtol=rtol, atol=atol)


def is_negative_or_nan(
    x: Any, /, *, rtol: float | None = None, atol: float | None = None
) -> Any:
    """Check if x < 0 or x == nan."""
    return is_negative(x, rtol=rtol, atol=atol) | isnan(x)


def is_non_empty(shape_or_array: int | tuple[int, ...] | NDArrayA, /) -> bool:
    """Check if an ndarray is non-empty."""
    if isinstance(shape_or_array, int):
        return shape_or_array >= 1
    if isinstance(shape_or_array, tuple):
        return (len(shape_or_array) >= 1) and (prod(shape_or_array).item() >= 1)
    return is_non_empty(shape_or_array.shape)


def is_non_negative(
    x: Any, /, *, rtol: float | None = None, atol: float | None = None
) -> Any:
    """Check if x >= 0."""
    return is_at_least(x, 0.0, rtol=rtol, atol=atol)


def is_non_negative_or_nan(
    x: Any, /, *, rtol: float | None = None, atol: float | None = None
) -> Any:
    """Check if x >= 0 or x == nan."""
    return is_non_negative(x, rtol=rtol, atol=atol) | isnan(x)


def is_non_positive(
    x: Any, /, *, rtol: float | None = None, atol: float | None = None
) -> Any:
    """Check if x <= 0."""
    return is_at_most(x, 0.0, rtol=rtol, atol=atol)


def is_non_positive_or_nan(
    x: Any, /, *, rtol: float | None = None, atol: float | None = None
) -> Any:
    """Check if x <=0 or x == nan."""
    return is_non_positive(x, rtol=rtol, atol=atol) | isnan(x)


def is_non_singular(
    array: NDArrayF | NDArrayI,
    /,
    *,
    rtol: float | None = None,
    atol: float | None = None,
) -> bool:
    """Check if det(x) != 0."""
    try:
        with errstate(over="raise"):
            return is_non_zero(det(array), rtol=rtol, atol=atol).item()
    except FloatingPointError:  # pragma: no cover
        return False


def is_non_zero(
    x: Any, /, *, rtol: float | None = None, atol: float | None = None
) -> Any:
    """Check if x != 0."""
    return ~_is_close(x, 0.0, rtol=rtol, atol=atol)


def is_non_zero_or_nan(
    x: Any, /, *, rtol: float | None = None, atol: float | None = None
) -> Any:
    """Check if x != 0 or x == nan."""
    return is_non_zero(x, rtol=rtol, atol=atol) | isnan(x)


def is_positive(
    x: Any, /, *, rtol: float | None = None, atol: float | None = None
) -> Any:
    """Check if x > 0."""
    return is_greater_than(x, 0, rtol=rtol, atol=atol)


def is_positive_or_nan(
    x: Any, /, *, rtol: float | None = None, atol: float | None = None
) -> Any:
    """Check if x > 0 or x == nan."""
    return is_positive(x, rtol=rtol, atol=atol) | isnan(x)


def is_positive_semidefinite(x: NDArrayF | NDArrayI, /) -> bool:
    """Check if `x` is positive semidefinite."""
    if not is_symmetric(x):
        return False
    w, _ = eig(x)
    return bool(is_non_negative(w).all())


def is_symmetric(
    array: NDArrayF | NDArrayI,
    /,
    *,
    rtol: float | None = None,
    atol: float | None = None,
    equal_nan: bool = False,
) -> bool:
    """Check if x == x.T."""
    m, n = array.shape
    return (m == n) and (
        _is_close(array, array.T, rtol=rtol, atol=atol, equal_nan=equal_nan)
        .all()
        .item()
    )


def is_zero(x: Any, /, *, rtol: float | None = None, atol: float | None = None) -> Any:
    """Check if x == 0."""
    return _is_close(x, 0.0, rtol=rtol, atol=atol)


def is_zero_or_finite_and_non_micro(
    x: Any, /, *, rtol: float | None = None, atol: float | None = None
) -> Any:
    """Check if x == 0, or -inf < x < inf and ~isclose(x, 0)."""
    zero = 0.0
    return (x == zero) | is_finite_and_non_zero(x, rtol=rtol, atol=atol)


def is_zero_or_finite_and_non_micro_or_nan(
    x: Any, /, *, rtol: float | None = None, atol: float | None = None
) -> Any:
    """Check if x == 0, or -inf < x < inf and ~isclose(x, 0), or x == nan."""
    return is_zero_or_finite_and_non_micro(x, rtol=rtol, atol=atol) | isnan(x)


def is_zero_or_nan(
    x: Any, /, *, rtol: float | None = None, atol: float | None = None
) -> Any:
    """Check if x > 0 or x == nan."""
    return is_zero(x, rtol=rtol, atol=atol) | isnan(x)


def is_zero_or_non_micro(
    x: Any, /, *, rtol: float | None = None, atol: float | None = None
) -> Any:
    """Check if x == 0 or ~isclose(x, 0)."""
    zero = 0.0
    return (x == zero) | is_non_zero(x, rtol=rtol, atol=atol)


def is_zero_or_non_micro_or_nan(
    x: Any, /, *, rtol: float | None = None, atol: float | None = None
) -> Any:
    """Check if x == 0 or ~isclose(x, 0) or x == nan."""
    return is_zero_or_non_micro(x, rtol=rtol, atol=atol) | isnan(x)


@overload
def maximum(x: float, /) -> float: ...
@overload
def maximum(x0: float, x1: float, /) -> float: ...
@overload
def maximum(x0: float, x1: NDArrayF, /) -> NDArrayF: ...
@overload
def maximum(x0: NDArrayF, x1: float, /) -> NDArrayF: ...
@overload
def maximum(x0: NDArrayF, x1: NDArrayF, /) -> NDArrayF: ...
@overload
def maximum(x0: float, x1: float, x2: float, /) -> float: ...
@overload
def maximum(x0: float, x1: float, x2: NDArrayF, /) -> NDArrayF: ...
@overload
def maximum(x0: float, x1: NDArrayF, x2: float, /) -> NDArrayF: ...
@overload
def maximum(x0: float, x1: NDArrayF, x2: NDArrayF, /) -> NDArrayF: ...
@overload
def maximum(x0: NDArrayF, x1: float, x2: float, /) -> NDArrayF: ...
@overload
def maximum(x0: NDArrayF, x1: float, x2: NDArrayF, /) -> NDArrayF: ...
@overload
def maximum(x0: NDArrayF, x1: NDArrayF, x2: float, /) -> NDArrayF: ...
@overload
def maximum(x0: NDArrayF, x1: NDArrayF, x2: NDArrayF, /) -> NDArrayF: ...
def maximum(*xs: float | NDArrayF) -> float | NDArrayF:
    """Compute the maximum of a number of quantities."""
    return reduce(np.maximum, xs)


@overload
def minimum(x: float, /) -> float: ...
@overload
def minimum(x0: float, x1: float, /) -> float: ...
@overload
def minimum(x0: float, x1: NDArrayF, /) -> NDArrayF: ...
@overload
def minimum(x0: NDArrayF, x1: float, /) -> NDArrayF: ...
@overload
def minimum(x0: NDArrayF, x1: NDArrayF, /) -> NDArrayF: ...
@overload
def minimum(x0: float, x1: float, x2: float, /) -> float: ...
@overload
def minimum(x0: float, x1: float, x2: NDArrayF, /) -> NDArrayF: ...
@overload
def minimum(x0: float, x1: NDArrayF, x2: float, /) -> NDArrayF: ...
@overload
def minimum(x0: float, x1: NDArrayF, x2: NDArrayF, /) -> NDArrayF: ...
@overload
def minimum(x0: NDArrayF, x1: float, x2: float, /) -> NDArrayF: ...
@overload
def minimum(x0: NDArrayF, x1: float, x2: NDArrayF, /) -> NDArrayF: ...
@overload
def minimum(x0: NDArrayF, x1: NDArrayF, x2: float, /) -> NDArrayF: ...
@overload
def minimum(x0: NDArrayF, x1: NDArrayF, x2: NDArrayF, /) -> NDArrayF: ...
def minimum(*xs: float | NDArrayF) -> float | NDArrayF:
    """Compute the minimum of a number of quantities."""
    return reduce(np.minimum, xs)


def pct_change(
    array: NDArrayF | NDArrayI,
    /,
    *,
    limit: int | None = None,
    n: int = 1,
    axis: int = -1,
) -> NDArrayF:
    """Compute the percentage change in an array."""
    if n == 0:
        raise PctChangeError
    if n > 0:
        filled = ffill(array.astype(float), limit=limit, axis=axis)
        shifted = shift(filled, n=n, axis=axis)
        with errstate(all="ignore"):
            ratio = (filled / shifted) if n >= 0 else (shifted / filled)
        return where(isfinite(array), ratio - 1.0, nan)
    flipped = cast(NDArrayF | NDArrayI, flip(array, axis=axis))
    result = pct_change(flipped, limit=limit, n=-n, axis=axis)
    return flip(result, axis=axis)


@dataclass(kw_only=True)
class PctChangeError(Exception):
    @override
    def __str__(self) -> str:
        return "Shift must be non-zero"


def shift(array: NDArrayF | NDArrayI, /, *, n: int = 1, axis: int = -1) -> NDArrayF:
    """Shift the elements of an array."""
    if n == 0:
        raise ShiftError
    as_float = array.astype(float)
    shifted = roll(as_float, n, axis=axis)
    indexer = list(repeat(slice(None), times=array.ndim))
    indexer[axis] = slice(n) if n >= 0 else slice(n, None)
    shifted[tuple(indexer)] = nan
    return shifted


@dataclass(kw_only=True)
class ShiftError(Exception):
    @override
    def __str__(self) -> str:
        return "Shift must be non-zero"


def shift_bool(
    array: NDArrayB, /, *, n: int = 1, axis: int = -1, fill_value: bool = False
) -> NDArrayB:
    """Shift the elements of a boolean array."""
    shifted = shift(array.astype(float), n=n, axis=axis)
    return fillna(shifted, value=float(fill_value)).astype(bool)


__all__ = [
    "DEFAULT_RNG",
    "AsIntError",
    "FlatN0EmptyError",
    "FlatN0Error",
    "FlatN0MultipleError",
    "NDArrayA",
    "NDArrayB",
    "NDArrayF",
    "NDArrayI",
    "NDArrayO",
    "PctChangeError",
    "ShiftError",
    "array_indexer",
    "as_int",
    "datetime64D",
    "datetime64M",
    "datetime64W",
    "datetime64Y",
    "datetime64as",
    "datetime64fs",
    "datetime64h",
    "datetime64m",
    "datetime64ms",
    "datetime64ns",
    "datetime64ps",
    "datetime64s",
    "datetime64us",
    "discretize",
    "ewma",
    "exp_moving_sum",
    "ffill",
    "ffill_non_nan_slices",
    "fillna",
    "flatn0",
    "has_dtype",
    "is_at_least",
    "is_at_least_or_nan",
    "is_at_most",
    "is_at_most_or_nan",
    "is_between",
    "is_between_or_nan",
    "is_empty",
    "is_finite_and_integral",
    "is_finite_and_integral_or_nan",
    "is_finite_and_negative",
    "is_finite_and_negative_or_nan",
    "is_finite_and_non_negative",
    "is_finite_and_non_negative_or_nan",
    "is_finite_and_non_positive",
    "is_finite_and_non_positive_or_nan",
    "is_finite_and_non_zero",
    "is_finite_and_non_zero_or_nan",
    "is_finite_and_positive",
    "is_finite_and_positive_or_nan",
    "is_finite_or_nan",
    "is_greater_than",
    "is_greater_than_or_nan",
    "is_integral",
    "is_integral_or_nan",
    "is_less_than",
    "is_less_than_or_nan",
    "is_negative",
    "is_negative_or_nan",
    "is_non_empty",
    "is_non_negative",
    "is_non_negative_or_nan",
    "is_non_positive",
    "is_non_positive_or_nan",
    "is_non_singular",
    "is_non_zero",
    "is_non_zero_or_nan",
    "is_positive",
    "is_positive_or_nan",
    "is_positive_semidefinite",
    "is_symmetric",
    "is_zero",
    "is_zero_or_finite_and_non_micro",
    "is_zero_or_finite_and_non_micro_or_nan",
    "is_zero_or_nan",
    "is_zero_or_non_micro",
    "is_zero_or_non_micro_or_nan",
    "maximum",
    "minimum",
    "pct_change",
    "shift",
    "shift_bool",
]
