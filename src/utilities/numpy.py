from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from functools import partial, reduce
from itertools import repeat
from typing import TYPE_CHECKING, Any, SupportsIndex, overload, override

import numpy as np
from numpy import (
    argsort,
    array,
    bool_,
    complex128,
    digitize,
    dtype,
    errstate,
    exp,
    flatnonzero,
    floating,
    full_like,
    inf,
    integer,
    isclose,
    isfinite,
    isinf,
    isnan,
    linspace,
    nan,
    nanquantile,
    object_,
    prod,
    rint,
    roll,
    where,
)
from numpy.fft import fft, fftfreq, ifft
from numpy.linalg import det, eig
from numpy.random import default_rng
from numpy.typing import NDArray

from utilities.iterables import always_iterable, is_iterable_not_str

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from utilities.types import MaybeIterable


type ShapeLike = SupportsIndex | Sequence[SupportsIndex]


##


DEFAULT_RNG = default_rng()


##


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


##


NDArrayA = NDArray[Any]
NDArrayB = NDArray[bool_]
NDArrayC128 = NDArray[complex128]
NDArrayF = NDArray[floating[Any]]
NDArrayI = NDArray[integer[Any]]
NDArrayO = NDArray[object_]


##


def array_indexer(i: int, ndim: int, /, *, axis: int = -1) -> tuple[int | slice, ...]:
    """Get the indexer which returns the `ith` slice of an array along an axis."""
    indexer: list[int | slice] = list(repeat(slice(None), times=ndim))
    indexer[axis] = i
    return tuple(indexer)


##


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


##


def bernoulli(
    *, true: float = 0.5, seed: int | None = None, size: ShapeLike = ()
) -> NDArrayB:
    """Return a set of Bernoulli random variates."""
    from numpy.random import default_rng

    rng = default_rng(seed=seed)
    return rng.binomial(1, true, size=size).astype(bool)


##


def boxcar(
    array: NDArrayF,
    /,
    *,
    loc_low: float = -1.0,
    slope_low: float = 1.0,
    loc_high: float = 1.0,
    slope_high: float = 1.0,
    rtol: float | None = None,
    atol: float | None = None,
) -> NDArrayF:
    """Construct a boxcar function."""
    if not is_at_most(loc_low, loc_high, rtol=rtol, atol=atol):
        raise _BoxCarLocationsError(low=loc_low, high=loc_high)
    if not is_positive(slope_low, rtol=rtol, atol=atol):
        raise _BoxCarLowerBoundSlopeError(slope=slope_low)
    if not is_positive(slope_high, rtol=rtol, atol=atol):
        raise _BoxCarUpperBoundSlopeError(slope=slope_high)
    return (
        sigmoid(array, loc=loc_low, slope=slope_low)
        + sigmoid(array, loc=loc_high, slope=-slope_high)
    ) / 2


@dataclass(kw_only=True, slots=True)
class BoxCarError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _BoxCarLocationsError(BoxCarError):
    low: float
    high: float

    @override
    def __str__(self) -> str:
        return f"Location parameters must be consistent; got {self.low} and {self.high}"


@dataclass(kw_only=True, slots=True)
class _BoxCarLowerBoundSlopeError(BoxCarError):
    slope: float

    @override
    def __str__(self) -> str:
        return f"Lower-bound slope parameter must be positive; got {self.slope}"


@dataclass(kw_only=True, slots=True)
class _BoxCarUpperBoundSlopeError(BoxCarError):
    slope: float

    @override
    def __str__(self) -> str:
        return f"Upper-bound slope parameter must be positive; got {self.slope}"


##


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


##


def fillna(array: NDArrayF, /, *, value: float = 0.0) -> NDArrayF:
    """Fill the null elements in an array."""
    return where(isnan(array), value, array)


##


def adjust_frequencies(
    array: NDArrayF,
    /,
    *,
    filters: MaybeIterable[Callable[[NDArrayF], NDArrayB]] | None = None,
    weights: MaybeIterable[Callable[[NDArrayF], NDArrayF]] | None = None,
    d: int = 1,
) -> NDArrayF:
    """Adjust an array via its FFT frequencies."""
    (n,) = array.shape
    amplitudes = fft(array)
    freqs = fftfreq(n, d=d)
    if filters is not None:
        amplitudes = reduce(
            partial(_adjust_frequencies_filter_one, freqs=freqs),
            always_iterable(filters),
            amplitudes,
        )
    if weights is not None:
        amplitudes = reduce(
            partial(_adjust_frequencies_weight_one, freqs=freqs),
            always_iterable(weights),
            amplitudes,
        )
    return ifft(amplitudes).real


def _adjust_frequencies_filter_one(
    acc: NDArrayC128, el: Callable[[NDArrayF], NDArrayB], /, *, freqs: NDArrayF
) -> NDArrayC128:
    return where(el(freqs), acc, 0.0)


def _adjust_frequencies_weight_one(
    acc: NDArrayC128, el: Callable[[NDArrayF], NDArrayF], /, *, freqs: NDArrayF
) -> NDArrayC128:
    return acc * el(freqs)


##


def flatn0(array: NDArrayB, /) -> int:
    """Return the index of the unique True element."""
    if not array.any():
        raise FlatN0EmptyError(array=array)
    flattened = flatnonzero(array)
    try:
        return flattened.item()
    except ValueError:
        raise FlatN0MultipleError(array=array) from None


@dataclass(kw_only=True, slots=True)
class FlatN0Error(Exception):
    array: NDArrayB


@dataclass(kw_only=True, slots=True)
class FlatN0EmptyError(FlatN0Error):
    @override
    def __str__(self) -> str:
        return f"Array {self.array} must contain a True."


@dataclass(kw_only=True, slots=True)
class FlatN0MultipleError(FlatN0Error):
    @override
    def __str__(self) -> str:
        return f"Array {self.array} must contain at most one True."


##


def get_frequency_spectrum(array: NDArrayF, /, *, d: int = 1) -> NDArrayF:
    """Get the frequency spectrum."""
    (n,) = array.shape
    amplitudes = fft(array)
    freqs = fftfreq(n, d=d)
    amplitudes = np.abs(amplitudes)
    data = np.hstack([freqs.reshape(-1, 1), amplitudes.reshape(-1, 1)])
    return data[argsort(data[:, 0])]


##


def has_dtype(x: Any, dtype: Any, /) -> bool:
    """Check if an object has the required dtype."""
    if is_iterable_not_str(dtype):
        return any(has_dtype(x, d) for d in dtype)
    return x.dtype == dtype


##


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


##


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


##


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


##


def is_empty(shape_or_array: int | tuple[int, ...] | NDArrayA, /) -> bool:
    """Check if an ndarray is empty."""
    if isinstance(shape_or_array, int):
        return shape_or_array == 0
    if isinstance(shape_or_array, tuple):
        return (len(shape_or_array) == 0) or (prod(shape_or_array).item() == 0)
    return is_empty(shape_or_array.shape)


##


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


##


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


##


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


##


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


##


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


##


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


##


def is_finite_or_nan(x: Any, /) -> Any:
    """Check if -inf < x < inf or x == nan."""
    return isfinite(x) | isnan(x)


##


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


##


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


##


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


##


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


##


def is_non_empty(shape_or_array: int | tuple[int, ...] | NDArrayA, /) -> bool:
    """Check if an ndarray is non-empty."""
    if isinstance(shape_or_array, int):
        return shape_or_array >= 1
    if isinstance(shape_or_array, tuple):
        return (len(shape_or_array) >= 1) and (prod(shape_or_array).item() >= 1)
    return is_non_empty(shape_or_array.shape)


##


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


##


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


##


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


##


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


##


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


##


def is_positive_semidefinite(x: NDArrayF | NDArrayI, /) -> bool:
    """Check if `x` is positive semidefinite."""
    if not is_symmetric(x):
        return False
    w, _ = eig(x)
    return bool(is_non_negative(w).all())


##


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


##


def is_zero(x: Any, /, *, rtol: float | None = None, atol: float | None = None) -> Any:
    """Check if x == 0."""
    return _is_close(x, 0.0, rtol=rtol, atol=atol)


def is_zero_or_nan(
    x: Any, /, *, rtol: float | None = None, atol: float | None = None
) -> Any:
    """Check if x > 0 or x == nan."""
    return is_zero(x, rtol=rtol, atol=atol) | isnan(x)


##


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


##


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


##


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


##


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


@dataclass(kw_only=True, slots=True)
class ShiftError(Exception):
    @override
    def __str__(self) -> str:
        return "Shift must be non-zero"


##


def shift_bool(
    array: NDArrayB, /, *, n: int = 1, axis: int = -1, fill_value: bool = False
) -> NDArrayB:
    """Shift the elements of a boolean array."""
    shifted = shift(array.astype(float), n=n, axis=axis)
    return fillna(shifted, value=float(fill_value)).astype(bool)


##


def sigmoid(
    array: NDArrayF,
    /,
    *,
    loc: float = 0.0,
    slope: float = 1.0,
    rtol: float | None = None,
    atol: float | None = None,
) -> NDArrayF:
    """Construct a sigmoid function."""
    if is_zero(slope, rtol=rtol, atol=atol):
        raise SigmoidError
    return 1 / (1 + exp(-slope * (array - loc)))


@dataclass(kw_only=True, slots=True)
class SigmoidError(Exception):
    @override
    def __str__(self) -> str:
        return "Slope must be non-zero"


##


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


__all__ = [
    "DEFAULT_RNG",
    "AsIntError",
    "BoxCarError",
    "FlatN0EmptyError",
    "FlatN0Error",
    "FlatN0MultipleError",
    "NDArrayA",
    "NDArrayB",
    "NDArrayF",
    "NDArrayI",
    "NDArrayO",
    "ShapeLike",
    "ShiftError",
    "SigmoidError",
    "adjust_frequencies",
    "array_indexer",
    "as_int",
    "bernoulli",
    "boxcar",
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
    "fillna",
    "flatn0",
    "get_frequency_spectrum",
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
    "shift_bool",
    "sigmoid",
]
