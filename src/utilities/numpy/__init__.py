from __future__ import annotations

import datetime as dt
from collections.abc import Iterable
from collections.abc import Iterator
from functools import reduce
from itertools import repeat
from typing import Any
from typing import Literal
from typing import NoReturn
from typing import cast
from typing import overload

import numpy as np
from bottleneck import push
from numpy import array
from numpy import datetime64
from numpy import digitize
from numpy import dtype
from numpy import errstate
from numpy import exp
from numpy import flatnonzero
from numpy import flip
from numpy import full_like
from numpy import inf
from numpy import isclose
from numpy import isfinite
from numpy import isinf
from numpy import isnan
from numpy import linspace
from numpy import log
from numpy import nan
from numpy import nanquantile
from numpy import ndarray
from numpy import prod
from numpy import rint
from numpy import roll
from numpy import where
from numpy.linalg import det
from numpy.linalg import eig

from utilities._numbagg import move_exp_nanmean
from utilities._numbagg import move_exp_nansum
from utilities.datetime import EPOCH_UTC
from utilities.errors import redirect_error
from utilities.iterables import is_iterable_not_str
from utilities.math.typing import FloatFinPos
from utilities.numpy.checks import _is_close
from utilities.numpy.checks import is_at_least
from utilities.numpy.checks import is_at_least_or_nan
from utilities.numpy.checks import is_at_most
from utilities.numpy.checks import is_at_most_or_nan
from utilities.numpy.checks import is_between
from utilities.numpy.checks import is_between_or_nan
from utilities.numpy.checks import is_finite_and_integral
from utilities.numpy.checks import is_finite_and_integral_or_nan
from utilities.numpy.checks import is_finite_and_negative
from utilities.numpy.checks import is_finite_and_negative_or_nan
from utilities.numpy.checks import is_finite_and_non_negative
from utilities.numpy.checks import is_finite_and_non_negative_or_nan
from utilities.numpy.checks import is_finite_and_non_positive
from utilities.numpy.checks import is_finite_and_non_positive_or_nan
from utilities.numpy.checks import is_finite_and_non_zero
from utilities.numpy.checks import is_finite_and_non_zero_or_nan
from utilities.numpy.checks import is_finite_and_positive
from utilities.numpy.checks import is_finite_and_positive_or_nan
from utilities.numpy.checks import is_finite_or_nan
from utilities.numpy.checks import is_greater_than
from utilities.numpy.checks import is_greater_than_or_nan
from utilities.numpy.checks import is_integral
from utilities.numpy.checks import is_integral_or_nan
from utilities.numpy.checks import is_less_than
from utilities.numpy.checks import is_less_than_or_nan
from utilities.numpy.checks import is_negative
from utilities.numpy.checks import is_negative_or_nan
from utilities.numpy.checks import is_non_negative
from utilities.numpy.checks import is_non_negative_or_nan
from utilities.numpy.checks import is_non_positive
from utilities.numpy.checks import is_non_positive_or_nan
from utilities.numpy.checks import is_non_zero
from utilities.numpy.checks import is_non_zero_or_nan
from utilities.numpy.checks import is_positive
from utilities.numpy.checks import is_positive_or_nan
from utilities.numpy.checks import is_zero
from utilities.numpy.checks import is_zero_or_finite_and_non_micro
from utilities.numpy.checks import is_zero_or_finite_and_non_micro_or_nan
from utilities.numpy.checks import is_zero_or_nan
from utilities.numpy.checks import is_zero_or_non_micro
from utilities.numpy.checks import is_zero_or_non_micro_or_nan
from utilities.numpy.dtypes import datetime64as
from utilities.numpy.dtypes import datetime64D
from utilities.numpy.dtypes import datetime64fs
from utilities.numpy.dtypes import datetime64h
from utilities.numpy.dtypes import datetime64M
from utilities.numpy.dtypes import datetime64m
from utilities.numpy.dtypes import datetime64ms
from utilities.numpy.dtypes import datetime64ns
from utilities.numpy.dtypes import datetime64ps
from utilities.numpy.dtypes import datetime64s
from utilities.numpy.dtypes import datetime64us
from utilities.numpy.dtypes import datetime64W
from utilities.numpy.dtypes import datetime64Y
from utilities.numpy.typing import NDArrayA
from utilities.numpy.typing import NDArrayB
from utilities.numpy.typing import NDArrayB1
from utilities.numpy.typing import NDArrayDD
from utilities.numpy.typing import NDArrayF
from utilities.numpy.typing import NDArrayF1
from utilities.numpy.typing import NDArrayF2
from utilities.numpy.typing import NDArrayI
from utilities.numpy.typing import NDArrayI2
from utilities.re import extract_group

_ = [
    datetime64Y,
    datetime64M,
    datetime64W,
    datetime64D,
    datetime64h,
    datetime64m,
    datetime64s,
    datetime64ms,
    datetime64us,
    datetime64ns,
    datetime64ps,
    datetime64fs,
    datetime64as,
    is_at_least_or_nan,
    is_at_least,
    is_at_most_or_nan,
    is_at_most,
    is_between_or_nan,
    is_between,
    is_finite_and_integral_or_nan,
    is_finite_and_integral,
    is_finite_and_negative_or_nan,
    is_finite_and_negative,
    is_finite_and_non_negative_or_nan,
    is_finite_and_non_negative,
    is_finite_and_non_positive_or_nan,
    is_finite_and_non_positive,
    is_finite_and_non_zero_or_nan,
    is_finite_and_non_zero,
    is_finite_and_positive_or_nan,
    is_finite_and_positive,
    is_finite_or_nan,
    is_greater_than_or_nan,
    is_greater_than,
    is_integral_or_nan,
    is_integral,
    is_less_than_or_nan,
    is_less_than,
    is_negative_or_nan,
    is_negative,
    is_non_negative_or_nan,
    is_non_negative,
    is_non_positive_or_nan,
    is_non_positive,
    is_non_zero_or_nan,
    is_non_zero,
    is_positive_or_nan,
    is_positive,
    is_zero_or_finite_and_non_micro_or_nan,
    is_zero_or_finite_and_non_micro,
    is_zero_or_nan,
    is_zero_or_non_micro_or_nan,
    is_zero_or_non_micro,
    is_zero,
]
Datetime64Unit = Literal[
    "Y", "M", "W", "D", "h", "m", "s", "ms", "us", "ns", "ps", "fs", "as"
]
Datetime64Kind = Literal["date", "time"]


def array_indexer(
    i: int, ndim: int, /, *, axis: int = -1
) -> tuple[int | slice, ...]:
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
            raise NanElementsError(msg)
        return as_int(where(is_nan, nan, array).astype(float))
    if (is_inf := isinf(array)).any():
        if inf is None:
            msg = f"{array=}"
            raise InfElementsError(msg)
        return as_int(where(is_inf, inf, array).astype(float))
    if (isfinite(array) & (~isclose(array, rint(array)))).any():
        msg = f"{array=}"
        raise NonIntegralElementsError(msg)
    return array.astype(int)


class NanElementsError(Exception):
    """Raised when there are nan elements."""


class InfElementsError(Exception):
    """Raised when there are inf elements."""


class NonIntegralElementsError(Exception):
    """Raised when there are non-integral elements."""


def date_to_datetime64(date: dt.date, /) -> datetime64:
    """Convert a `dt.date` to `numpy.datetime64`."""

    return datetime64(date, "D")


DATE_MIN_AS_DATETIME64 = date_to_datetime64(dt.date.min)
DATE_MAX_AS_DATETIME64 = date_to_datetime64(dt.date.max)


def datetime_to_datetime64(datetime: dt.datetime, /) -> datetime64:
    """Convert a `dt.datetime` to `numpy.datetime64`."""

    return datetime64(datetime, "us")


DATETIME_MIN_AS_DATETIMETIME64 = datetime_to_datetime64(dt.datetime.min)
DATETIME_MAX_AS_DATETIMETIME64 = datetime_to_datetime64(dt.datetime.max)


def datetime64_to_date(datetime: datetime64, /) -> dt.date:
    """Convert a `numpy.datetime64` to a `dt.date`."""

    as_int = datetime64_to_int(datetime)
    if (dtype := datetime.dtype) == datetime64D:
        try:
            return (EPOCH_UTC + dt.timedelta(days=as_int)).date()
        except OverflowError:
            msg = f"{datetime=}, {dtype=}"
            raise DateOverflowError(msg) from None
    msg = f"{datetime=}, {dtype=}"
    raise NotImplementedError(msg)


def datetime64_to_int(datetime: datetime64, /) -> int:
    """Convert a `numpy.datetime64` to an `int`."""

    return datetime.astype(int).item()


DATE_MIN_AS_INT = datetime64_to_int(DATE_MIN_AS_DATETIME64)
DATE_MAX_AS_INT = datetime64_to_int(DATE_MAX_AS_DATETIME64)
DATETIME_MIN_AS_INT = datetime64_to_int(DATETIME_MIN_AS_DATETIMETIME64)
DATETIME_MAX_AS_INT = datetime64_to_int(DATETIME_MAX_AS_DATETIMETIME64)


def datetime64_to_datetime(datetime: datetime64, /) -> dt.datetime:
    """Convert a `numpy.datetime64` to a `dt.datetime`."""

    as_int = datetime64_to_int(datetime)
    if (dtype := datetime.dtype) == datetime64ms:
        try:
            return EPOCH_UTC + dt.timedelta(milliseconds=as_int)
        except OverflowError:
            msg = f"{datetime=}, {dtype=}"
            raise DateOverflowError(msg) from None
    elif dtype == datetime64us:
        return EPOCH_UTC + dt.timedelta(microseconds=as_int)
    elif dtype == datetime64ns:
        microseconds, nanoseconds = divmod(as_int, int(1e3))
        if nanoseconds != 0:
            msg = f"{datetime=}, {nanoseconds=}"
            raise LossOfNanosecondsError(msg)
        return EPOCH_UTC + dt.timedelta(microseconds=microseconds)
    else:
        msg = f"{datetime=}, {dtype=}"
        raise NotImplementedError(msg)


def datetime64_dtype_to_unit(dtype: Any, /) -> Datetime64Unit:
    """Convert a `datetime64` dtype to a unit."""
    return cast(Datetime64Unit, extract_group(r"^<M8\[(\w+)\]$", dtype.str))


def datetime64_unit_to_dtype(unit: Datetime64Unit, /) -> Any:
    """Convert a `datetime64` unit to a dtype."""
    return dtype(f"datetime64[{unit}]")


def datetime64_unit_to_kind(unit: Datetime64Unit, /) -> Datetime64Kind:
    """Convert a `datetime64` unit to a kind."""
    return "date" if unit in {"Y", "M", "W", "D"} else "time"


class DateOverflowError(ValueError):
    """Raised when a date overflows."""


class LossOfNanosecondsError(ValueError):
    """Raised when nanoseconds are lost."""


def discretize(x: NDArrayF1, bins: int | Iterable[float], /) -> NDArrayF1:
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


def ewma(
    array: NDArrayF, halflife: FloatFinPos, /, *, axis: int = -1
) -> NDArrayF:
    """Compute the EWMA of an array."""
    alpha = _exp_weighted_alpha(halflife)
    return cast(Any, move_exp_nanmean)(array, axis=axis, alpha=alpha)


def exp_moving_sum(
    array: NDArrayF, halflife: FloatFinPos, /, *, axis: int = -1
) -> NDArrayF:
    """Compute the exponentially-weighted moving sum of an array."""
    alpha = _exp_weighted_alpha(halflife)
    return cast(Any, move_exp_nansum)(array, axis=axis, alpha=alpha)


def _exp_weighted_alpha(halflife: FloatFinPos, /) -> float:
    """Get the alpha."""
    decay = 1.0 - exp(log(0.5) / halflife)
    com = 1.0 / decay - 1.0
    return 1.0 / (1.0 + com)


def ffill(
    array: NDArrayF, /, *, limit: int | None = None, axis: int = -1
) -> NDArrayF:
    """Forward fill the elements in an array."""
    return push(array, n=limit, axis=axis)


def ffill_non_nan_slices(
    array: NDArrayF, /, *, limit: int | None = None, axis: int = -1
) -> NDArrayF:
    """Forward fill the slices in an array which contain non-nan values."""

    ndim = array.ndim
    arrays = (
        array[array_indexer(i, ndim, axis=axis)]
        for i in range(array.shape[axis])
    )
    out = array.copy()
    for i, repl_i in _ffill_non_nan_slices_helper(arrays, limit=limit):
        out[array_indexer(i, ndim, axis=axis)] = repl_i
    return out


def _ffill_non_nan_slices_helper(
    arrays: Iterator[NDArrayF], /, *, limit: int | None = None
) -> Iterator[tuple[int, NDArrayF]]:
    """Iterator yielding the slices to be pasted in."""
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


def flatn0(array: NDArrayB1, /) -> int:
    """Return the index of the unique True element."""
    if not array.any():
        msg = f"{array=}"
        raise NoTrueElementsError(msg)
    try:
        return flatnonzero(array).item()
    except ValueError as error:
        msg = f"{array=}"
        redirect_error(
            error,
            "can only convert an array of size 1 to a Python scalar",
            MultipleTrueElementsError(msg),
        )


class NoTrueElementsError(Exception):
    """Raised when an array has no true elements."""


class MultipleTrueElementsError(Exception):
    """Raised when an array has multiple true elements."""


def get_fill_value(dtype: Any, /) -> Any:
    """Get the default fill value for a given dtype."""
    if dtype == bool:
        return False
    if dtype in (datetime64D, datetime64Y, datetime64ns):
        return datetime64("NaT")
    if dtype == float:
        return nan
    if dtype == int:
        return 0
    if dtype == object:
        return None
    msg = f"{dtype=}"
    raise InvalidDTypeError(msg)


class InvalidDTypeError(TypeError):
    """Raised when a dtype is invalid."""


def has_dtype(x: Any, dtype: Any, /) -> bool:
    """Check if an object has the required dtype."""
    if is_iterable_not_str(dtype):
        return any(has_dtype(x, d) for d in dtype)
    return x.dtype == dtype


def is_empty(shape_or_array: int | tuple[int, ...] | NDArrayA, /) -> bool:
    """Check if an ndarray is empty."""
    if isinstance(shape_or_array, int):
        return shape_or_array == 0
    if isinstance(shape_or_array, tuple):
        return (len(shape_or_array) == 0) or (prod(shape_or_array).item() == 0)
    return is_empty(shape_or_array.shape)


def is_non_empty(shape_or_array: int | tuple[int, ...] | NDArrayA, /) -> bool:
    """Check if an ndarray is non-empty."""
    if isinstance(shape_or_array, int):
        return shape_or_array >= 1
    if isinstance(shape_or_array, tuple):
        return (len(shape_or_array) >= 1) and (prod(shape_or_array).item() >= 1)
    return is_non_empty(shape_or_array.shape)


def is_non_singular(
    array: NDArrayF2 | NDArrayI2,
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


def is_positive_semidefinite(x: NDArrayF2 | NDArrayI2, /) -> bool:
    """Check if `x` is positive semidefinite."""
    if not is_symmetric(x):
        return False
    w, _ = eig(x)
    return bool(is_non_negative(w).all())


def is_symmetric(
    array: NDArrayF2 | NDArrayI2,
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


@overload
def maximum(x: float, /) -> float:
    ...


@overload
def maximum(x0: float, x1: float, /) -> float:
    ...


@overload
def maximum(x0: float, x1: NDArrayF, /) -> NDArrayF:
    ...


@overload
def maximum(x0: NDArrayF, x1: float, /) -> NDArrayF:
    ...


@overload
def maximum(x0: NDArrayF, x1: NDArrayF, /) -> NDArrayF:
    ...


@overload
def maximum(x0: float, x1: float, x2: float, /) -> float:
    ...


@overload
def maximum(x0: float, x1: float, x2: NDArrayF, /) -> NDArrayF:
    ...


@overload
def maximum(x0: float, x1: NDArrayF, x2: float, /) -> NDArrayF:
    ...


@overload
def maximum(x0: float, x1: NDArrayF, x2: NDArrayF, /) -> NDArrayF:
    ...


@overload
def maximum(x0: NDArrayF, x1: float, x2: float, /) -> NDArrayF:
    ...


@overload
def maximum(x0: NDArrayF, x1: float, x2: NDArrayF, /) -> NDArrayF:
    ...


@overload
def maximum(x0: NDArrayF, x1: NDArrayF, x2: float, /) -> NDArrayF:
    ...


@overload
def maximum(x0: NDArrayF, x1: NDArrayF, x2: NDArrayF, /) -> NDArrayF:
    ...


def maximum(*xs: float | NDArrayF) -> float | NDArrayF:
    """Compute the maximum of a number of quantities."""
    return reduce(np.maximum, xs)


@overload
def minimum(x: float, /) -> float:
    ...


@overload
def minimum(x0: float, x1: float, /) -> float:
    ...


@overload
def minimum(x0: float, x1: NDArrayF, /) -> NDArrayF:
    ...


@overload
def minimum(x0: NDArrayF, x1: float, /) -> NDArrayF:
    ...


@overload
def minimum(x0: NDArrayF, x1: NDArrayF, /) -> NDArrayF:
    ...


@overload
def minimum(x0: float, x1: float, x2: float, /) -> float:
    ...


@overload
def minimum(x0: float, x1: float, x2: NDArrayF, /) -> NDArrayF:
    ...


@overload
def minimum(x0: float, x1: NDArrayF, x2: float, /) -> NDArrayF:
    ...


@overload
def minimum(x0: float, x1: NDArrayF, x2: NDArrayF, /) -> NDArrayF:
    ...


@overload
def minimum(x0: NDArrayF, x1: float, x2: float, /) -> NDArrayF:
    ...


@overload
def minimum(x0: NDArrayF, x1: float, x2: NDArrayF, /) -> NDArrayF:
    ...


@overload
def minimum(x0: NDArrayF, x1: NDArrayF, x2: float, /) -> NDArrayF:
    ...


@overload
def minimum(x0: NDArrayF, x1: NDArrayF, x2: NDArrayF, /) -> NDArrayF:
    ...


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
        msg = f"{n=}"
        raise ZeroPercentageChangeSpanError(msg)
    if n > 0:
        filled = ffill(array.astype(float), limit=limit, axis=axis)
        shifted = shift(filled, n=n, axis=axis)
        with errstate(all="ignore"):
            ratio = (filled / shifted) if n >= 0 else (shifted / filled)
        return where(isfinite(array), ratio - 1.0, nan)
    flipped = cast(NDArrayF | NDArrayI, flip(array, axis=axis))
    result = pct_change(flipped, limit=limit, n=-n, axis=axis)
    return flip(result, axis=axis)


class ZeroPercentageChangeSpanError(Exception):
    """Raised when the percentage change span is zero."""


def redirect_to_empty_numpy_concatenate_error(error: ValueError, /) -> NoReturn:
    """Redirect to the `EmptyNumpyConcatenateError`."""
    redirect_error(
        error,
        "need at least one array to concatenate",
        EmptyNumpyConcatenateError,
    )


class EmptyNumpyConcatenateError(ValueError):
    """Raised when there are no arrays to concatenate."""


def shift(
    array: NDArrayF | NDArrayI, /, *, n: int = 1, axis: int = -1
) -> NDArrayF:
    """Shift the elements of an array."""
    if n == 0:
        msg = f"{n=}"
        raise ZeroShiftError(msg)
    as_float = array.astype(float)
    shifted = roll(as_float, n, axis=axis)
    indexer = list(repeat(slice(None), times=array.ndim))
    indexer[axis] = slice(n) if n >= 0 else slice(n, None)
    shifted[tuple(indexer)] = nan
    return shifted


class ZeroShiftError(Exception):
    """Raised when the shift is zero."""


def shift_bool(
    array: NDArrayB, /, *, n: int = 1, axis: int = -1, fill_value: bool = False
) -> NDArrayB:
    """Shift the elements of a boolean array."""
    shifted = shift(array.astype(float), n=n, axis=axis)
    return fillna(shifted, value=float(fill_value)).astype(bool)


@overload
def year(date: datetime64, /) -> int:
    ...


@overload
def year(date: NDArrayDD, /) -> NDArrayI:
    ...


def year(date: datetime64 | NDArrayDD, /) -> int | NDArrayI:
    """Convert a date/array of dates into a year/array of years."""
    years = 1970 + date.astype(datetime64Y).astype(int)
    return years if isinstance(date, ndarray) else years.item()
