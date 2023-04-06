import datetime as dt
from collections.abc import Iterable, Iterator
from functools import reduce
from itertools import repeat
from typing import Any, Literal, NoReturn, Optional, Union, cast, overload

import numpy as np
from beartype import beartype
from bottleneck import push
from numpy import (
    array,
    datetime64,
    digitize,
    dtype,
    errstate,
    flatnonzero,
    flip,
    full_like,
    inf,
    isclose,
    isfinite,
    isinf,
    isnan,
    linspace,
    nan,
    nanquantile,
    ndarray,
    prod,
    rint,
    roll,
    where,
)
from numpy.linalg import det, eig
from numpy.typing import NDArray

from utilities.datetime import EPOCH_UTC
from utilities.errors import redirect_error
from utilities.iterables import is_iterable_not_str
from utilities.numpy.typing import (
    NDArrayB,
    NDArrayB1,
    NDArrayDD,
    NDArrayF,
    NDArrayF1,
    NDArrayF2,
    NDArrayI,
    NDArrayI2,
    _is_close,
    datetime64D,
    datetime64ms,
    datetime64ns,
    datetime64us,
    datetime64Y,
    is_at_least,
    is_at_least_or_nan,
    is_at_most,
    is_at_most_or_nan,
    is_between,
    is_between_or_nan,
    is_finite_and_integral,
    is_finite_and_integral_or_nan,
    is_finite_and_negative,
    is_finite_and_negative_or_nan,
    is_finite_and_non_negative,
    is_finite_and_non_negative_or_nan,
    is_finite_and_non_positive,
    is_finite_and_non_positive_or_nan,
    is_finite_and_non_zero,
    is_finite_and_non_zero_or_nan,
    is_finite_and_positive,
    is_finite_and_positive_or_nan,
    is_finite_or_nan,
    is_greater_than,
    is_greater_than_or_nan,
    is_integral,
    is_integral_or_nan,
    is_less_than,
    is_less_than_or_nan,
    is_negative,
    is_negative_or_nan,
    is_non_negative,
    is_non_negative_or_nan,
    is_non_positive,
    is_non_positive_or_nan,
    is_non_zero,
    is_non_zero_or_nan,
    is_positive,
    is_positive_or_nan,
    is_zero,
    is_zero_or_finite_and_non_micro,
    is_zero_or_finite_and_non_micro_or_nan,
    is_zero_or_nan,
    is_zero_or_non_micro,
    is_zero_or_non_micro_or_nan,
)
from utilities.re import extract_group

_ = (
    datetime64D,
    datetime64Y,
    datetime64ns,
    is_at_least,
    is_at_least_or_nan,
    is_at_most,
    is_at_most_or_nan,
    is_between,
    is_between_or_nan,
    is_finite_and_integral,
    is_finite_and_integral_or_nan,
    is_finite_and_negative,
    is_finite_and_negative_or_nan,
    is_finite_and_non_negative,
    is_finite_and_non_negative_or_nan,
    is_finite_and_non_positive,
    is_finite_and_non_positive_or_nan,
    is_finite_and_non_zero,
    is_finite_and_non_zero_or_nan,
    is_finite_and_positive,
    is_finite_and_positive_or_nan,
    is_finite_or_nan,
    is_greater_than,
    is_greater_than_or_nan,
    is_integral,
    is_integral_or_nan,
    is_less_than,
    is_less_than_or_nan,
    is_negative,
    is_negative_or_nan,
    is_non_negative,
    is_non_negative_or_nan,
    is_non_positive,
    is_non_positive_or_nan,
    is_non_zero,
    is_non_zero_or_nan,
    is_positive,
    is_positive_or_nan,
    is_zero,
    is_zero_or_finite_and_non_micro,
    is_zero_or_finite_and_non_micro_or_nan,
    is_zero_or_nan,
    is_zero_or_non_micro,
    is_zero_or_non_micro_or_nan,
)


Datetime64Unit = Literal[
    "Y", "M", "W", "D", "h", "m", "s", "ms", "us", "ns", "ps", "fs", "as"
]


@beartype
def array_indexer(
    i: int, ndim: int, /, *, axis: int = -1
) -> tuple[Union[int, slice], ...]:
    """Get the indexer which returns the `ith` slice of an array along an axis."""
    indexer: list[Union[int, slice]] = list(repeat(slice(None), times=ndim))
    indexer[axis] = i
    return tuple(indexer)


@beartype
def as_int(
    array: NDArrayF, /, *, nan: Optional[int] = None, inf: Optional[int] = None
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


@beartype
def date_to_datetime64(date: dt.date, /) -> datetime64:
    """Convert a `dt.date` to `numpy.datetime64`."""

    return datetime64(date, "D")


DATE_MIN_AS_DATETIME64 = date_to_datetime64(dt.date.min)
DATE_MAX_AS_DATETIME64 = date_to_datetime64(dt.date.max)


@beartype
def datetime_to_datetime64(datetime: dt.datetime, /) -> datetime64:
    """Convert a `dt.datetime` to `numpy.datetime64`."""

    return datetime64(datetime, "us")


@beartype
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


@beartype
def datetime64_to_int(datetime: datetime64, /) -> int:
    """Convert a `numpy.datetime64` to an `int`."""

    return datetime.astype(int).item()


@beartype
def datetime64_to_datetime(datetime: datetime64, /) -> dt.datetime:
    """Convert a `numpy.datetime64` to a `dt.datetime`."""

    as_int = datetime.astype(int).item()
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


@beartype
def datetime64_dtype_to_unit(dtype: Any, /) -> Datetime64Unit:
    """Convert a `datetime64` dtype to a unit."""
    return cast(Datetime64Unit, extract_group(r"^<M8\[(\w+)\]$", dtype.str))


@beartype
def datetime64_unit_to_dtype(unit: Datetime64Unit, /) -> Any:
    """Convert a `datetime64` unit to a dtype."""
    return dtype(f"datetime64[{unit}]")


class DateOverflowError(ValueError):
    """Raised when a date overflows."""


class LossOfNanosecondsError(ValueError):
    """Raised when nanoseconds are lost."""


@beartype
def discretize(x: NDArrayF1, bins: Union[int, Iterable[float]], /) -> NDArrayF1:
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


@beartype
def ffill(
    array: NDArrayF, /, *, limit: Optional[int] = None, axis: int = -1
) -> NDArrayF:
    """Forward fill the elements in an array."""
    return push(array, n=limit, axis=axis)


@beartype
def ffill_non_nan_slices(
    array: NDArrayF, /, *, limit: Optional[int] = None, axis: int = -1
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


@beartype
def _ffill_non_nan_slices_helper(
    arrays: Iterator[NDArrayF], /, *, limit: Optional[int] = None
) -> Iterator[tuple[int, NDArrayF]]:
    """Iterator yielding the slices to be pasted in."""
    last: Optional[tuple[int, NDArrayF]] = None
    for i, arr_i in enumerate(arrays):
        if (~isnan(arr_i)).any():
            last = i, arr_i
        elif last is not None:
            last_i, last_sl = last
            if (limit is None) or ((i - last_i) <= limit):
                yield i, last_sl


@beartype
def fillna(array: NDArrayF, /, *, value: float = 0.0) -> NDArrayF:
    """Fill the null elements in an array."""
    return where(isnan(array), value, array)


@beartype
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


@beartype
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


@beartype
def has_dtype(x: Any, dtype: Any, /) -> bool:
    """Check if an object has the required dtype."""
    if is_iterable_not_str(dtype):
        return any(has_dtype(x, d) for d in dtype)
    return x.dtype == dtype


@beartype
def is_empty(shape_or_array: Union[int, tuple[int, ...], NDArray[Any]], /) -> bool:
    """Check if an ndarray is empty."""
    if isinstance(shape_or_array, int):
        return shape_or_array == 0
    if isinstance(shape_or_array, tuple):
        return (len(shape_or_array) == 0) or (prod(shape_or_array).item() == 0)
    return is_empty(shape_or_array.shape)


@beartype
def is_non_empty(shape_or_array: Union[int, tuple[int, ...], NDArray[Any]], /) -> bool:
    """Check if an ndarray is non-empty."""
    if isinstance(shape_or_array, int):
        return shape_or_array >= 1
    if isinstance(shape_or_array, tuple):
        return (len(shape_or_array) >= 1) and (prod(shape_or_array).item() >= 1)
    return is_non_empty(shape_or_array.shape)


@beartype
def is_non_singular(
    array: Union[NDArrayF2, NDArrayI2],
    /,
    *,
    rtol: Optional[float] = None,
    atol: Optional[float] = None,
) -> bool:
    """Check if det(x) != 0."""
    try:
        with errstate(over="raise"):
            return is_non_zero(det(array), rtol=rtol, atol=atol).item()
    except FloatingPointError:
        return False


@beartype
def is_positive_semidefinite(x: Union[NDArrayF2, NDArrayI2], /) -> bool:
    """Check if `x` is positive semidefinite."""
    if not is_symmetric(x):
        return False
    w, _ = eig(x)
    return bool(is_non_negative(w).all())


@beartype
def is_symmetric(
    array: Union[NDArrayF2, NDArrayI2],
    /,
    *,
    rtol: Optional[float] = None,
    atol: Optional[float] = None,
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


@beartype
def maximum(*xs: Union[float, NDArrayF]) -> Union[float, NDArrayF]:
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


@beartype
def minimum(*xs: Union[float, NDArrayF]) -> Union[float, NDArrayF]:
    """Compute the minimum of a number of quantities."""
    return reduce(np.minimum, xs)


@beartype
def pct_change(
    array: Union[NDArrayF, NDArrayI],
    /,
    *,
    limit: Optional[int] = None,
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
    flipped = cast(Union[NDArrayF, NDArrayI], flip(array, axis=axis))
    result = pct_change(flipped, limit=limit, n=-n, axis=axis)
    return flip(result, axis=axis)


class ZeroPercentageChangeSpanError(Exception):
    """Raised when the percentage change span is zero."""


@beartype
def redirect_to_empty_numpy_concatenate_error(error: ValueError, /) -> NoReturn:
    """Redirect to the `EmptyNumpyConcatenateError`."""
    redirect_error(
        error, "need at least one array to concatenate", EmptyNumpyConcatenateError
    )


class EmptyNumpyConcatenateError(ValueError):
    """Raised when there are no arrays to concatenate."""


@beartype
def shift(
    array: Union[NDArrayF, NDArrayI], /, *, n: int = 1, axis: int = -1
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


@beartype
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


@beartype
def year(date: Union[datetime64, NDArrayDD], /) -> Union[int, NDArrayI]:
    """Convert a date/array of dates into a year/array of years."""
    years = 1970 + date.astype(datetime64Y).astype(int)
    return years if isinstance(date, ndarray) else years.item()
