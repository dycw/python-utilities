from __future__ import annotations

import datetime as dt
from collections.abc import Callable, Iterable, Iterator
from functools import reduce
from itertools import repeat
from typing import Annotated, Any, Literal, NoReturn, cast, overload

import numpy as np
from numpy import (
    array,
    bool_,
    datetime64,
    digitize,
    dtype,
    errstate,
    flatnonzero,
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
    ndarray,
    object_,
    prod,
    rint,
    roll,
    unravel_index,
    where,
)
from numpy.linalg import det, eig
from numpy.random import default_rng
from numpy.typing import NDArray

from utilities.datetime import EPOCH_UTC
from utilities.errors import redirect_error
from utilities.itertools import is_iterable_not_str
from utilities.re import extract_group

# RNG
DEFAULT_RNG = default_rng()


# types
Datetime64Unit = Literal[
    "Y", "M", "W", "D", "h", "m", "s", "ms", "us", "ns", "ps", "fs", "as"
]
Datetime64Kind = Literal["date", "time"]


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


# annotations - dtypes
NDArrayA = NDArray[Any]
NDArrayB = NDArray[bool_]
NDArrayDY = NDArray[cast(Any, datetime64Y)]
NDArrayDM = NDArray[cast(Any, datetime64M)]
NDArrayDW = NDArray[cast(Any, datetime64W)]
NDArrayDD = NDArray[cast(Any, datetime64D)]
NDArrayDh = NDArray[cast(Any, datetime64h)]
NDArrayDm = NDArray[cast(Any, datetime64m)]
NDArrayDs = NDArray[cast(Any, datetime64s)]
NDArrayDms = NDArray[cast(Any, datetime64ms)]
NDArrayDus = NDArray[cast(Any, datetime64us)]
NDArrayDns = NDArray[cast(Any, datetime64ns)]
NDArrayDps = NDArray[cast(Any, datetime64ps)]
NDArrayDfs = NDArray[cast(Any, datetime64fs)]
NDArrayDas = NDArray[cast(Any, datetime64as)]
NDArrayD = (
    NDArrayDY
    | NDArrayDM
    | NDArrayDW
    | NDArrayDD
    | NDArrayDh
    | NDArrayDm
    | NDArrayDs
    | NDArrayDms
    | NDArrayDus
    | NDArrayDns
    | NDArrayDps
    | NDArrayDfs
    | NDArrayDas
)
NDArrayF = NDArray[float64]
NDArrayI = NDArray[int64]
NDArrayO = NDArray[object_]


# annotations - dtypes
class _HasDType:
    def __init__(self, dtype: Any, /) -> None:
        super().__init__()  # pragma: no cover
        self.ndim = dtype  # pragma: no cover


def _dtype_annotation(dtype: Any, /) -> Any:
    try:
        from beartype.vale import IsAttr, IsEqual
    except ModuleNotFoundError:  # pragma: no cover
        return _HasDType(dtype)
    return IsAttr["dtype", IsEqual[dtype]]


DTypeB = _dtype_annotation(bool)
DTypeDns = _dtype_annotation(datetime64ns)
DTypeF = _dtype_annotation(float)
DTypeI = _dtype_annotation(int64)
DTypeO = _dtype_annotation(object)


# annotations - ndims
class _HasNDim:
    def __init__(self, ndim: int, /) -> None:
        super().__init__()  # pragma: no cover
        self.ndim = ndim  # pragma: no cover


def _ndim_annotation(ndim: int, /) -> Any:
    try:
        from beartype.vale import IsAttr, IsEqual
    except ModuleNotFoundError:  # pragma: no cover
        return _HasNDim(ndim)
    return IsAttr["ndim", IsEqual[ndim]]


NDim0 = _ndim_annotation(0)
NDim1 = _ndim_annotation(1)
NDim2 = _ndim_annotation(2)
NDim3 = _ndim_annotation(3)
NDArray0 = Annotated[NDArrayA, NDim0]
NDArray1 = Annotated[NDArrayA, NDim1]
NDArray2 = Annotated[NDArrayA, NDim2]
NDArray3 = Annotated[NDArrayA, NDim3]


# annotations - dtype & ndim
NDArrayB0 = Annotated[NDArrayB, NDim0]
NDArrayD0 = Annotated[NDArrayD, NDim0]
NDArrayDY0 = Annotated[NDArrayDY, NDim0]
NDArrayDM0 = Annotated[NDArrayDM, NDim0]
NDArrayDW0 = Annotated[NDArrayDW, NDim0]
NDArrayDD0 = Annotated[NDArrayDD, NDim0]
NDArrayDh0 = Annotated[NDArrayDh, NDim0]
NDArrayDm0 = Annotated[NDArrayDm, NDim0]
NDArrayDs0 = Annotated[NDArrayDs, NDim0]
NDArrayDms0 = Annotated[NDArrayDms, NDim0]
NDArrayDus0 = Annotated[NDArrayDus, NDim0]
NDArrayDns0 = Annotated[NDArrayDns, NDim0]
NDArrayDps0 = Annotated[NDArrayDps, NDim0]
NDArrayDfs0 = Annotated[NDArrayDfs, NDim0]
NDArrayDas0 = Annotated[NDArrayDas, NDim0]
NDArrayF0 = Annotated[NDArrayF, NDim0]
NDArrayI0 = Annotated[NDArrayI, NDim0]
NDArrayO0 = Annotated[NDArrayO, NDim0]

NDArrayB1 = Annotated[NDArrayB, NDim1]
NDArrayD1 = Annotated[NDArrayD, NDim1]
NDArrayDY1 = Annotated[NDArrayDY, NDim1]
NDArrayDM1 = Annotated[NDArrayDM, NDim1]
NDArrayDW1 = Annotated[NDArrayDW, NDim1]
NDArrayDD1 = Annotated[NDArrayDD, NDim1]
NDArrayDh1 = Annotated[NDArrayDh, NDim1]
NDArrayDm1 = Annotated[NDArrayDm, NDim1]
NDArrayDs1 = Annotated[NDArrayDs, NDim1]
NDArrayDms1 = Annotated[NDArrayDms, NDim1]
NDArrayDus1 = Annotated[NDArrayDus, NDim1]
NDArrayDns1 = Annotated[NDArrayDns, NDim1]
NDArrayDps1 = Annotated[NDArrayDps, NDim1]
NDArrayDfs1 = Annotated[NDArrayDfs, NDim1]
NDArrayDas1 = Annotated[NDArrayDas, NDim1]
NDArrayF1 = Annotated[NDArrayF, NDim1]
NDArrayI1 = Annotated[NDArrayI, NDim1]
NDArrayO1 = Annotated[NDArrayO, NDim1]

NDArrayB2 = Annotated[NDArrayB, NDim2]
NDArrayD2 = Annotated[NDArrayD, NDim2]
NDArrayDY2 = Annotated[NDArrayDY, NDim2]
NDArrayDM2 = Annotated[NDArrayDM, NDim2]
NDArrayDW2 = Annotated[NDArrayDW, NDim2]
NDArrayDD2 = Annotated[NDArrayDD, NDim2]
NDArrayDh2 = Annotated[NDArrayDh, NDim2]
NDArrayDm2 = Annotated[NDArrayDm, NDim2]
NDArrayDs2 = Annotated[NDArrayDs, NDim2]
NDArrayDms2 = Annotated[NDArrayDms, NDim2]
NDArrayDus2 = Annotated[NDArrayDus, NDim2]
NDArrayDns2 = Annotated[NDArrayDns, NDim2]
NDArrayDps2 = Annotated[NDArrayDps, NDim2]
NDArrayDfs2 = Annotated[NDArrayDfs, NDim2]
NDArrayDas2 = Annotated[NDArrayDas, NDim2]
NDArrayF2 = Annotated[NDArrayF, NDim2]
NDArrayI2 = Annotated[NDArrayI, NDim2]
NDArrayO2 = Annotated[NDArrayO, NDim2]

NDArrayB3 = Annotated[NDArrayB, NDim3]
NDArrayD3 = Annotated[NDArrayD, NDim3]
NDArrayDY3 = Annotated[NDArrayDY, NDim3]
NDArrayDM3 = Annotated[NDArrayDM, NDim3]
NDArrayDW3 = Annotated[NDArrayDW, NDim3]
NDArrayDD3 = Annotated[NDArrayDD, NDim3]
NDArrayDh3 = Annotated[NDArrayDh, NDim3]
NDArrayDm3 = Annotated[NDArrayDm, NDim3]
NDArrayDs3 = Annotated[NDArrayDs, NDim3]
NDArrayDms3 = Annotated[NDArrayDms, NDim3]
NDArrayDus3 = Annotated[NDArrayDus, NDim3]
NDArrayDns3 = Annotated[NDArrayDns, NDim3]
NDArrayDps3 = Annotated[NDArrayDps, NDim3]
NDArrayDfs3 = Annotated[NDArrayDfs, NDim3]
NDArrayDas3 = Annotated[NDArrayDas, NDim3]
NDArrayF3 = Annotated[NDArrayF, NDim3]
NDArrayI3 = Annotated[NDArrayI, NDim3]
NDArrayO3 = Annotated[NDArrayO, NDim3]


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


DATETIME_MIN_AS_DATETIME64 = datetime_to_datetime64(dt.datetime.min)
DATETIME_MAX_AS_DATETIME64 = datetime_to_datetime64(dt.datetime.max)


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

    return datetime.astype(int64).item()


DATE_MIN_AS_INT = datetime64_to_int(DATE_MIN_AS_DATETIME64)
DATE_MAX_AS_INT = datetime64_to_int(DATE_MAX_AS_DATETIME64)
DATETIME_MIN_AS_INT = datetime64_to_int(DATETIME_MIN_AS_DATETIME64)
DATETIME_MAX_AS_INT = datetime64_to_int(DATETIME_MAX_AS_DATETIME64)


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


class DateOverflowError(Exception):
    """Raised when a date overflows."""


class LossOfNanosecondsError(Exception):
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


class InvalidDTypeError(Exception):
    """Raised when a dtype is invalid."""


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


def redirect_to_empty_numpy_concatenate_error(error: ValueError, /) -> NoReturn:
    """Redirect to the `EmptyNumpyConcatenateError`."""
    redirect_error(
        error, "need at least one array to concatenate", EmptyNumpyConcatenateError
    )


class EmptyNumpyConcatenateError(Exception):
    """Raised when there are no arrays to concatenate."""


def shift(array: NDArrayF | NDArrayI, /, *, n: int = 1, axis: int = -1) -> NDArrayF:
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


# annotations - predicates


class _HasPredicate:
    def __init__(self, predicate: Callable[..., Any], /) -> None:
        super().__init__()  # pragma: no cover
        self.predicate = predicate  # pragma: no cover


def _predicate_annotation(predicate: Callable[..., Any], /) -> Any:
    """Apply the predicate to a subset of a float array."""
    try:
        from beartype.vale import Is
    except ModuleNotFoundError:  # pragma: no cover
        return _HasPredicate(predicate)

    def inner(array: NDArrayI | NDArrayF, /) -> bool:
        if (size := array.size) == 0:
            return True
        if size == 1:
            return predicate(array).item()
        num_samples = round(log(size))
        indices = DEFAULT_RNG.integers(0, size, size=num_samples)
        sample = array[unravel_index(indices, array.shape)]
        return predicate(sample).all().item()

    return Is[cast(Any, inner)]


IsFinite = _predicate_annotation(isfinite)
IsFiniteAndIntegral = _predicate_annotation(is_finite_and_integral)
IsFiniteAndIntegralOrNan = _predicate_annotation(is_finite_and_integral_or_nan)
IsFiniteAndNegative = _predicate_annotation(is_finite_and_negative)
IsFiniteAndNegativeOrNan = _predicate_annotation(is_finite_and_negative_or_nan)
IsFiniteAndNonNegative = _predicate_annotation(is_finite_and_non_negative)
IsFiniteAndNonNegativeOrNan = _predicate_annotation(is_finite_and_non_negative_or_nan)
IsFiniteAndNonPositive = _predicate_annotation(is_finite_and_non_positive)
IsFiniteAndNonPositiveOrNan = _predicate_annotation(is_finite_and_non_positive_or_nan)
IsFiniteAndNonZero = _predicate_annotation(is_finite_and_non_zero)
IsFiniteAndNonZeroOrNan = _predicate_annotation(is_finite_and_non_zero_or_nan)
IsFiniteAndPositive = _predicate_annotation(is_finite_and_positive)
IsFiniteAndPositiveOrNan = _predicate_annotation(is_finite_and_positive_or_nan)
IsFiniteOrNan = _predicate_annotation(is_finite_or_nan)
IsIntegral = _predicate_annotation(is_integral)
IsIntegralOrNan = _predicate_annotation(is_integral_or_nan)
IsNegative = _predicate_annotation(is_negative)
IsNegativeOrNan = _predicate_annotation(is_negative_or_nan)
IsNonNegative = _predicate_annotation(is_non_negative)
IsNonNegativeOrNan = _predicate_annotation(is_non_negative_or_nan)
IsNonPositive = _predicate_annotation(is_non_positive)
IsNonPositiveOrNan = _predicate_annotation(is_non_positive_or_nan)
IsNonZero = _predicate_annotation(is_non_zero)
IsNonZeroOrNan = _predicate_annotation(is_non_zero_or_nan)
IsPositive = _predicate_annotation(is_positive)
IsPositiveOrNan = _predicate_annotation(is_positive_or_nan)
IsZero = _predicate_annotation(is_zero)
IsZeroOrFiniteAndNonMicro = _predicate_annotation(is_zero_or_finite_and_non_micro)
IsZeroOrFiniteAndNonMicroOrNan = _predicate_annotation(
    is_zero_or_finite_and_non_micro_or_nan
)
IsZeroOrNan = _predicate_annotation(is_zero_or_nan)
IsZeroOrNonMicro = _predicate_annotation(is_zero_or_non_micro)
IsZeroOrNonMicroOrNan = _predicate_annotation(is_zero_or_non_micro_or_nan)


# annotations - int & predicates
NDArrayINeg = Annotated[NDArrayI, IsNegative]
NDArrayINonNeg = Annotated[NDArrayI, IsNonNegative]
NDArrayINonPos = Annotated[NDArrayI, IsNonPositive]
NDArrayINonZr = Annotated[NDArrayI, IsNonZero]
NDArrayIPos = Annotated[NDArrayI, IsPositive]
NDArrayIZr = Annotated[NDArrayI, IsZero]


# annotations - float & predicates
NDArrayFFin = Annotated[NDArrayF, IsFinite]
NDArrayFFinInt = Annotated[NDArrayF, IsFiniteAndIntegral]
NDArrayFFinIntNan = Annotated[NDArrayF, IsFiniteAndIntegralOrNan]
NDArrayFFinNeg = Annotated[NDArrayF, IsFiniteAndNegative]
NDArrayFFinNegNan = Annotated[NDArrayF, IsFiniteAndNegativeOrNan]
NDArrayFFinNonNeg = Annotated[NDArrayF, IsFiniteAndNonNegative]
NDArrayFFinNonNegNan = Annotated[NDArrayF, IsFiniteAndNonNegativeOrNan]
NDArrayFFinNonPos = Annotated[NDArrayF, IsFiniteAndNonPositive]
NDArrayFFinNonPosNan = Annotated[NDArrayF, IsFiniteAndNonPositiveOrNan]
NDArrayFFinNonZr = Annotated[NDArrayF, IsFiniteAndNonZero]
NDArrayFFinNonZrNan = Annotated[NDArrayF, IsFiniteAndNonZeroOrNan]
NDArrayFFinPos = Annotated[NDArrayF, IsFiniteAndPositive]
NDArrayFFinPosNan = Annotated[NDArrayF, IsFiniteAndPositiveOrNan]
NDArrayFFinNan = Annotated[NDArrayF, IsFiniteOrNan]
NDArrayFInt = Annotated[NDArrayF, IsIntegral]
NDArrayFIntNan = Annotated[NDArrayF, IsIntegralOrNan]
NDArrayFNeg = Annotated[NDArrayF, IsNegative]
NDArrayFNegNan = Annotated[NDArrayF, IsNegativeOrNan]
NDArrayFNonNeg = Annotated[NDArrayF, IsNonNegative]
NDArrayFNonNegNan = Annotated[NDArrayF, IsNonNegativeOrNan]
NDArrayFNonPos = Annotated[NDArrayF, IsNonPositive]
NDArrayFNonPosNan = Annotated[NDArrayF, IsNonPositiveOrNan]
NDArrayFNonZr = Annotated[NDArrayF, IsNonZero]
NDArrayFNonZrNan = Annotated[NDArrayF, IsNonZeroOrNan]
NDArrayFPos = Annotated[NDArrayF, IsPositive]
NDArrayFPosNan = Annotated[NDArrayF, IsPositiveOrNan]
NDArrayFZr = Annotated[NDArrayF, IsZero]
NDArrayFZrFinNonMic = Annotated[NDArrayF, IsZeroOrFiniteAndNonMicro]
NDArrayFZrFinNonMicNan = Annotated[NDArrayF, IsZeroOrFiniteAndNonMicroOrNan]
NDArrayFZrNan = Annotated[NDArrayF, IsZeroOrNan]
NDArrayFZrNonMic = Annotated[NDArrayF, IsZeroOrNonMicro]
NDArrayFZrNonMicNan = Annotated[NDArrayF, IsZeroOrNonMicroOrNan]


# annotations - int, ndim & predicate
NDArrayI0Neg = Annotated[NDArrayI, NDim0, IsNegative]
NDArrayI0NonNeg = Annotated[NDArrayI, NDim0, IsNonNegative]
NDArrayI0NonPos = Annotated[NDArrayI, NDim0, IsNonPositive]
NDArrayI0NonZr = Annotated[NDArrayI, NDim0, IsNonZero]
NDArrayI0Pos = Annotated[NDArrayI, NDim0, IsPositive]
NDArrayI0Zr = Annotated[NDArrayI, NDim0, IsZero]

NDArrayI1Neg = Annotated[NDArrayI, NDim1, IsNegative]
NDArrayI1NonNeg = Annotated[NDArrayI, NDim1, IsNonNegative]
NDArrayI1NonPos = Annotated[NDArrayI, NDim1, IsNonPositive]
NDArrayI1NonZr = Annotated[NDArrayI, NDim1, IsNonZero]
NDArrayI1Pos = Annotated[NDArrayI, NDim1, IsPositive]
NDArrayI1Zr = Annotated[NDArrayI, NDim1, IsZero]

NDArrayI2Neg = Annotated[NDArrayI, NDim2, IsNegative]
NDArrayI2NonNeg = Annotated[NDArrayI, NDim2, IsNonNegative]
NDArrayI2NonPos = Annotated[NDArrayI, NDim2, IsNonPositive]
NDArrayI2NonZr = Annotated[NDArrayI, NDim2, IsNonZero]
NDArrayI2Pos = Annotated[NDArrayI, NDim2, IsPositive]
NDArrayI2Zr = Annotated[NDArrayI, NDim2, IsZero]

NDArrayI3Neg = Annotated[NDArrayI, NDim1, IsNegative]
NDArrayI3NonNeg = Annotated[NDArrayI, NDim3, IsNonNegative]
NDArrayI3NonPos = Annotated[NDArrayI, NDim3, IsNonPositive]
NDArrayI3NonZr = Annotated[NDArrayI, NDim3, IsNonZero]
NDArrayI3Pos = Annotated[NDArrayI, NDim3, IsPositive]
NDArrayI3Zr = Annotated[NDArrayI, NDim3, IsZero]


# annotations - float, ndim & predicate
NDArrayF0Fin = Annotated[NDArrayF, NDim0, IsFinite]
NDArrayF0FinInt = Annotated[NDArrayF, NDim0, IsFiniteAndIntegral]
NDArrayF0FinIntNan = Annotated[NDArrayF, NDim0, IsFiniteAndIntegralOrNan]
NDArrayF0FinNeg = Annotated[NDArrayF, NDim0, IsFiniteAndNegative]
NDArrayF0FinNegNan = Annotated[NDArrayF, NDim0, IsFiniteAndNegativeOrNan]
NDArrayF0FinNonNeg = Annotated[NDArrayF, NDim0, IsFiniteAndNonNegative]
NDArrayF0FinNonNegNan = Annotated[NDArrayF, NDim0, IsFiniteAndNonNegativeOrNan]
NDArrayF0FinNonPos = Annotated[NDArrayF, NDim0, IsFiniteAndNonPositive]
NDArrayF0FinNonPosNan = Annotated[NDArrayF, NDim0, IsFiniteAndNonPositiveOrNan]
NDArrayF0FinNonZr = Annotated[NDArrayF, NDim0, IsFiniteAndNonZero]
NDArrayF0FinNonZrNan = Annotated[NDArrayF, NDim0, IsFiniteAndNonZeroOrNan]
NDArrayF0FinPos = Annotated[NDArrayF, NDim0, IsFiniteAndPositive]
NDArrayF0FinPosNan = Annotated[NDArrayF, NDim0, IsFiniteAndPositiveOrNan]
NDArrayF0FinNan = Annotated[NDArrayF, NDim0, IsFiniteOrNan]
NDArrayF0Int = Annotated[NDArrayF, NDim0, IsIntegral]
NDArrayF0IntNan = Annotated[NDArrayF, NDim0, IsIntegralOrNan]
NDArrayF0Neg = Annotated[NDArrayF, NDim0, IsNegative]
NDArrayF0NegNan = Annotated[NDArrayF, NDim0, IsNegativeOrNan]
NDArrayF0NonNeg = Annotated[NDArrayF, NDim0, IsNonNegative]
NDArrayF0NonNegNan = Annotated[NDArrayF, NDim0, IsNonNegativeOrNan]
NDArrayF0NonPos = Annotated[NDArrayF, NDim0, IsNonPositive]
NDArrayF0NonPosNan = Annotated[NDArrayF, NDim0, IsNonPositiveOrNan]
NDArrayF0NonZr = Annotated[NDArrayF, NDim0, IsNonZero]
NDArrayF0NonZrNan = Annotated[NDArrayF, NDim0, IsNonZeroOrNan]
NDArrayF0Pos = Annotated[NDArrayF, NDim0, IsPositive]
NDArrayF0PosNan = Annotated[NDArrayF, NDim0, IsPositiveOrNan]
NDArrayF0Zr = Annotated[NDArrayF, NDim0, IsZero]
NDArrayF0ZrFinNonMic = Annotated[NDArrayF, NDim0, IsZeroOrFiniteAndNonMicro]
NDArrayF0ZrFinNonMicNan = Annotated[NDArrayF, NDim0, IsZeroOrFiniteAndNonMicroOrNan]
NDArrayF0ZrNan = Annotated[NDArrayF, NDim0, IsZeroOrNan]
NDArrayF0ZrNonMic = Annotated[NDArrayF, NDim0, IsZeroOrNonMicro]
NDArrayF0ZrNonMicNan = Annotated[NDArrayF, NDim0, IsZeroOrNonMicroOrNan]

NDArrayF1Fin = Annotated[NDArrayF, NDim1, IsFinite]
NDArrayF1FinInt = Annotated[NDArrayF, NDim1, IsFiniteAndIntegral]
NDArrayF1FinIntNan = Annotated[NDArrayF, NDim1, IsFiniteAndIntegralOrNan]
NDArrayF1FinNeg = Annotated[NDArrayF, NDim1, IsFiniteAndNegative]
NDArrayF1FinNegNan = Annotated[NDArrayF, NDim1, IsFiniteAndNegativeOrNan]
NDArrayF1FinNonNeg = Annotated[NDArrayF, NDim1, IsFiniteAndNonNegative]
NDArrayF1FinNonNegNan = Annotated[NDArrayF, NDim1, IsFiniteAndNonNegativeOrNan]
NDArrayF1FinNonPos = Annotated[NDArrayF, NDim1, IsFiniteAndNonPositive]
NDArrayF1FinNonPosNan = Annotated[NDArrayF, NDim1, IsFiniteAndNonPositiveOrNan]
NDArrayF1FinNonZr = Annotated[NDArrayF, NDim1, IsFiniteAndNonZero]
NDArrayF1FinNonZrNan = Annotated[NDArrayF, NDim1, IsFiniteAndNonZeroOrNan]
NDArrayF1FinPos = Annotated[NDArrayF, NDim1, IsFiniteAndPositive]
NDArrayF1FinPosNan = Annotated[NDArrayF, NDim1, IsFiniteAndPositiveOrNan]
NDArrayF1FinNan = Annotated[NDArrayF, NDim1, IsFiniteOrNan]
NDArrayF1Int = Annotated[NDArrayF, NDim1, IsIntegral]
NDArrayF1IntNan = Annotated[NDArrayF, NDim1, IsIntegralOrNan]
NDArrayF1Neg = Annotated[NDArrayF, NDim1, IsNegative]
NDArrayF1NegNan = Annotated[NDArrayF, NDim1, IsNegativeOrNan]
NDArrayF1NonNeg = Annotated[NDArrayF, NDim1, IsNonNegative]
NDArrayF1NonNegNan = Annotated[NDArrayF, NDim1, IsNonNegativeOrNan]
NDArrayF1NonPos = Annotated[NDArrayF, NDim1, IsNonPositive]
NDArrayF1NonPosNan = Annotated[NDArrayF, NDim1, IsNonPositiveOrNan]
NDArrayF1NonZr = Annotated[NDArrayF, NDim1, IsNonZero]
NDArrayF1NonZrNan = Annotated[NDArrayF, NDim1, IsNonZeroOrNan]
NDArrayF1Pos = Annotated[NDArrayF, NDim1, IsPositive]
NDArrayF1PosNan = Annotated[NDArrayF, NDim1, IsPositiveOrNan]
NDArrayF1Zr = Annotated[NDArrayF, NDim1, IsZero]
NDArrayF1ZrFinNonMic = Annotated[NDArrayF, NDim1, IsZeroOrFiniteAndNonMicro]
NDArrayF1ZrFinNonMicNan = Annotated[NDArrayF, NDim1, IsZeroOrFiniteAndNonMicroOrNan]
NDArrayF1ZrNan = Annotated[NDArrayF, NDim1, IsZeroOrNan]
NDArrayF1ZrNonMic = Annotated[NDArrayF, NDim1, IsZeroOrNonMicro]
NDArrayF1ZrNonMicNan = Annotated[NDArrayF, NDim1, IsZeroOrNonMicroOrNan]

NDArrayF2Fin = Annotated[NDArrayF, NDim2, IsFinite]
NDArrayF2FinInt = Annotated[NDArrayF, NDim2, IsFiniteAndIntegral]
NDArrayF2FinIntNan = Annotated[NDArrayF, NDim2, IsFiniteAndIntegralOrNan]
NDArrayF2FinNeg = Annotated[NDArrayF, NDim2, IsFiniteAndNegative]
NDArrayF2FinNegNan = Annotated[NDArrayF, NDim2, IsFiniteAndNegativeOrNan]
NDArrayF2FinNonNeg = Annotated[NDArrayF, NDim2, IsFiniteAndNonNegative]
NDArrayF2FinNonNegNan = Annotated[NDArrayF, NDim2, IsFiniteAndNonNegativeOrNan]
NDArrayF2FinNonPos = Annotated[NDArrayF, NDim2, IsFiniteAndNonPositive]
NDArrayF2FinNonPosNan = Annotated[NDArrayF, NDim2, IsFiniteAndNonPositiveOrNan]
NDArrayF2FinNonZr = Annotated[NDArrayF, NDim2, IsFiniteAndNonZero]
NDArrayF2FinNonZrNan = Annotated[NDArrayF, NDim2, IsFiniteAndNonZeroOrNan]
NDArrayF2FinPos = Annotated[NDArrayF, NDim2, IsFiniteAndPositive]
NDArrayF2FinPosNan = Annotated[NDArrayF, NDim2, IsFiniteAndPositiveOrNan]
NDArrayF2FinNan = Annotated[NDArrayF, NDim2, IsFiniteOrNan]
NDArrayF2Int = Annotated[NDArrayF, NDim2, IsIntegral]
NDArrayF2IntNan = Annotated[NDArrayF, NDim2, IsIntegralOrNan]
NDArrayF2Neg = Annotated[NDArrayF, NDim2, IsNegative]
NDArrayF2NegNan = Annotated[NDArrayF, NDim2, IsNegativeOrNan]
NDArrayF2NonNeg = Annotated[NDArrayF, NDim2, IsNonNegative]
NDArrayF2NonNegNan = Annotated[NDArrayF, NDim2, IsNonNegativeOrNan]
NDArrayF2NonPos = Annotated[NDArrayF, NDim2, IsNonPositive]
NDArrayF2NonPosNan = Annotated[NDArrayF, NDim2, IsNonPositiveOrNan]
NDArrayF2NonZr = Annotated[NDArrayF, NDim2, IsNonZero]
NDArrayF2NonZrNan = Annotated[NDArrayF, NDim2, IsNonZeroOrNan]
NDArrayF2Pos = Annotated[NDArrayF, NDim2, IsPositive]
NDArrayF2PosNan = Annotated[NDArrayF, NDim2, IsPositiveOrNan]
NDArrayF2Zr = Annotated[NDArrayF, NDim2, IsZero]
NDArrayF2ZrFinNonMic = Annotated[NDArrayF, NDim2, IsZeroOrFiniteAndNonMicro]
NDArrayF2ZrFinNonMicNan = Annotated[NDArrayF, NDim2, IsZeroOrFiniteAndNonMicroOrNan]
NDArrayF2ZrNan = Annotated[NDArrayF, NDim2, IsZeroOrNan]
NDArrayF2ZrNonMic = Annotated[NDArrayF, NDim2, IsZeroOrNonMicro]
NDArrayF2ZrNonMicNan = Annotated[NDArrayF, NDim2, IsZeroOrNonMicroOrNan]

NDArrayF3Fin = Annotated[NDArrayF, NDim3, IsFinite]
NDArrayF3FinInt = Annotated[NDArrayF, NDim3, IsFiniteAndIntegral]
NDArrayF3FinIntNan = Annotated[NDArrayF, NDim3, IsFiniteAndIntegralOrNan]
NDArrayF3FinNeg = Annotated[NDArrayF, NDim3, IsFiniteAndNegative]
NDArrayF3FinNegNan = Annotated[NDArrayF, NDim3, IsFiniteAndNegativeOrNan]
NDArrayF3FinNonNeg = Annotated[NDArrayF, NDim3, IsFiniteAndNonNegative]
NDArrayF3FinNonNegNan = Annotated[NDArrayF, NDim3, IsFiniteAndNonNegativeOrNan]
NDArrayF3FinNonPos = Annotated[NDArrayF, NDim3, IsFiniteAndNonPositive]
NDArrayF3FinNonPosNan = Annotated[NDArrayF, NDim3, IsFiniteAndNonPositiveOrNan]
NDArrayF3FinNonZr = Annotated[NDArrayF, NDim3, IsFiniteAndNonZero]
NDArrayF3FinNonZrNan = Annotated[NDArrayF, NDim3, IsFiniteAndNonZeroOrNan]
NDArrayF3FinPos = Annotated[NDArrayF, NDim3, IsFiniteAndPositive]
NDArrayF3FinPosNan = Annotated[NDArrayF, NDim3, IsFiniteAndPositiveOrNan]
NDArrayF3FinNan = Annotated[NDArrayF, NDim3, IsFiniteOrNan]
NDArrayF3Int = Annotated[NDArrayF, NDim3, IsIntegral]
NDArrayF3IntNan = Annotated[NDArrayF, NDim3, IsIntegralOrNan]
NDArrayF3Neg = Annotated[NDArrayF, NDim3, IsNegative]
NDArrayF3NegNan = Annotated[NDArrayF, NDim3, IsNegativeOrNan]
NDArrayF3NonNeg = Annotated[NDArrayF, NDim3, IsNonNegative]
NDArrayF3NonNegNan = Annotated[NDArrayF, NDim3, IsNonNegativeOrNan]
NDArrayF3NonPos = Annotated[NDArrayF, NDim3, IsNonPositive]
NDArrayF3NonPosNan = Annotated[NDArrayF, NDim3, IsNonPositiveOrNan]
NDArrayF3NonZr = Annotated[NDArrayF, NDim3, IsNonZero]
NDArrayF3NonZrNan = Annotated[NDArrayF, NDim3, IsNonZeroOrNan]
NDArrayF3Pos = Annotated[NDArrayF, NDim3, IsPositive]
NDArrayF3PosNan = Annotated[NDArrayF, NDim3, IsPositiveOrNan]
NDArrayF3Zr = Annotated[NDArrayF, NDim3, IsZero]
NDArrayF3ZrFinNonMic = Annotated[NDArrayF, NDim3, IsZeroOrFiniteAndNonMicro]
NDArrayF3ZrFinNonMicNan = Annotated[NDArrayF, NDim3, IsZeroOrFiniteAndNonMicroOrNan]
NDArrayF3ZrNan = Annotated[NDArrayF, NDim3, IsZeroOrNan]
NDArrayF3ZrNonMic = Annotated[NDArrayF, NDim3, IsZeroOrNonMicro]
NDArrayF3ZrNonMicNan = Annotated[NDArrayF, NDim3, IsZeroOrNonMicroOrNan]


__all__ = [
    "array_indexer",
    "as_int",
    "DATE_MAX_AS_DATETIME64",
    "DATE_MAX_AS_INT",
    "DATE_MIN_AS_DATETIME64",
    "DATE_MIN_AS_INT",
    "date_to_datetime64",
    "DateOverflowError",
    "DATETIME_MAX_AS_DATETIME64",
    "DATETIME_MAX_AS_INT",
    "DATETIME_MIN_AS_DATETIME64",
    "DATETIME_MIN_AS_INT",
    "datetime_to_datetime64",
    "datetime64_dtype_to_unit",
    "datetime64_to_date",
    "datetime64_to_datetime",
    "datetime64_to_int",
    "datetime64_unit_to_dtype",
    "datetime64_unit_to_kind",
    "datetime64as",
    "datetime64D",
    "datetime64fs",
    "datetime64h",
    "Datetime64Kind",
    "datetime64m",
    "datetime64M",
    "datetime64ms",
    "datetime64ns",
    "datetime64ps",
    "datetime64s",
    "Datetime64Unit",
    "datetime64us",
    "datetime64W",
    "datetime64Y",
    "DEFAULT_RNG",
    "discretize",
    "DTypeB",
    "DTypeDns",
    "DTypeF",
    "DTypeI",
    "DTypeO",
    "EmptyNumpyConcatenateError",
    "ffill_non_nan_slices",
    "fillna",
    "flatn0",
    "get_fill_value",
    "has_dtype",
    "InfElementsError",
    "InvalidDTypeError",
    "is_at_least_or_nan",
    "is_at_least",
    "is_at_most_or_nan",
    "is_at_most",
    "is_between_or_nan",
    "is_between",
    "is_empty",
    "is_finite_and_integral_or_nan",
    "is_finite_and_integral",
    "is_finite_and_negative_or_nan",
    "is_finite_and_negative",
    "is_finite_and_non_negative_or_nan",
    "is_finite_and_non_negative",
    "is_finite_and_non_positive_or_nan",
    "is_finite_and_non_positive",
    "is_finite_and_non_zero_or_nan",
    "is_finite_and_non_zero",
    "is_finite_and_positive_or_nan",
    "is_finite_and_positive",
    "is_finite_or_nan",
    "is_greater_than_or_nan",
    "is_greater_than",
    "is_integral_or_nan",
    "is_integral",
    "is_less_than_or_nan",
    "is_less_than",
    "is_negative_or_nan",
    "is_negative",
    "is_non_empty",
    "is_non_negative_or_nan",
    "is_non_negative",
    "is_non_positive_or_nan",
    "is_non_positive",
    "is_non_singular",
    "is_non_zero_or_nan",
    "is_non_zero",
    "is_positive_or_nan",
    "is_positive_semidefinite",
    "is_positive",
    "is_symmetric",
    "is_zero_or_finite_and_non_micro_or_nan",
    "is_zero_or_finite_and_non_micro",
    "is_zero_or_nan",
    "is_zero_or_non_micro_or_nan",
    "is_zero_or_non_micro",
    "is_zero",
    "IsFinite",
    "IsFiniteAndIntegral",
    "IsFiniteAndIntegralOrNan",
    "IsFiniteAndNegative",
    "IsFiniteAndNegativeOrNan",
    "IsFiniteAndNonNegative",
    "IsFiniteAndNonNegativeOrNan",
    "IsFiniteAndNonPositive",
    "IsFiniteAndNonPositiveOrNan",
    "IsFiniteAndNonZero",
    "IsFiniteAndNonZeroOrNan",
    "IsFiniteAndPositive",
    "IsFiniteAndPositiveOrNan",
    "IsFiniteOrNan",
    "IsIntegral",
    "IsIntegralOrNan",
    "IsNegative",
    "IsNegativeOrNan",
    "IsNonNegative",
    "IsNonNegativeOrNan",
    "IsNonPositive",
    "IsNonPositiveOrNan",
    "IsNonZero",
    "IsNonZeroOrNan",
    "IsPositive",
    "IsPositiveOrNan",
    "IsZero",
    "IsZeroOrFiniteAndNonMicro",
    "IsZeroOrFiniteAndNonMicroOrNan",
    "IsZeroOrNan",
    "IsZeroOrNonMicro",
    "IsZeroOrNonMicroOrNan",
    "LossOfNanosecondsError",
    "maximum",
    "minimum",
    "MultipleTrueElementsError",
    "NanElementsError",
    "NDArray0",
    "NDArray1",
    "NDArray2",
    "NDArray3",
    "NDArrayA",
    "NDArrayB",
    "NDArrayB0",
    "NDArrayB1",
    "NDArrayB2",
    "NDArrayB3",
    "NDArrayD",
    "NDArrayD0",
    "NDArrayD1",
    "NDArrayD2",
    "NDArrayD3",
    "NDArrayDas",
    "NDArrayDas0",
    "NDArrayDas1",
    "NDArrayDas2",
    "NDArrayDas3",
    "NDArrayDD",
    "NDArrayDD0",
    "NDArrayDD1",
    "NDArrayDD2",
    "NDArrayDD3",
    "NDArrayDfs",
    "NDArrayDfs0",
    "NDArrayDfs1",
    "NDArrayDfs2",
    "NDArrayDfs3",
    "NDArrayDh",
    "NDArrayDh0",
    "NDArrayDh1",
    "NDArrayDh2",
    "NDArrayDh3",
    "NDArrayDm",
    "NDArrayDM",
    "NDArrayDm0",
    "NDArrayDM0",
    "NDArrayDm1",
    "NDArrayDM1",
    "NDArrayDm2",
    "NDArrayDM2",
    "NDArrayDm3",
    "NDArrayDM3",
    "NDArrayDms",
    "NDArrayDms0",
    "NDArrayDms1",
    "NDArrayDms2",
    "NDArrayDms3",
    "NDArrayDns",
    "NDArrayDns0",
    "NDArrayDns1",
    "NDArrayDns2",
    "NDArrayDns3",
    "NDArrayDps",
    "NDArrayDps0",
    "NDArrayDps1",
    "NDArrayDps2",
    "NDArrayDps3",
    "NDArrayDs",
    "NDArrayDs0",
    "NDArrayDs1",
    "NDArrayDs2",
    "NDArrayDs3",
    "NDArrayDus",
    "NDArrayDus0",
    "NDArrayDus1",
    "NDArrayDus2",
    "NDArrayDus3",
    "NDArrayDW",
    "NDArrayDW0",
    "NDArrayDW1",
    "NDArrayDW2",
    "NDArrayDW3",
    "NDArrayDY",
    "NDArrayDY0",
    "NDArrayDY1",
    "NDArrayDY2",
    "NDArrayDY3",
    "NDArrayF",
    "NDArrayF0",
    "NDArrayF0Fin",
    "NDArrayF0FinInt",
    "NDArrayF0FinIntNan",
    "NDArrayF0FinNan",
    "NDArrayF0FinNeg",
    "NDArrayF0FinNegNan",
    "NDArrayF0FinNonNeg",
    "NDArrayF0FinNonNegNan",
    "NDArrayF0FinNonPos",
    "NDArrayF0FinNonPosNan",
    "NDArrayF0FinNonZr",
    "NDArrayF0FinNonZrNan",
    "NDArrayF0FinPos",
    "NDArrayF0FinPosNan",
    "NDArrayF0Int",
    "NDArrayF0IntNan",
    "NDArrayF0Neg",
    "NDArrayF0NegNan",
    "NDArrayF0NonNeg",
    "NDArrayF0NonNegNan",
    "NDArrayF0NonPos",
    "NDArrayF0NonPosNan",
    "NDArrayF0NonZr",
    "NDArrayF0NonZrNan",
    "NDArrayF0Pos",
    "NDArrayF0PosNan",
    "NDArrayF0Zr",
    "NDArrayF0ZrFinNonMic",
    "NDArrayF0ZrFinNonMicNan",
    "NDArrayF0ZrNan",
    "NDArrayF0ZrNonMic",
    "NDArrayF0ZrNonMicNan",
    "NDArrayF1",
    "NDArrayF1Fin",
    "NDArrayF1FinInt",
    "NDArrayF1FinIntNan",
    "NDArrayF1FinNan",
    "NDArrayF1FinNeg",
    "NDArrayF1FinNegNan",
    "NDArrayF1FinNonNeg",
    "NDArrayF1FinNonNegNan",
    "NDArrayF1FinNonPos",
    "NDArrayF1FinNonPosNan",
    "NDArrayF1FinNonZr",
    "NDArrayF1FinNonZrNan",
    "NDArrayF1FinPos",
    "NDArrayF1FinPosNan",
    "NDArrayF1Int",
    "NDArrayF1IntNan",
    "NDArrayF1Neg",
    "NDArrayF1NegNan",
    "NDArrayF1NonNeg",
    "NDArrayF1NonNegNan",
    "NDArrayF1NonPos",
    "NDArrayF1NonPosNan",
    "NDArrayF1NonZr",
    "NDArrayF1NonZrNan",
    "NDArrayF1Pos",
    "NDArrayF1PosNan",
    "NDArrayF1Zr",
    "NDArrayF1ZrFinNonMic",
    "NDArrayF1ZrFinNonMicNan",
    "NDArrayF1ZrNan",
    "NDArrayF1ZrNonMic",
    "NDArrayF1ZrNonMicNan",
    "NDArrayF2",
    "NDArrayF2Fin",
    "NDArrayF2FinInt",
    "NDArrayF2FinIntNan",
    "NDArrayF2FinNan",
    "NDArrayF2FinNeg",
    "NDArrayF2FinNegNan",
    "NDArrayF2FinNonNeg",
    "NDArrayF2FinNonNegNan",
    "NDArrayF2FinNonPos",
    "NDArrayF2FinNonPosNan",
    "NDArrayF2FinNonZr",
    "NDArrayF2FinNonZrNan",
    "NDArrayF2FinPos",
    "NDArrayF2FinPosNan",
    "NDArrayF2Int",
    "NDArrayF2IntNan",
    "NDArrayF2Neg",
    "NDArrayF2NegNan",
    "NDArrayF2NonNeg",
    "NDArrayF2NonNegNan",
    "NDArrayF2NonPos",
    "NDArrayF2NonPosNan",
    "NDArrayF2NonZr",
    "NDArrayF2NonZrNan",
    "NDArrayF2Pos",
    "NDArrayF2PosNan",
    "NDArrayF2Zr",
    "NDArrayF2ZrFinNonMic",
    "NDArrayF2ZrFinNonMicNan",
    "NDArrayF2ZrNan",
    "NDArrayF2ZrNonMic",
    "NDArrayF2ZrNonMicNan",
    "NDArrayF3",
    "NDArrayF3Fin",
    "NDArrayF3FinInt",
    "NDArrayF3FinIntNan",
    "NDArrayF3FinNan",
    "NDArrayF3FinNeg",
    "NDArrayF3FinNegNan",
    "NDArrayF3FinNonNeg",
    "NDArrayF3FinNonNegNan",
    "NDArrayF3FinNonPos",
    "NDArrayF3FinNonPosNan",
    "NDArrayF3FinNonZr",
    "NDArrayF3FinNonZrNan",
    "NDArrayF3FinPos",
    "NDArrayF3FinPosNan",
    "NDArrayF3Int",
    "NDArrayF3IntNan",
    "NDArrayF3Neg",
    "NDArrayF3NegNan",
    "NDArrayF3NonNeg",
    "NDArrayF3NonNegNan",
    "NDArrayF3NonPos",
    "NDArrayF3NonPosNan",
    "NDArrayF3NonZr",
    "NDArrayF3NonZrNan",
    "NDArrayF3Pos",
    "NDArrayF3PosNan",
    "NDArrayF3Zr",
    "NDArrayF3ZrFinNonMic",
    "NDArrayF3ZrFinNonMicNan",
    "NDArrayF3ZrNan",
    "NDArrayF3ZrNonMic",
    "NDArrayF3ZrNonMicNan",
    "NDArrayFFin",
    "NDArrayFFinInt",
    "NDArrayFFinIntNan",
    "NDArrayFFinNan",
    "NDArrayFFinNeg",
    "NDArrayFFinNegNan",
    "NDArrayFFinNonNeg",
    "NDArrayFFinNonNegNan",
    "NDArrayFFinNonPos",
    "NDArrayFFinNonPosNan",
    "NDArrayFFinNonZr",
    "NDArrayFFinNonZrNan",
    "NDArrayFFinPos",
    "NDArrayFFinPosNan",
    "NDArrayFInt",
    "NDArrayFIntNan",
    "NDArrayFNeg",
    "NDArrayFNegNan",
    "NDArrayFNonNeg",
    "NDArrayFNonNegNan",
    "NDArrayFNonPos",
    "NDArrayFNonPosNan",
    "NDArrayFNonZr",
    "NDArrayFNonZrNan",
    "NDArrayFPos",
    "NDArrayFPosNan",
    "NDArrayFZr",
    "NDArrayFZrFinNonMic",
    "NDArrayFZrFinNonMicNan",
    "NDArrayFZrNan",
    "NDArrayFZrNonMic",
    "NDArrayFZrNonMicNan",
    "NDArrayI",
    "NDArrayI0",
    "NDArrayI0Neg",
    "NDArrayI0NonNeg",
    "NDArrayI0NonPos",
    "NDArrayI0NonZr",
    "NDArrayI0Pos",
    "NDArrayI0Zr",
    "NDArrayI1",
    "NDArrayI1Neg",
    "NDArrayI1NonNeg",
    "NDArrayI1NonPos",
    "NDArrayI1NonZr",
    "NDArrayI1Pos",
    "NDArrayI1Zr",
    "NDArrayI2",
    "NDArrayI2Neg",
    "NDArrayI2NonNeg",
    "NDArrayI2NonPos",
    "NDArrayI2NonZr",
    "NDArrayI2Pos",
    "NDArrayI2Zr",
    "NDArrayI3",
    "NDArrayI3Neg",
    "NDArrayI3NonNeg",
    "NDArrayI3NonPos",
    "NDArrayI3NonZr",
    "NDArrayI3Pos",
    "NDArrayI3Zr",
    "NDArrayINeg",
    "NDArrayINonNeg",
    "NDArrayINonPos",
    "NDArrayINonZr",
    "NDArrayIPos",
    "NDArrayIZr",
    "NDArrayO",
    "NDArrayO0",
    "NDArrayO1",
    "NDArrayO2",
    "NDArrayO3",
    "NDim0",
    "NDim1",
    "NDim2",
    "NDim3",
    "NonIntegralElementsError",
    "NoTrueElementsError",
    "redirect_to_empty_numpy_concatenate_error",
    "shift_bool",
    "shift",
    "year",
    "ZeroShiftError",
]
