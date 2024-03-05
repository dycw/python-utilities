from __future__ import annotations

import datetime as dt
from contextlib import contextmanager
from dataclasses import dataclass
from functools import reduce
from itertools import repeat
from typing import TYPE_CHECKING, Annotated, Any, Literal, cast, overload

import numpy as np
from numpy import (
    array,
    bool_,
    datetime64,
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
    ndarray,
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

from utilities.datetime import EPOCH_UTC, UTC
from utilities.errors import redirect_error
from utilities.iterables import is_iterable_not_str
from utilities.re import extract_group

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from utilities.math import FloatFinPos

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
NDArrayDY = Annotated[NDArrayA, datetime64Y]
NDArrayDM = Annotated[NDArrayA, datetime64M]
NDArrayDW = Annotated[NDArrayA, datetime64W]
NDArrayDD = Annotated[NDArrayA, datetime64D]
NDArrayDh = Annotated[NDArrayA, datetime64h]
NDArrayDm = Annotated[NDArrayA, datetime64m]
NDArrayDs = Annotated[NDArrayA, datetime64s]
NDArrayDms = Annotated[NDArrayA, datetime64ms]
NDArrayDus = Annotated[NDArrayA, datetime64us]
NDArrayDns = Annotated[NDArrayA, datetime64ns]
NDArrayDps = Annotated[NDArrayA, datetime64ps]
NDArrayDfs = Annotated[NDArrayA, datetime64fs]
NDArrayDas = Annotated[NDArrayA, datetime64as]
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


# annotations - ndims
NDArray0 = Annotated[NDArrayA, 0]
NDArray1 = Annotated[NDArrayA, 1]
NDArray2 = Annotated[NDArrayA, 2]
NDArray3 = Annotated[NDArrayA, 3]


# annotations - dtype & ndim
NDArrayB0 = Annotated[NDArrayB, 0]
NDArrayD0 = Annotated[NDArrayD, 0]
NDArrayF0 = Annotated[NDArrayF, 0]
NDArrayI0 = Annotated[NDArrayI, 0]
NDArrayO0 = Annotated[NDArrayO, 0]
NDArrayDY0 = Annotated[NDArrayDY, 0]
NDArrayDM0 = Annotated[NDArrayDM, 0]
NDArrayDW0 = Annotated[NDArrayDW, 0]
NDArrayDD0 = Annotated[NDArrayDD, 0]
NDArrayDh0 = Annotated[NDArrayDh, 0]
NDArrayDm0 = Annotated[NDArrayDm, 0]
NDArrayDs0 = Annotated[NDArrayDs, 0]
NDArrayDms0 = Annotated[NDArrayDms, 0]
NDArrayDus0 = Annotated[NDArrayDus, 0]
NDArrayDns0 = Annotated[NDArrayDns, 0]
NDArrayDps0 = Annotated[NDArrayDps, 0]
NDArrayDfs0 = Annotated[NDArrayDfs, 0]
NDArrayDas0 = Annotated[NDArrayDas, 0]

NDArrayB1 = Annotated[NDArrayB, 1]
NDArrayD1 = Annotated[NDArrayD, 1]
NDArrayF1 = Annotated[NDArrayF, 1]
NDArrayI1 = Annotated[NDArrayI, 1]
NDArrayO1 = Annotated[NDArrayO, 1]
NDArrayDY1 = Annotated[NDArrayDY, 1]
NDArrayDM1 = Annotated[NDArrayDM, 1]
NDArrayDW1 = Annotated[NDArrayDW, 1]
NDArrayDD1 = Annotated[NDArrayDD, 1]
NDArrayDh1 = Annotated[NDArrayDh, 1]
NDArrayDm1 = Annotated[NDArrayDm, 1]
NDArrayDs1 = Annotated[NDArrayDs, 1]
NDArrayDms1 = Annotated[NDArrayDms, 1]
NDArrayDus1 = Annotated[NDArrayDus, 1]
NDArrayDns1 = Annotated[NDArrayDns, 1]
NDArrayDps1 = Annotated[NDArrayDps, 1]
NDArrayDfs1 = Annotated[NDArrayDfs, 1]
NDArrayDas1 = Annotated[NDArrayDas, 1]

NDArrayB2 = Annotated[NDArrayB, 2]
NDArrayD2 = Annotated[NDArrayD, 2]
NDArrayF2 = Annotated[NDArrayF, 2]
NDArrayI2 = Annotated[NDArrayI, 2]
NDArrayO2 = Annotated[NDArrayO, 2]
NDArrayDY2 = Annotated[NDArrayDY, 2]
NDArrayDM2 = Annotated[NDArrayDM, 2]
NDArrayDW2 = Annotated[NDArrayDW, 2]
NDArrayDD2 = Annotated[NDArrayDD, 2]
NDArrayDh2 = Annotated[NDArrayDh, 2]
NDArrayDm2 = Annotated[NDArrayDm, 2]
NDArrayDs2 = Annotated[NDArrayDs, 2]
NDArrayDms2 = Annotated[NDArrayDms, 2]
NDArrayDus2 = Annotated[NDArrayDus, 2]
NDArrayDns2 = Annotated[NDArrayDns, 2]
NDArrayDps2 = Annotated[NDArrayDps, 2]
NDArrayDfs2 = Annotated[NDArrayDfs, 2]
NDArrayDas2 = Annotated[NDArrayDas, 2]

NDArrayB3 = Annotated[NDArrayB, 3]
NDArrayD3 = Annotated[NDArrayD, 3]
NDArrayF3 = Annotated[NDArrayF, 3]
NDArrayI3 = Annotated[NDArrayI, 3]
NDArrayO3 = Annotated[NDArrayO, 3]
NDArrayDY3 = Annotated[NDArrayDY, 3]
NDArrayDM3 = Annotated[NDArrayDM, 3]
NDArrayDW3 = Annotated[NDArrayDW, 3]
NDArrayDD3 = Annotated[NDArrayDD, 3]
NDArrayDh3 = Annotated[NDArrayDh, 3]
NDArrayDm3 = Annotated[NDArrayDm, 3]
NDArrayDs3 = Annotated[NDArrayDs, 3]
NDArrayDms3 = Annotated[NDArrayDms, 3]
NDArrayDus3 = Annotated[NDArrayDus, 3]
NDArrayDns3 = Annotated[NDArrayDns, 3]
NDArrayDps3 = Annotated[NDArrayDps, 3]
NDArrayDfs3 = Annotated[NDArrayDfs, 3]
NDArrayDas3 = Annotated[NDArrayDas, 3]


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


def date_to_datetime64(date: dt.date, /) -> datetime64:
    """Convert a `dt.date` to `numpy.datetime64`."""

    return datetime64(date, "D")


DATE_MIN_AS_DATETIME64 = date_to_datetime64(dt.date.min)
DATE_MAX_AS_DATETIME64 = date_to_datetime64(dt.date.max)


def datetime_to_datetime64(datetime: dt.datetime, /) -> datetime64:
    """Convert a `dt.datetime` to `numpy.datetime64`."""

    if (tz := datetime.tzinfo) is None:
        datetime_use = datetime
    elif tz is UTC:
        datetime_use = datetime.replace(tzinfo=None)
    else:
        raise DatetimeToDatetime64Error(datetime=datetime, tzinfo=tz)
    return datetime64(datetime_use, "us")


@dataclass(kw_only=True)
class DatetimeToDatetime64Error(Exception):
    datetime: dt.datetime
    tzinfo: dt.tzinfo

    @override
    def __str__(self) -> str:
        return (  # pragma: no cover
            f"Timezone must be None or UTC; got {self.tzinfo}."
        )


DATETIME_MIN_AS_DATETIME64 = datetime_to_datetime64(dt.datetime.min)
DATETIME_MAX_AS_DATETIME64 = datetime_to_datetime64(dt.datetime.max)


def datetime64_to_date(datetime: datetime64, /) -> dt.date:
    """Convert a `numpy.datetime64` to a `dt.date`."""

    as_int = datetime64_to_int(datetime)
    if (dtype := datetime.dtype) == datetime64D:
        with redirect_error(
            OverflowError, DateTime64ToDateError(f"{datetime=}, {dtype=}")
        ):
            return (EPOCH_UTC + dt.timedelta(days=as_int)).date()
    msg = f"{datetime=}, {dtype=}"
    raise NotImplementedError(msg)


class DateTime64ToDateError(Exception): ...


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
        with redirect_error(
            OverflowError, DateTime64ToDateTimeError(f"{datetime=}, {dtype=}")
        ):
            return EPOCH_UTC + dt.timedelta(milliseconds=as_int)
    if dtype == datetime64us:
        return EPOCH_UTC + dt.timedelta(microseconds=as_int)
    if dtype == datetime64ns:
        microseconds, nanoseconds = divmod(as_int, int(1e3))
        if nanoseconds != 0:
            msg = f"{datetime=}, {nanoseconds=}"
            raise DateTime64ToDateTimeError(msg)
        return EPOCH_UTC + dt.timedelta(microseconds=microseconds)
    msg = f"{datetime=}, {dtype=}"
    raise NotImplementedError(msg)


class DateTime64ToDateTimeError(Exception): ...


def datetime64_dtype_to_unit(dtype: Any, /) -> Datetime64Unit:
    """Convert a `datetime64` dtype to a unit."""
    return cast(Datetime64Unit, extract_group(r"^<M8\[(\w+)\]$", dtype.str))


def datetime64_unit_to_dtype(unit: Datetime64Unit, /) -> Any:
    """Convert a `datetime64` unit to a dtype."""
    return dtype(f"datetime64[{unit}]")


def datetime64_unit_to_kind(unit: Datetime64Unit, /) -> Datetime64Kind:
    """Convert a `datetime64` unit to a kind."""
    return "date" if unit in {"Y", "M", "W", "D"} else "time"


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


def ewma(array: NDArrayF, halflife: FloatFinPos, /, *, axis: int = -1) -> NDArrayF:
    """Compute the EWMA of an array."""
    from numbagg import move_exp_nanmean

    alpha = _exp_weighted_alpha(halflife)
    return cast(Any, move_exp_nanmean)(array, axis=axis, alpha=alpha)


def exp_moving_sum(
    array: NDArrayF, halflife: FloatFinPos, /, *, axis: int = -1
) -> NDArrayF:
    """Compute the exponentially-weighted moving sum of an array."""
    from numbagg import move_exp_nansum

    alpha = _exp_weighted_alpha(halflife)
    return cast(Any, move_exp_nansum)(array, axis=axis, alpha=alpha)


def _exp_weighted_alpha(halflife: FloatFinPos, /) -> float:
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
        raise FlatN0Error(msg)
    flattened = flatnonzero(array)
    with redirect_error(
        ValueError,
        FlatN0Error(f"{array=}"),
        match="can only convert an array of size 1 to a Python scalar",
    ):
        return flattened.item()


class FlatN0Error(Exception): ...


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
    raise GetFillValueError(msg)


class GetFillValueError(Exception): ...


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


@contextmanager
def redirect_empty_numpy_concatenate() -> Iterator[None]:
    """Redirect to the `EmptyNumpyConcatenateError`."""
    with redirect_error(
        ValueError,
        EmptyNumpyConcatenateError,
        match="need at least one array to concatenate",
    ):
        yield


class EmptyNumpyConcatenateError(Exception): ...


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


@overload
def year(date: datetime64, /) -> int: ...


@overload
def year(date: NDArrayDD, /) -> NDArrayI: ...


def year(date: datetime64 | NDArrayDD, /) -> int | NDArrayI:
    """Convert a date/array of dates into a year/array of years."""
    years = 1970 + date.astype(datetime64Y).astype(int)
    return years if isinstance(date, ndarray) else years.item()


# annotations - int & predicates
NDArrayINeg = Annotated[NDArrayI, is_negative]
NDArrayINonNeg = Annotated[NDArrayI, is_non_negative]
NDArrayINonPos = Annotated[NDArrayI, is_non_positive]
NDArrayINonZr = Annotated[NDArrayI, is_non_zero]
NDArrayIPos = Annotated[NDArrayI, is_positive]
NDArrayIZr = Annotated[NDArrayI, is_zero]


# annotations - float & predicates
NDArrayFFin = Annotated[NDArrayF, isfinite]
NDArrayFFinInt = Annotated[NDArrayF, is_finite_and_integral]
NDArrayFFinIntNan = Annotated[NDArrayF, is_finite_and_integral_or_nan]
NDArrayFFinNeg = Annotated[NDArrayF, is_finite_and_negative]
NDArrayFFinNegNan = Annotated[NDArrayF, is_finite_and_negative_or_nan]
NDArrayFFinNonNeg = Annotated[NDArrayF, is_finite_and_non_negative]
NDArrayFFinNonNegNan = Annotated[NDArrayF, is_finite_and_non_negative_or_nan]
NDArrayFFinNonPos = Annotated[NDArrayF, is_finite_and_non_positive]
NDArrayFFinNonPosNan = Annotated[NDArrayF, is_finite_and_non_positive_or_nan]
NDArrayFFinNonZr = Annotated[NDArrayF, is_finite_and_non_zero]
NDArrayFFinNonZrNan = Annotated[NDArrayF, is_finite_and_non_zero_or_nan]
NDArrayFFinPos = Annotated[NDArrayF, is_finite_and_positive]
NDArrayFFinPosNan = Annotated[NDArrayF, is_finite_and_positive_or_nan]
NDArrayFFinNan = Annotated[NDArrayF, is_finite_or_nan]
NDArrayFInt = Annotated[NDArrayF, is_integral]
NDArrayFIntNan = Annotated[NDArrayF, is_integral_or_nan]
NDArrayFNeg = Annotated[NDArrayF, is_negative]
NDArrayFNegNan = Annotated[NDArrayF, is_negative_or_nan]
NDArrayFNonNeg = Annotated[NDArrayF, is_non_negative]
NDArrayFNonNegNan = Annotated[NDArrayF, is_non_negative_or_nan]
NDArrayFNonPos = Annotated[NDArrayF, is_non_positive]
NDArrayFNonPosNan = Annotated[NDArrayF, is_non_positive_or_nan]
NDArrayFNonZr = Annotated[NDArrayF, is_non_zero]
NDArrayFNonZrNan = Annotated[NDArrayF, is_non_zero_or_nan]
NDArrayFPos = Annotated[NDArrayF, is_positive]
NDArrayFPosNan = Annotated[NDArrayF, is_positive_or_nan]
NDArrayFZr = Annotated[NDArrayF, is_zero]
NDArrayFZrFinNonMic = Annotated[NDArrayF, is_zero_or_finite_and_non_micro]
NDArrayFZrFinNonMicNan = Annotated[NDArrayF, is_zero_or_finite_and_non_micro_or_nan]
NDArrayFZrNan = Annotated[NDArrayF, is_zero_or_nan]
NDArrayFZrNonMic = Annotated[NDArrayF, is_zero_or_non_micro]
NDArrayFZrNonMicNan = Annotated[NDArrayF, is_zero_or_non_micro_or_nan]


# annotations - int, ndim & predicate
NDArrayI0Neg = Annotated[NDArrayI0, is_negative]
NDArrayI0NonNeg = Annotated[NDArrayI0, is_non_negative]
NDArrayI0NonPos = Annotated[NDArrayI0, is_non_positive]
NDArrayI0NonZr = Annotated[NDArrayI0, is_non_zero]
NDArrayI0Pos = Annotated[NDArrayI0, is_positive]
NDArrayI0Zr = Annotated[NDArrayI0, is_zero]

NDArrayI1Neg = Annotated[NDArrayI1, is_negative]
NDArrayI1NonNeg = Annotated[NDArrayI1, is_non_negative]
NDArrayI1NonPos = Annotated[NDArrayI1, is_non_positive]
NDArrayI1NonZr = Annotated[NDArrayI1, is_non_zero]
NDArrayI1Pos = Annotated[NDArrayI1, is_positive]
NDArrayI1Zr = Annotated[NDArrayI1, is_zero]

NDArrayI2Neg = Annotated[NDArrayI2, is_negative]
NDArrayI2NonNeg = Annotated[NDArrayI2, is_non_negative]
NDArrayI2NonPos = Annotated[NDArrayI2, is_non_positive]
NDArrayI2NonZr = Annotated[NDArrayI2, is_non_zero]
NDArrayI2Pos = Annotated[NDArrayI2, is_positive]
NDArrayI2Zr = Annotated[NDArrayI2, is_zero]

NDArrayI3Neg = Annotated[NDArrayI1, is_negative]
NDArrayI3NonNeg = Annotated[NDArrayI3, is_non_negative]
NDArrayI3NonPos = Annotated[NDArrayI3, is_non_positive]
NDArrayI3NonZr = Annotated[NDArrayI3, is_non_zero]
NDArrayI3Pos = Annotated[NDArrayI3, is_positive]
NDArrayI3Zr = Annotated[NDArrayI3, is_zero]


# annotations - float, ndim & predicate
NDArrayF0Fin = Annotated[NDArrayF0, isfinite]
NDArrayF0FinInt = Annotated[NDArrayF0, is_finite_and_integral]
NDArrayF0FinIntNan = Annotated[NDArrayF0, is_finite_and_integral_or_nan]
NDArrayF0FinNeg = Annotated[NDArrayF0, is_finite_and_negative]
NDArrayF0FinNegNan = Annotated[NDArrayF0, is_finite_and_negative_or_nan]
NDArrayF0FinNonNeg = Annotated[NDArrayF0, is_finite_and_non_negative]
NDArrayF0FinNonNegNan = Annotated[NDArrayF0, is_finite_and_non_negative_or_nan]
NDArrayF0FinNonPos = Annotated[NDArrayF0, is_finite_and_non_positive]
NDArrayF0FinNonPosNan = Annotated[NDArrayF0, is_finite_and_non_positive_or_nan]
NDArrayF0FinNonZr = Annotated[NDArrayF0, is_finite_and_non_zero]
NDArrayF0FinNonZrNan = Annotated[NDArrayF0, is_finite_and_non_zero_or_nan]
NDArrayF0FinPos = Annotated[NDArrayF0, is_finite_and_positive]
NDArrayF0FinPosNan = Annotated[NDArrayF0, is_finite_and_positive_or_nan]
NDArrayF0FinNan = Annotated[NDArrayF0, is_finite_or_nan]
NDArrayF0Int = Annotated[NDArrayF0, is_integral]
NDArrayF0IntNan = Annotated[NDArrayF0, is_integral_or_nan]
NDArrayF0Neg = Annotated[NDArrayF0, is_negative]
NDArrayF0NegNan = Annotated[NDArrayF0, is_negative_or_nan]
NDArrayF0NonNeg = Annotated[NDArrayF0, is_non_negative]
NDArrayF0NonNegNan = Annotated[NDArrayF0, is_non_negative_or_nan]
NDArrayF0NonPos = Annotated[NDArrayF0, is_non_positive]
NDArrayF0NonPosNan = Annotated[NDArrayF0, is_non_positive_or_nan]
NDArrayF0NonZr = Annotated[NDArrayF0, is_non_zero]
NDArrayF0NonZrNan = Annotated[NDArrayF0, is_non_zero_or_nan]
NDArrayF0Pos = Annotated[NDArrayF0, is_positive]
NDArrayF0PosNan = Annotated[NDArrayF0, is_positive_or_nan]
NDArrayF0Zr = Annotated[NDArrayF0, is_zero]
NDArrayF0ZrFinNonMic = Annotated[NDArrayF0, is_zero_or_finite_and_non_micro]
NDArrayF0ZrFinNonMicNan = Annotated[NDArrayF0, is_zero_or_finite_and_non_micro_or_nan]
NDArrayF0ZrNan = Annotated[NDArrayF0, is_zero_or_nan]
NDArrayF0ZrNonMic = Annotated[NDArrayF0, is_zero_or_non_micro]
NDArrayF0ZrNonMicNan = Annotated[NDArrayF0, is_zero_or_non_micro_or_nan]

NDArrayF1Fin = Annotated[NDArrayF1, isfinite]
NDArrayF1FinInt = Annotated[NDArrayF1, is_finite_and_integral]
NDArrayF1FinIntNan = Annotated[NDArrayF1, is_finite_and_integral_or_nan]
NDArrayF1FinNeg = Annotated[NDArrayF1, is_finite_and_negative]
NDArrayF1FinNegNan = Annotated[NDArrayF1, is_finite_and_negative_or_nan]
NDArrayF1FinNonNeg = Annotated[NDArrayF1, is_finite_and_non_negative]
NDArrayF1FinNonNegNan = Annotated[NDArrayF1, is_finite_and_non_negative_or_nan]
NDArrayF1FinNonPos = Annotated[NDArrayF1, is_finite_and_non_positive]
NDArrayF1FinNonPosNan = Annotated[NDArrayF1, is_finite_and_non_positive_or_nan]
NDArrayF1FinNonZr = Annotated[NDArrayF1, is_finite_and_non_zero]
NDArrayF1FinNonZrNan = Annotated[NDArrayF1, is_finite_and_non_zero_or_nan]
NDArrayF1FinPos = Annotated[NDArrayF1, is_finite_and_positive]
NDArrayF1FinPosNan = Annotated[NDArrayF1, is_finite_and_positive_or_nan]
NDArrayF1FinNan = Annotated[NDArrayF1, is_finite_or_nan]
NDArrayF1Int = Annotated[NDArrayF1, is_integral]
NDArrayF1IntNan = Annotated[NDArrayF1, is_integral_or_nan]
NDArrayF1Neg = Annotated[NDArrayF1, is_negative]
NDArrayF1NegNan = Annotated[NDArrayF1, is_negative_or_nan]
NDArrayF1NonNeg = Annotated[NDArrayF1, is_non_negative]
NDArrayF1NonNegNan = Annotated[NDArrayF1, is_non_negative_or_nan]
NDArrayF1NonPos = Annotated[NDArrayF1, is_non_positive]
NDArrayF1NonPosNan = Annotated[NDArrayF1, is_non_positive_or_nan]
NDArrayF1NonZr = Annotated[NDArrayF1, is_non_zero]
NDArrayF1NonZrNan = Annotated[NDArrayF1, is_non_zero_or_nan]
NDArrayF1Pos = Annotated[NDArrayF1, is_positive]
NDArrayF1PosNan = Annotated[NDArrayF1, is_positive_or_nan]
NDArrayF1Zr = Annotated[NDArrayF1, is_zero]
NDArrayF1ZrFinNonMic = Annotated[NDArrayF1, is_zero_or_finite_and_non_micro]
NDArrayF1ZrFinNonMicNan = Annotated[NDArrayF1, is_zero_or_finite_and_non_micro_or_nan]
NDArrayF1ZrNan = Annotated[NDArrayF1, is_zero_or_nan]
NDArrayF1ZrNonMic = Annotated[NDArrayF1, is_zero_or_non_micro]
NDArrayF1ZrNonMicNan = Annotated[NDArrayF1, is_zero_or_non_micro_or_nan]

NDArrayF2Fin = Annotated[NDArrayF2, isfinite]
NDArrayF2FinInt = Annotated[NDArrayF2, is_finite_and_integral]
NDArrayF2FinIntNan = Annotated[NDArrayF2, is_finite_and_integral_or_nan]
NDArrayF2FinNeg = Annotated[NDArrayF2, is_finite_and_negative]
NDArrayF2FinNegNan = Annotated[NDArrayF2, is_finite_and_negative_or_nan]
NDArrayF2FinNonNeg = Annotated[NDArrayF2, is_finite_and_non_negative]
NDArrayF2FinNonNegNan = Annotated[NDArrayF2, is_finite_and_non_negative_or_nan]
NDArrayF2FinNonPos = Annotated[NDArrayF2, is_finite_and_non_positive]
NDArrayF2FinNonPosNan = Annotated[NDArrayF2, is_finite_and_non_positive_or_nan]
NDArrayF2FinNonZr = Annotated[NDArrayF2, is_finite_and_non_zero]
NDArrayF2FinNonZrNan = Annotated[NDArrayF2, is_finite_and_non_zero_or_nan]
NDArrayF2FinPos = Annotated[NDArrayF2, is_finite_and_positive]
NDArrayF2FinPosNan = Annotated[NDArrayF2, is_finite_and_positive_or_nan]
NDArrayF2FinNan = Annotated[NDArrayF2, is_finite_or_nan]
NDArrayF2Int = Annotated[NDArrayF2, is_integral]
NDArrayF2IntNan = Annotated[NDArrayF2, is_integral_or_nan]
NDArrayF2Neg = Annotated[NDArrayF2, is_negative]
NDArrayF2NegNan = Annotated[NDArrayF2, is_negative_or_nan]
NDArrayF2NonNeg = Annotated[NDArrayF2, is_non_negative]
NDArrayF2NonNegNan = Annotated[NDArrayF2, is_non_negative_or_nan]
NDArrayF2NonPos = Annotated[NDArrayF2, is_non_positive]
NDArrayF2NonPosNan = Annotated[NDArrayF2, is_non_positive_or_nan]
NDArrayF2NonZr = Annotated[NDArrayF2, is_non_zero]
NDArrayF2NonZrNan = Annotated[NDArrayF2, is_non_zero_or_nan]
NDArrayF2Pos = Annotated[NDArrayF2, is_positive]
NDArrayF2PosNan = Annotated[NDArrayF2, is_positive_or_nan]
NDArrayF2Zr = Annotated[NDArrayF2, is_zero]
NDArrayF2ZrFinNonMic = Annotated[NDArrayF2, is_zero_or_finite_and_non_micro]
NDArrayF2ZrFinNonMicNan = Annotated[NDArrayF2, is_zero_or_finite_and_non_micro_or_nan]
NDArrayF2ZrNan = Annotated[NDArrayF2, is_zero_or_nan]
NDArrayF2ZrNonMic = Annotated[NDArrayF2, is_zero_or_non_micro]
NDArrayF2ZrNonMicNan = Annotated[NDArrayF2, is_zero_or_non_micro_or_nan]

NDArrayF3Fin = Annotated[NDArrayF3, isfinite]
NDArrayF3FinInt = Annotated[NDArrayF3, is_finite_and_integral]
NDArrayF3FinIntNan = Annotated[NDArrayF3, is_finite_and_integral_or_nan]
NDArrayF3FinNeg = Annotated[NDArrayF3, is_finite_and_negative]
NDArrayF3FinNegNan = Annotated[NDArrayF3, is_finite_and_negative_or_nan]
NDArrayF3FinNonNeg = Annotated[NDArrayF3, is_finite_and_non_negative]
NDArrayF3FinNonNegNan = Annotated[NDArrayF3, is_finite_and_non_negative_or_nan]
NDArrayF3FinNonPos = Annotated[NDArrayF3, is_finite_and_non_positive]
NDArrayF3FinNonPosNan = Annotated[NDArrayF3, is_finite_and_non_positive_or_nan]
NDArrayF3FinNonZr = Annotated[NDArrayF3, is_finite_and_non_zero]
NDArrayF3FinNonZrNan = Annotated[NDArrayF3, is_finite_and_non_zero_or_nan]
NDArrayF3FinPos = Annotated[NDArrayF3, is_finite_and_positive]
NDArrayF3FinPosNan = Annotated[NDArrayF3, is_finite_and_positive_or_nan]
NDArrayF3FinNan = Annotated[NDArrayF3, is_finite_or_nan]
NDArrayF3Int = Annotated[NDArrayF3, is_integral]
NDArrayF3IntNan = Annotated[NDArrayF3, is_integral_or_nan]
NDArrayF3Neg = Annotated[NDArrayF3, is_negative]
NDArrayF3NegNan = Annotated[NDArrayF3, is_negative_or_nan]
NDArrayF3NonNeg = Annotated[NDArrayF3, is_non_negative]
NDArrayF3NonNegNan = Annotated[NDArrayF3, is_non_negative_or_nan]
NDArrayF3NonPos = Annotated[NDArrayF3, is_non_positive]
NDArrayF3NonPosNan = Annotated[NDArrayF3, is_non_positive_or_nan]
NDArrayF3NonZr = Annotated[NDArrayF3, is_non_zero]
NDArrayF3NonZrNan = Annotated[NDArrayF3, is_non_zero_or_nan]
NDArrayF3Pos = Annotated[NDArrayF3, is_positive]
NDArrayF3PosNan = Annotated[NDArrayF3, is_positive_or_nan]
NDArrayF3Zr = Annotated[NDArrayF3, is_zero]
NDArrayF3ZrFinNonMic = Annotated[NDArrayF3, is_zero_or_finite_and_non_micro]
NDArrayF3ZrFinNonMicNan = Annotated[NDArrayF3, is_zero_or_finite_and_non_micro_or_nan]
NDArrayF3ZrNan = Annotated[NDArrayF3, is_zero_or_nan]
NDArrayF3ZrNonMic = Annotated[NDArrayF3, is_zero_or_non_micro]
NDArrayF3ZrNonMicNan = Annotated[NDArrayF3, is_zero_or_non_micro_or_nan]


__all__ = [
    "AsIntError",
    "DATETIME_MAX_AS_DATETIME64",
    "DATETIME_MAX_AS_INT",
    "DATETIME_MIN_AS_DATETIME64",
    "DATETIME_MIN_AS_INT",
    "DATE_MAX_AS_DATETIME64",
    "DATE_MAX_AS_INT",
    "DATE_MIN_AS_DATETIME64",
    "DATE_MIN_AS_INT",
    "DEFAULT_RNG",
    "DateTime64ToDateError",
    "DateTime64ToDateTimeError",
    "Datetime64Kind",
    "Datetime64Unit",
    "EmptyNumpyConcatenateError",
    "FlatN0Error",
    "GetFillValueError",
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
    "NDArrayDD",
    "NDArrayDD0",
    "NDArrayDD1",
    "NDArrayDD2",
    "NDArrayDD3",
    "NDArrayDM",
    "NDArrayDM0",
    "NDArrayDM1",
    "NDArrayDM2",
    "NDArrayDM3",
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
    "NDArrayDas",
    "NDArrayDas0",
    "NDArrayDas1",
    "NDArrayDas2",
    "NDArrayDas3",
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
    "NDArrayDm0",
    "NDArrayDm1",
    "NDArrayDm2",
    "NDArrayDm3",
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
    "PctChangeError",
    "ShiftError",
    "array_indexer",
    "as_int",
    "date_to_datetime64",
    "datetime64D",
    "datetime64M",
    "datetime64W",
    "datetime64Y",
    "datetime64_dtype_to_unit",
    "datetime64_to_date",
    "datetime64_to_datetime",
    "datetime64_to_int",
    "datetime64_unit_to_dtype",
    "datetime64_unit_to_kind",
    "datetime64as",
    "datetime64fs",
    "datetime64h",
    "datetime64m",
    "datetime64ms",
    "datetime64ns",
    "datetime64ps",
    "datetime64s",
    "datetime64us",
    "datetime_to_datetime64",
    "discretize",
    "ewma",
    "exp_moving_sum",
    "ffill",
    "ffill_non_nan_slices",
    "fillna",
    "flatn0",
    "get_fill_value",
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
    "redirect_empty_numpy_concatenate",
    "shift",
    "shift_bool",
    "year",
]
