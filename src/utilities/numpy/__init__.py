from collections.abc import Iterable
from functools import reduce
from itertools import repeat
from typing import Any, Optional, Union, cast, overload

import numpy as np
from beartype import beartype
from bottleneck import push
from numpy import (
    array,
    digitize,
    errstate,
    flatnonzero,
    flip,
    full_like,
    inf,
    int64,
    isclose,
    isfinite,
    isinf,
    isnan,
    linspace,
    nan,
    nanquantile,
    rint,
    roll,
    where,
)

from utilities.errors import redirect_error
from utilities.iterables import is_iterable_not_str
from utilities.numpy.typing import (
    NDArrayB,
    NDArrayB1,
    NDArrayDD,
    NDArrayF,
    NDArrayF1,
    NDArrayI,
    datetime64D,
    datetime64ns,
    datetime64Y,
)

_ = (datetime64D, datetime64Y, datetime64ns)


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
    other_axes = list(range(array.ndim))
    del other_axes[axis]
    any_non_nan = (~isnan(array)).any(axis=tuple(other_axes))
    (indices_any_non_nan,) = any_non_nan.nonzero()
    (indices_all_nan,) = (~any_non_nan).nonzero()

    @beartype
    def get_index_from(idx_to: int64, /) -> Optional[int64]:
        candidates = {idx_fr for idx_fr in indices_any_non_nan if idx_fr <= idx_to}
        if limit is not None:
            candidates = {idx_fr for idx_fr in candidates if (idx_to - idx_fr) <= limit}
        return max(candidates, default=None)

    @beartype
    def lift(index: int64, /) -> tuple[Union[int64, slice], ...]:
        indexer: list[Union[int64, slice]] = list(repeat(slice(None), times=array.ndim))
        indexer[axis] = index
        return tuple(indexer)

    out = array.copy()
    for idx_to in indices_all_nan:
        if (idx_fr := get_index_from(idx_to)) is not None:
            out[lift(idx_to)] = out[lift(idx_fr)]
    return out


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
def has_dtype(x: Any, dtype: Any, /) -> bool:
    """Check if an object has the required dtype."""
    if is_iterable_not_str(dtype):
        return any(has_dtype(x, d) for d in dtype)
    return x.dtype == dtype


@beartype
def is_at_least(
    x: Any,
    y: Any,
    /,
    *,
    rtol: Optional[float] = None,
    atol: Optional[float] = None,
    equal_nan: bool = False,
) -> Any:
    """Check if x >= y."""
    return (x >= y) | _is_close(x, y, rtol=rtol, atol=atol, equal_nan=equal_nan)


@beartype
def is_at_least_or_nan(
    x: Any,
    y: Any,
    /,
    *,
    rtol: Optional[float] = None,
    atol: Optional[float] = None,
    equal_nan: bool = False,
) -> Any:
    """Check if x >= y or x == nan."""
    return is_at_least(x, y, rtol=rtol, atol=atol, equal_nan=equal_nan) | isnan(x)


@beartype
def is_at_most(
    x: Any,
    y: Any,
    /,
    *,
    rtol: Optional[float] = None,
    atol: Optional[float] = None,
    equal_nan: bool = False,
) -> Any:
    """Check if x <= y."""
    return (x <= y) | _is_close(x, y, rtol=rtol, atol=atol, equal_nan=equal_nan)


@beartype
def is_at_most_or_nan(
    x: Any,
    y: Any,
    /,
    *,
    rtol: Optional[float] = None,
    atol: Optional[float] = None,
    equal_nan: bool = False,
) -> Any:
    """Check if x <= y or x == nan."""
    return is_at_most(x, y, rtol=rtol, atol=atol, equal_nan=equal_nan) | isnan(x)


@beartype
def is_between(
    x: Any,
    low: Any,
    high: Any,
    /,
    *,
    rtol: Optional[float] = None,
    atol: Optional[float] = None,
    equal_nan: bool = False,
    low_equal_nan: bool = False,
    high_equal_nan: bool = False,
) -> Any:
    """Check if low <= x <= high."""
    return is_at_least(
        x, low, rtol=rtol, atol=atol, equal_nan=equal_nan or low_equal_nan
    ) & is_at_most(x, high, rtol=rtol, atol=atol, equal_nan=equal_nan or high_equal_nan)


@beartype
def is_between_or_nan(
    x: Any,
    low: Any,
    high: Any,
    /,
    *,
    rtol: Optional[float] = None,
    atol: Optional[float] = None,
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


@beartype
def is_finite_and_integral(
    x: Any, /, *, rtol: Optional[float] = None, atol: Optional[float] = None
) -> Any:
    """Check if -inf < x < inf and x == int(x)."""
    return isfinite(x) and is_integral(x, rtol=rtol, atol=atol)


@beartype
def is_finite_and_integral_or_nan(
    x: Any, /, *, rtol: Optional[float] = None, atol: Optional[float] = None
) -> Any:
    """Check if -inf < x < inf and x == int(x), or x == nan."""
    return is_finite_and_integral(x, rtol=rtol, atol=atol) | isnan(x)


@beartype
def is_finite_and_negative(
    x: Any, /, *, rtol: Optional[float] = None, atol: Optional[float] = None
) -> Any:
    """Check if -inf < x < 0."""
    return isfinite(x) & is_negative(x, rtol=rtol, atol=atol)


@beartype
def is_finite_and_negative_or_nan(
    x: Any, /, *, rtol: Optional[float] = None, atol: Optional[float] = None
) -> Any:
    """Check if -inf < x < 0 or x == nan."""
    return is_finite_and_negative(x, rtol=rtol, atol=atol) | isnan(x)


@beartype
def is_finite_and_non_negative(
    x: Any, /, *, rtol: Optional[float] = None, atol: Optional[float] = None
) -> Any:
    """Check if 0 <= x < inf."""
    return isfinite(x) & is_non_negative(x, rtol=rtol, atol=atol)


@beartype
def is_finite_and_non_negative_or_nan(
    x: Any, /, *, rtol: Optional[float] = None, atol: Optional[float] = None
) -> Any:
    """Check if 0 <= x < inf or x == nan."""
    return is_finite_and_non_negative(x, rtol=rtol, atol=atol) | isnan(x)


@beartype
def is_finite_and_non_positive(
    x: Any, /, *, rtol: Optional[float] = None, atol: Optional[float] = None
) -> Any:
    """Check if -inf < x <= 0."""
    return isfinite(x) & is_non_positive(x, rtol=rtol, atol=atol)


@beartype
def is_finite_and_non_positive_or_nan(
    x: Any, /, *, rtol: Optional[float] = None, atol: Optional[float] = None
) -> Any:
    """Check if -inf < x <= 0 or x == nan."""
    return is_finite_and_non_positive(x, rtol=rtol, atol=atol) | isnan(x)


@beartype
def is_finite_and_non_zero(
    x: Any, /, *, rtol: Optional[float] = None, atol: Optional[float] = None
) -> Any:
    """Check if -inf < x < inf, x != 0."""
    return isfinite(x) & is_non_zero(x, rtol=rtol, atol=atol)


@beartype
def is_finite_and_non_zero_or_nan(
    x: Any, /, *, rtol: Optional[float] = None, atol: Optional[float] = None
) -> Any:
    """Check if x != 0 or x == nan."""
    return is_finite_and_non_zero(x, rtol=rtol, atol=atol) | isnan(x)


@beartype
def is_finite_and_positive(
    x: Any, /, *, rtol: Optional[float] = None, atol: Optional[float] = None
) -> Any:
    """Check if 0 < x < inf."""
    return isfinite(x) & is_positive(x, rtol=rtol, atol=atol)


@beartype
def is_finite_and_positive_or_nan(
    x: Any, /, *, rtol: Optional[float] = None, atol: Optional[float] = None
) -> Any:
    """Check if 0 < x < inf or x == nan."""
    return is_finite_and_positive(x, rtol=rtol, atol=atol) | isnan(x)


@beartype
def is_greater_than(
    x: Any,
    y: Any,
    /,
    *,
    rtol: Optional[float] = None,
    atol: Optional[float] = None,
    equal_nan: bool = False,
) -> Any:
    """Check if x > y."""
    return ((x > y) & ~_is_close(x, y, rtol=rtol, atol=atol)) | (
        equal_nan & isnan(x) & isnan(y)
    )


@beartype
def is_greater_than_or_nan(
    x: Any,
    y: Any,
    /,
    *,
    rtol: Optional[float] = None,
    atol: Optional[float] = None,
    equal_nan: bool = False,
) -> Any:
    """Check if x > y or x == nan."""
    return is_greater_than(x, y, rtol=rtol, atol=atol, equal_nan=equal_nan) | isnan(x)


@beartype
def is_integral(
    x: Any, /, *, rtol: Optional[float] = None, atol: Optional[float] = None
) -> Any:
    """Check if x == int(x)."""
    return _is_close(x, rint(x), rtol=rtol, atol=atol)


@beartype
def is_integral_or_nan(
    x: Any, /, *, rtol: Optional[float] = None, atol: Optional[float] = None
) -> Any:
    """Check if x == int(x) or x == nan."""
    return is_integral(x, rtol=rtol, atol=atol) | isnan(x)


@beartype
def is_less_than(
    x: Any,
    y: Any,
    /,
    *,
    rtol: Optional[float] = None,
    atol: Optional[float] = None,
    equal_nan: bool = False,
) -> Any:
    """Check if x < y."""
    return ((x < y) & ~_is_close(x, y, rtol=rtol, atol=atol)) | (
        equal_nan & isnan(x) & isnan(y)
    )


@beartype
def is_less_than_or_nan(
    x: Any,
    y: Any,
    /,
    *,
    rtol: Optional[float] = None,
    atol: Optional[float] = None,
    equal_nan: bool = False,
) -> Any:
    """Check if x < y or x == nan."""
    return is_less_than(x, y, rtol=rtol, atol=atol, equal_nan=equal_nan) | isnan(x)


@beartype
def is_negative(
    x: Any, /, *, rtol: Optional[float] = None, atol: Optional[float] = None
) -> Any:
    """Check if x < 0."""
    return is_less_than(x, 0.0, rtol=rtol, atol=atol)


@beartype
def is_negative_or_nan(
    x: Any, /, *, rtol: Optional[float] = None, atol: Optional[float] = None
) -> Any:
    """Check if x < 0 or x == nan."""
    return is_negative(x, rtol=rtol, atol=atol) | isnan(x)


@beartype
def is_non_negative(
    x: Any, /, *, rtol: Optional[float] = None, atol: Optional[float] = None
) -> Any:
    """Check if x >= 0."""
    return is_at_least(x, 0.0, rtol=rtol, atol=atol)


@beartype
def is_non_negative_or_nan(
    x: Any, /, *, rtol: Optional[float] = None, atol: Optional[float] = None
) -> Any:
    """Check if x >= 0 or x == nan."""
    return is_non_negative(x, rtol=rtol, atol=atol) | isnan(x)


@beartype
def is_non_positive(
    x: Any, /, *, rtol: Optional[float] = None, atol: Optional[float] = None
) -> Any:
    """Check if x <= 0."""
    return is_at_most(x, 0.0, rtol=rtol, atol=atol)


@beartype
def is_non_positive_or_nan(
    x: Any, /, *, rtol: Optional[float] = None, atol: Optional[float] = None
) -> Any:
    """Check if x <=0 or x == nan."""
    return is_non_positive(x, rtol=rtol, atol=atol) | isnan(x)


@beartype
def is_non_zero(
    x: Any, /, *, rtol: Optional[float] = None, atol: Optional[float] = None
) -> Any:
    """Check if x != 0."""
    return ~_is_close(x, 0.0, rtol=rtol, atol=atol)


@beartype
def is_non_zero_or_nan(
    x: Any, /, *, rtol: Optional[float] = None, atol: Optional[float] = None
) -> Any:
    """Check if x != 0 or x == nan."""
    return is_non_zero(x, rtol=rtol, atol=atol) | isnan(x)


@beartype
def is_positive(
    x: Any, /, *, rtol: Optional[float] = None, atol: Optional[float] = None
) -> Any:
    """Check if x > 0."""
    return is_greater_than(x, 0, rtol=rtol, atol=atol)


@beartype
def is_positive_or_nan(
    x: Any, /, *, rtol: Optional[float] = None, atol: Optional[float] = None
) -> Any:
    """Check if x > 0 or x == nan."""
    return is_positive(x, rtol=rtol, atol=atol) | isnan(x)


@beartype
def is_zero(
    x: Any, /, *, rtol: Optional[float] = None, atol: Optional[float] = None
) -> Any:
    """Check if x == 0."""
    return _is_close(x, 0.0, rtol=rtol, atol=atol)


@beartype
def is_zero_or_nan(
    x: Any, /, *, rtol: Optional[float] = None, atol: Optional[float] = None
) -> Any:
    """Check if x > 0 or x == nan."""
    return is_zero(x, rtol=rtol, atol=atol) | isnan(x)


@beartype
def _is_close(
    x: Any,
    y: Any,
    /,
    *,
    rtol: Optional[float] = None,
    atol: Optional[float] = None,
    equal_nan: bool = False,
) -> Any:
    """Check if `x` is close to `y`."""
    return np.isclose(
        x,
        y,
        **({} if rtol is None else {"rtol": rtol}),
        **({} if atol is None else {"atol": atol}),
        equal_nan=equal_nan,
    )


@overload
def maximum(*xs: float) -> float:  # type: ignore[reportOverlappingOverload]
    ...


@overload
def maximum(*xs: Union[float, NDArrayF]) -> NDArrayF:
    ...


@beartype
def maximum(*xs: Union[float, NDArrayF]) -> Union[float, NDArrayF]:
    """Compute the maximum of a number of quantities."""
    return reduce(np.maximum, xs)


@overload
def minimum(*xs: float) -> float:  # type: ignore[reportOverlappingOverload]
    ...


@overload
def minimum(*xs: Union[float, NDArrayF]) -> NDArrayF:
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


@beartype
def year(array: NDArrayDD, /) -> NDArrayI:
    """Convert an array of dates into an array of years."""
    return 1970 + array.astype(datetime64Y).astype(int)
