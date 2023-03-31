from collections.abc import Callable
from typing import Annotated, Any, Optional, Union, cast

import numpy as np
from beartype import beartype
from beartype.vale import Is, IsAttr, IsEqual
from numpy import (
    bool_,
    dtype,
    float64,
    int64,
    isfinite,
    isnan,
    log,
    object_,
    rint,
    unravel_index,
)
from numpy.random import default_rng
from numpy.typing import NDArray

from utilities.beartype import NDim0, NDim1, NDim2, NDim3

# dtypes
datetime64D = dtype("datetime64[D]")  # noqa: N816
datetime64Y = dtype("datetime64[Y]")  # noqa: N816
datetime64ms = dtype("datetime64[ms]")
datetime64ns = dtype("datetime64[ns]")
datetime64us = dtype("datetime64[us]")

# dtype checkers
DTypeB = IsAttr["dtype", IsEqual[bool]]
DTypeDD = IsAttr["dtype", IsEqual[datetime64D]]
DTypeDY = IsAttr["dtype", IsEqual[datetime64Y]]
DTypeDms = IsAttr["dtype", IsEqual[datetime64ms]]
DTypeDns = IsAttr["dtype", IsEqual[datetime64ns]]
DTypeDus = IsAttr["dtype", IsEqual[datetime64us]]
DTypeF = IsAttr["dtype", IsEqual[float]]
DTypeI = IsAttr["dtype", IsEqual[int]]
DTypeO = IsAttr["dtype", IsEqual[object]]

# annotated; dtype
NDArrayB = NDArray[bool_]
NDArrayDD = NDArray[cast(Any, datetime64D)]
NDArrayDY = NDArray[cast(Any, datetime64Y)]
NDArrayDms = NDArray[cast(Any, datetime64ms)]
NDArrayDns = NDArray[cast(Any, datetime64ns)]
NDArrayDus = NDArray[cast(Any, datetime64us)]
NDArrayF = NDArray[float64]
NDArrayI = NDArray[int64]
NDArrayO = NDArray[object_]

# annotated; ndim
NDArray0 = Annotated[NDArray[Any], NDim0]
NDArray1 = Annotated[NDArray[Any], NDim1]
NDArray2 = Annotated[NDArray[Any], NDim2]
NDArray3 = Annotated[NDArray[Any], NDim3]

# annotated; dtype & ndim
NDArrayB0 = Annotated[NDArrayB, NDim0]
NDArrayDD0 = Annotated[NDArrayDD, NDim0]
NDArrayDY0 = Annotated[NDArrayDY, NDim0]
NDArrayDms0 = Annotated[NDArrayDms, NDim0]
NDArrayDns0 = Annotated[NDArrayDns, NDim0]
NDArrayDus0 = Annotated[NDArrayDus, NDim0]
NDArrayF0 = Annotated[NDArrayF, NDim0]
NDArrayI0 = Annotated[NDArrayI, NDim0]
NDArrayO0 = Annotated[NDArrayO, NDim0]

NDArrayB1 = Annotated[NDArrayB, NDim1]
NDArrayDD1 = Annotated[NDArrayDD, NDim1]
NDArrayDY1 = Annotated[NDArrayDY, NDim1]
NDArrayDms1 = Annotated[NDArrayDms, NDim1]
NDArrayDns1 = Annotated[NDArrayDns, NDim1]
NDArrayDus1 = Annotated[NDArrayDus, NDim1]
NDArrayF1 = Annotated[NDArrayF, NDim1]
NDArrayI1 = Annotated[NDArrayI, NDim1]
NDArrayO1 = Annotated[NDArrayO, NDim1]

NDArrayB2 = Annotated[NDArrayB, NDim2]
NDArrayDD2 = Annotated[NDArrayDD, NDim2]
NDArrayDY2 = Annotated[NDArrayDY, NDim2]
NDArrayDms2 = Annotated[NDArrayDms, NDim2]
NDArrayDns2 = Annotated[NDArrayDns, NDim2]
NDArrayDus2 = Annotated[NDArrayDus, NDim2]
NDArrayF2 = Annotated[NDArrayF, NDim2]
NDArrayI2 = Annotated[NDArrayI, NDim2]
NDArrayO2 = Annotated[NDArrayO, NDim2]

NDArrayB3 = Annotated[NDArrayB, NDim3]
NDArrayDD3 = Annotated[NDArrayDD, NDim3]
NDArrayDY3 = Annotated[NDArrayDY, NDim3]
NDArrayDms3 = Annotated[NDArrayDms, NDim3]
NDArrayDns3 = Annotated[NDArrayDns, NDim3]
NDArrayDus3 = Annotated[NDArrayDus, NDim3]
NDArrayF3 = Annotated[NDArrayF, NDim3]
NDArrayI3 = Annotated[NDArrayI, NDim3]
NDArrayO3 = Annotated[NDArrayO, NDim3]


# checks


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
    return isfinite(x) & is_integral(x, rtol=rtol, atol=atol)


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
def is_finite_or_nan(x: Any, /) -> Any:
    """Check if -inf < x < inf or x == nan."""
    return isfinite(x) | isnan(x)


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
def is_zero_or_finite_and_non_micro(
    x: Any, /, *, rtol: Optional[float] = None, atol: Optional[float] = None
) -> Any:
    """Check if x == 0, or -inf < x < inf and ~isclose(x, 0)."""
    zero = 0.0
    return (x == zero) | is_finite_and_non_zero(x, rtol=rtol, atol=atol)


@beartype
def is_zero_or_finite_and_non_micro_or_nan(
    x: Any, /, *, rtol: Optional[float] = None, atol: Optional[float] = None
) -> Any:
    """Check if x == 0, or -inf < x < inf and ~isclose(x, 0), or x == nan."""
    return is_zero_or_finite_and_non_micro(x, rtol=rtol, atol=atol) | isnan(x)


@beartype
def is_zero_or_nan(
    x: Any, /, *, rtol: Optional[float] = None, atol: Optional[float] = None
) -> Any:
    """Check if x > 0 or x == nan."""
    return is_zero(x, rtol=rtol, atol=atol) | isnan(x)


@beartype
def is_zero_or_non_micro(
    x: Any, /, *, rtol: Optional[float] = None, atol: Optional[float] = None
) -> Any:
    """Check if x == 0 or ~isclose(x, 0)."""
    zero = 0.0
    return (x == zero) | is_non_zero(x, rtol=rtol, atol=atol)


@beartype
def is_zero_or_non_micro_or_nan(
    x: Any, /, *, rtol: Optional[float] = None, atol: Optional[float] = None
) -> Any:
    """Check if x == 0 or ~isclose(x, 0) or x == nan."""
    return is_zero_or_non_micro(x, rtol=rtol, atol=atol) | isnan(x)


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
    """Check if x == y."""
    return np.isclose(
        x,
        y,
        **({} if rtol is None else {"rtol": rtol}),
        **({} if atol is None else {"atol": atol}),
        equal_nan=equal_nan,
    )


# lifted checks


@beartype
def _lift(check: Callable[..., Any], /) -> Any:
    """Lift a check to work on a subset of a float array."""
    rng = default_rng()

    @beartype
    def predicate(array: Union[NDArrayI, NDArrayF], /) -> bool:
        if (size := array.size) == 0:
            return True
        if size == 1:
            return check(array).item()
        num_samples = round(log(size))
        indices = rng.integers(0, size, size=num_samples)
        sample = array[unravel_index(indices, array.shape)]
        return check(sample).all().item()

    return Is[cast(Any, predicate)]


_is_finite = _lift(isfinite)
_is_finite_and_integral = _lift(is_finite_and_integral)
_is_finite_and_integral_or_nan = _lift(is_finite_and_integral_or_nan)
_is_finite_and_negative = _lift(is_finite_and_negative)
_is_finite_and_negative_or_nan = _lift(is_finite_and_negative_or_nan)
_is_finite_and_non_negative = _lift(is_finite_and_non_negative)
_is_finite_and_non_negative_or_nan = _lift(is_finite_and_non_negative_or_nan)
_is_finite_and_non_positive = _lift(is_finite_and_non_positive)
_is_finite_and_non_positive_or_nan = _lift(is_finite_and_non_positive_or_nan)
_is_finite_and_non_zero = _lift(is_finite_and_non_zero)
_is_finite_and_non_zero_or_nan = _lift(is_finite_and_non_zero_or_nan)
_is_finite_and_positive = _lift(is_finite_and_positive)
_is_finite_and_positive_or_nan = _lift(is_finite_and_positive_or_nan)
_is_finite_or_nan = _lift(is_finite_or_nan)
_is_integral = _lift(is_integral)
_is_integral_or_nan = _lift(is_integral_or_nan)
_is_negative = _lift(is_negative)
_is_negative_or_nan = _lift(is_negative_or_nan)
_is_non_negative = _lift(is_non_negative)
_is_non_negative_or_nan = _lift(is_non_negative_or_nan)
_is_non_positive = _lift(is_non_positive)
_is_non_positive_or_nan = _lift(is_non_positive_or_nan)
_is_non_zero = _lift(is_non_zero)
_is_non_zero_or_nan = _lift(is_non_zero_or_nan)
_is_positive = _lift(is_positive)
_is_positive_or_nan = _lift(is_positive_or_nan)
_is_zero = _lift(is_zero)
_is_zero_or_finite_and_non_micro = _lift(is_zero_or_finite_and_non_micro)
_is_zero_or_finite_and_non_micro_or_nan = _lift(is_zero_or_finite_and_non_micro_or_nan)
_is_zero_or_nan = _lift(is_zero_or_nan)
_is_zero_or_non_micro = _lift(is_zero_or_non_micro)
_is_zero_or_non_micro_or_nan = _lift(is_zero_or_non_micro_or_nan)


# annotated; int & checks
NDArrayINeg = Annotated[NDArrayI, _is_negative]
NDArrayINonNeg = Annotated[NDArrayI, _is_non_negative]
NDArrayINonPos = Annotated[NDArrayI, _is_non_positive]
NDArrayINonZr = Annotated[NDArrayI, _is_non_zero]
NDArrayIPos = Annotated[NDArrayI, _is_positive]
NDArrayIZr = Annotated[NDArrayI, _is_zero]


# annotated; float & checks
NDArrayFFin = Annotated[NDArrayF, _is_finite]
NDArrayFFinInt = Annotated[NDArrayF, _is_finite_and_integral]
NDArrayFFinIntNan = Annotated[NDArrayF, _is_finite_and_integral_or_nan]
NDArrayFFinNeg = Annotated[NDArrayF, _is_finite_and_negative]
NDArrayFFinNegNan = Annotated[NDArrayF, _is_finite_and_negative_or_nan]
NDArrayFFinNonNeg = Annotated[NDArrayF, _is_finite_and_non_negative]
NDArrayFFinNonNegNan = Annotated[NDArrayF, _is_finite_and_non_negative_or_nan]
NDArrayFFinNonPos = Annotated[NDArrayF, _is_finite_and_non_positive]
NDArrayFFinNonPosNan = Annotated[NDArrayF, _is_finite_and_non_positive_or_nan]
NDArrayFFinNonZr = Annotated[NDArrayF, _is_finite_and_non_zero]
NDArrayFFinNonZrNan = Annotated[NDArrayF, _is_finite_and_non_zero_or_nan]
NDArrayFFinPos = Annotated[NDArrayF, _is_finite_and_positive]
NDArrayFFinPosNan = Annotated[NDArrayF, _is_finite_and_positive_or_nan]
NDArrayFFinNan = Annotated[NDArrayF, _is_finite_or_nan]
NDArrayFInt = Annotated[NDArrayF, _is_integral]
NDArrayFIntNan = Annotated[NDArrayF, _is_integral_or_nan]
NDArrayFNeg = Annotated[NDArrayF, _is_negative]
NDArrayFNegNan = Annotated[NDArrayF, _is_negative_or_nan]
NDArrayFNonNeg = Annotated[NDArrayF, _is_non_negative]
NDArrayFNonNegNan = Annotated[NDArrayF, _is_non_negative_or_nan]
NDArrayFNonPos = Annotated[NDArrayF, _is_non_positive]
NDArrayFNonPosNan = Annotated[NDArrayF, _is_non_positive_or_nan]
NDArrayFNonZr = Annotated[NDArrayF, _is_non_zero]
NDArrayFNonZrNan = Annotated[NDArrayF, _is_non_zero_or_nan]
NDArrayFPos = Annotated[NDArrayF, _is_positive]
NDArrayFPosNan = Annotated[NDArrayF, _is_positive_or_nan]
NDArrayFZr = Annotated[NDArrayF, _is_zero]
NDArrayFZrFinNonMic = Annotated[NDArrayF, _is_zero_or_finite_and_non_micro]
NDArrayFZrFinNonMicNan = Annotated[NDArrayF, _is_zero_or_finite_and_non_micro_or_nan]
NDArrayFZrNan = Annotated[NDArrayF, _is_zero_or_nan]
NDArrayFZrNonMic = Annotated[NDArrayF, _is_zero_or_non_micro]
NDArrayFZrNonMicNan = Annotated[NDArrayF, _is_zero_or_non_micro_or_nan]

# annotated; int, ndim & checks
NDArrayI0Neg = Annotated[NDArrayI, NDim0 & _is_negative]
NDArrayI0NonNeg = Annotated[NDArrayI, NDim0 & _is_non_negative]
NDArrayI0NonPos = Annotated[NDArrayI, NDim0 & _is_non_positive]
NDArrayI0NonZr = Annotated[NDArrayI, NDim0 & _is_non_zero]
NDArrayI0Pos = Annotated[NDArrayI, NDim0 & _is_positive]
NDArrayI0Zr = Annotated[NDArrayI, NDim0 & _is_zero]

NDArrayI1Neg = Annotated[NDArrayI, NDim1 & _is_negative]
NDArrayI1NonNeg = Annotated[NDArrayI, NDim1 & _is_non_negative]
NDArrayI1NonPos = Annotated[NDArrayI, NDim1 & _is_non_positive]
NDArrayI1NonZr = Annotated[NDArrayI, NDim1 & _is_non_zero]
NDArrayI1Pos = Annotated[NDArrayI, NDim1 & _is_positive]
NDArrayI1Zr = Annotated[NDArrayI, NDim1 & _is_zero]

NDArrayI2Neg = Annotated[NDArrayI, NDim2 & _is_negative]
NDArrayI2NonNeg = Annotated[NDArrayI, NDim2 & _is_non_negative]
NDArrayI2NonPos = Annotated[NDArrayI, NDim2 & _is_non_positive]
NDArrayI2NonZr = Annotated[NDArrayI, NDim2 & _is_non_zero]
NDArrayI2Pos = Annotated[NDArrayI, NDim2 & _is_positive]
NDArrayI2Zr = Annotated[NDArrayI, NDim2 & _is_zero]

NDArrayI3Neg = Annotated[NDArrayI, NDim1 & _is_negative]
NDArrayI3NonNeg = Annotated[NDArrayI, NDim3 & _is_non_negative]
NDArrayI3NonPos = Annotated[NDArrayI, NDim3 & _is_non_positive]
NDArrayI3NonZr = Annotated[NDArrayI, NDim3 & _is_non_zero]
NDArrayI3Pos = Annotated[NDArrayI, NDim3 & _is_positive]
NDArrayI3Zr = Annotated[NDArrayI, NDim3 & _is_zero]

# annotated; float, ndim & checks
NDArrayF0Fin = Annotated[NDArrayF, NDim0 & _is_finite]
NDArrayF0FinInt = Annotated[NDArrayF, NDim0 & _is_finite_and_integral]
NDArrayF0FinIntNan = Annotated[NDArrayF, NDim0 & _is_finite_and_integral_or_nan]
NDArrayF0FinNeg = Annotated[NDArrayF, NDim0 & _is_finite_and_negative]
NDArrayF0FinNegNan = Annotated[NDArrayF, NDim0 & _is_finite_and_negative_or_nan]
NDArrayF0FinNonNeg = Annotated[NDArrayF, NDim0 & _is_finite_and_non_negative]
NDArrayF0FinNonNegNan = Annotated[NDArrayF, NDim0 & _is_finite_and_non_negative_or_nan]
NDArrayF0FinNonPos = Annotated[NDArrayF, NDim0 & _is_finite_and_non_positive]
NDArrayF0FinNonPosNan = Annotated[NDArrayF, NDim0 & _is_finite_and_non_positive_or_nan]
NDArrayF0FinNonZr = Annotated[NDArrayF, NDim0 & _is_finite_and_non_zero]
NDArrayF0FinNonZrNan = Annotated[NDArrayF, NDim0 & _is_finite_and_non_zero_or_nan]
NDArrayF0FinPos = Annotated[NDArrayF, NDim0 & _is_finite_and_positive]
NDArrayF0FinPosNan = Annotated[NDArrayF, NDim0 & _is_finite_and_positive_or_nan]
NDArrayF0FinNan = Annotated[NDArrayF, NDim0 & _is_finite_or_nan]
NDArrayF0Int = Annotated[NDArrayF, NDim0 & _is_integral]
NDArrayF0IntNan = Annotated[NDArrayF, NDim0 & _is_integral_or_nan]
NDArrayF0Neg = Annotated[NDArrayF, NDim0 & _is_negative]
NDArrayF0NegNan = Annotated[NDArrayF, NDim0 & _is_negative_or_nan]
NDArrayF0NonNeg = Annotated[NDArrayF, NDim0 & _is_non_negative]
NDArrayF0NonNegNan = Annotated[NDArrayF, NDim0 & _is_non_negative_or_nan]
NDArrayF0NonPos = Annotated[NDArrayF, NDim0 & _is_non_positive]
NDArrayF0NonPosNan = Annotated[NDArrayF, NDim0 & _is_non_positive_or_nan]
NDArrayF0NonZr = Annotated[NDArrayF, NDim0 & _is_non_zero]
NDArrayF0NonZrNan = Annotated[NDArrayF, NDim0 & _is_non_zero_or_nan]
NDArrayF0Pos = Annotated[NDArrayF, NDim0 & _is_positive]
NDArrayF0PosNan = Annotated[NDArrayF, NDim0 & _is_positive_or_nan]
NDArrayF0Zr = Annotated[NDArrayF, NDim0 & _is_zero]
NDArrayF0ZrFinNonMic = Annotated[NDArrayF, NDim0 & _is_zero_or_finite_and_non_micro]
NDArrayF0ZrFinNonMicNan = Annotated[
    NDArrayF, NDim0 & _is_zero_or_finite_and_non_micro_or_nan
]
NDArrayF0ZrNan = Annotated[NDArrayF, NDim0 & _is_zero_or_nan]
NDArrayF0ZrNonMic = Annotated[NDArrayF, NDim0 & _is_zero_or_non_micro]
NDArrayF0ZrNonMicNan = Annotated[NDArrayF, NDim0 & _is_zero_or_non_micro_or_nan]

NDArrayF1Fin = Annotated[NDArrayF, NDim1 & _is_finite]
NDArrayF1FinInt = Annotated[NDArrayF, NDim1 & _is_finite_and_integral]
NDArrayF1FinIntNan = Annotated[NDArrayF, NDim1 & _is_finite_and_integral_or_nan]
NDArrayF1FinNeg = Annotated[NDArrayF, NDim1 & _is_finite_and_negative]
NDArrayF1FinNegNan = Annotated[NDArrayF, NDim1 & _is_finite_and_negative_or_nan]
NDArrayF1FinNonNeg = Annotated[NDArrayF, NDim1 & _is_finite_and_non_negative]
NDArrayF1FinNonNegNan = Annotated[NDArrayF, NDim1 & _is_finite_and_non_negative_or_nan]
NDArrayF1FinNonPos = Annotated[NDArrayF, NDim1 & _is_finite_and_non_positive]
NDArrayF1FinNonPosNan = Annotated[NDArrayF, NDim1 & _is_finite_and_non_positive_or_nan]
NDArrayF1FinNonZr = Annotated[NDArrayF, NDim1 & _is_finite_and_non_zero]
NDArrayF1FinNonZrNan = Annotated[NDArrayF, NDim1 & _is_finite_and_non_zero_or_nan]
NDArrayF1FinPos = Annotated[NDArrayF, NDim1 & _is_finite_and_positive]
NDArrayF1FinPosNan = Annotated[NDArrayF, NDim1 & _is_finite_and_positive_or_nan]
NDArrayF1FinNan = Annotated[NDArrayF, NDim1 & _is_finite_or_nan]
NDArrayF1Int = Annotated[NDArrayF, NDim1 & _is_integral]
NDArrayF1IntNan = Annotated[NDArrayF, NDim1 & _is_integral_or_nan]
NDArrayF1Neg = Annotated[NDArrayF, NDim1 & _is_negative]
NDArrayF1NegNan = Annotated[NDArrayF, NDim1 & _is_negative_or_nan]
NDArrayF1NonNeg = Annotated[NDArrayF, NDim1 & _is_non_negative]
NDArrayF1NonNegNan = Annotated[NDArrayF, NDim1 & _is_non_negative_or_nan]
NDArrayF1NonPos = Annotated[NDArrayF, NDim1 & _is_non_positive]
NDArrayF1NonPosNan = Annotated[NDArrayF, NDim1 & _is_non_positive_or_nan]
NDArrayF1NonZr = Annotated[NDArrayF, NDim1 & _is_non_zero]
NDArrayF1NonZrNan = Annotated[NDArrayF, NDim1 & _is_non_zero_or_nan]
NDArrayF1Pos = Annotated[NDArrayF, NDim1 & _is_positive]
NDArrayF1PosNan = Annotated[NDArrayF, NDim1 & _is_positive_or_nan]
NDArrayF1Zr = Annotated[NDArrayF, NDim1 & _is_zero]
NDArrayF1ZrFinNonMic = Annotated[NDArrayF, NDim1 & _is_zero_or_finite_and_non_micro]
NDArrayF1ZrFinNonMicNan = Annotated[
    NDArrayF, NDim1 & _is_zero_or_finite_and_non_micro_or_nan
]
NDArrayF1ZrNan = Annotated[NDArrayF, NDim1 & _is_zero_or_nan]
NDArrayF1ZrNonMic = Annotated[NDArrayF, NDim1 & _is_zero_or_non_micro]
NDArrayF1ZrNonMicNan = Annotated[NDArrayF, NDim1 & _is_zero_or_non_micro_or_nan]

NDArrayF2Fin = Annotated[NDArrayF, NDim2 & _is_finite]
NDArrayF2FinInt = Annotated[NDArrayF, NDim2 & _is_finite_and_integral]
NDArrayF2FinIntNan = Annotated[NDArrayF, NDim2 & _is_finite_and_integral_or_nan]
NDArrayF2FinNeg = Annotated[NDArrayF, NDim2 & _is_finite_and_negative]
NDArrayF2FinNegNan = Annotated[NDArrayF, NDim2 & _is_finite_and_negative_or_nan]
NDArrayF2FinNonNeg = Annotated[NDArrayF, NDim2 & _is_finite_and_non_negative]
NDArrayF2FinNonNegNan = Annotated[NDArrayF, NDim2 & _is_finite_and_non_negative_or_nan]
NDArrayF2FinNonPos = Annotated[NDArrayF, NDim2 & _is_finite_and_non_positive]
NDArrayF2FinNonPosNan = Annotated[NDArrayF, NDim2 & _is_finite_and_non_positive_or_nan]
NDArrayF2FinNonZr = Annotated[NDArrayF, NDim2 & _is_finite_and_non_zero]
NDArrayF2FinNonZrNan = Annotated[NDArrayF, NDim2 & _is_finite_and_non_zero_or_nan]
NDArrayF2FinPos = Annotated[NDArrayF, NDim2 & _is_finite_and_positive]
NDArrayF2FinPosNan = Annotated[NDArrayF, NDim2 & _is_finite_and_positive_or_nan]
NDArrayF2FinNan = Annotated[NDArrayF, NDim2 & _is_finite_or_nan]
NDArrayF2Int = Annotated[NDArrayF, NDim2 & _is_integral]
NDArrayF2IntNan = Annotated[NDArrayF, NDim2 & _is_integral_or_nan]
NDArrayF2Neg = Annotated[NDArrayF, NDim2 & _is_negative]
NDArrayF2NegNan = Annotated[NDArrayF, NDim2 & _is_negative_or_nan]
NDArrayF2NonNeg = Annotated[NDArrayF, NDim2 & _is_non_negative]
NDArrayF2NonNegNan = Annotated[NDArrayF, NDim2 & _is_non_negative_or_nan]
NDArrayF2NonPos = Annotated[NDArrayF, NDim2 & _is_non_positive]
NDArrayF2NonPosNan = Annotated[NDArrayF, NDim2 & _is_non_positive_or_nan]
NDArrayF2NonZr = Annotated[NDArrayF, NDim2 & _is_non_zero]
NDArrayF2NonZrNan = Annotated[NDArrayF, NDim2 & _is_non_zero_or_nan]
NDArrayF2Pos = Annotated[NDArrayF, NDim2 & _is_positive]
NDArrayF2PosNan = Annotated[NDArrayF, NDim2 & _is_positive_or_nan]
NDArrayF2Zr = Annotated[NDArrayF, NDim2 & _is_zero]
NDArrayF2ZrFinNonMic = Annotated[NDArrayF, NDim2 & _is_zero_or_finite_and_non_micro]
NDArrayF2ZrFinNonMicNan = Annotated[
    NDArrayF, NDim2 & _is_zero_or_finite_and_non_micro_or_nan
]
NDArrayF2ZrNan = Annotated[NDArrayF, NDim2 & _is_zero_or_nan]
NDArrayF2ZrNonMic = Annotated[NDArrayF, NDim2 & _is_zero_or_non_micro]
NDArrayF2ZrNonMicNan = Annotated[NDArrayF, NDim2 & _is_zero_or_non_micro_or_nan]

NDArrayF3Fin = Annotated[NDArrayF, NDim3 & _is_finite]
NDArrayF3FinInt = Annotated[NDArrayF, NDim3 & _is_finite_and_integral]
NDArrayF3FinIntNan = Annotated[NDArrayF, NDim3 & _is_finite_and_integral_or_nan]
NDArrayF3FinNeg = Annotated[NDArrayF, NDim3 & _is_finite_and_negative]
NDArrayF3FinNegNan = Annotated[NDArrayF, NDim3 & _is_finite_and_negative_or_nan]
NDArrayF3FinNonNeg = Annotated[NDArrayF, NDim3 & _is_finite_and_non_negative]
NDArrayF3FinNonNegNan = Annotated[NDArrayF, NDim3 & _is_finite_and_non_negative_or_nan]
NDArrayF3FinNonPos = Annotated[NDArrayF, NDim3 & _is_finite_and_non_positive]
NDArrayF3FinNonPosNan = Annotated[NDArrayF, NDim3 & _is_finite_and_non_positive_or_nan]
NDArrayF3FinNonZr = Annotated[NDArrayF, NDim3 & _is_finite_and_non_zero]
NDArrayF3FinNonZrNan = Annotated[NDArrayF, NDim3 & _is_finite_and_non_zero_or_nan]
NDArrayF3FinPos = Annotated[NDArrayF, NDim3 & _is_finite_and_positive]
NDArrayF3FinPosNan = Annotated[NDArrayF, NDim3 & _is_finite_and_positive_or_nan]
NDArrayF3FinNan = Annotated[NDArrayF, NDim3 & _is_finite_or_nan]
NDArrayF3Int = Annotated[NDArrayF, NDim3 & _is_integral]
NDArrayF3IntNan = Annotated[NDArrayF, NDim3 & _is_integral_or_nan]
NDArrayF3Neg = Annotated[NDArrayF, NDim3 & _is_negative]
NDArrayF3NegNan = Annotated[NDArrayF, NDim3 & _is_negative_or_nan]
NDArrayF3NonNeg = Annotated[NDArrayF, NDim3 & _is_non_negative]
NDArrayF3NonNegNan = Annotated[NDArrayF, NDim3 & _is_non_negative_or_nan]
NDArrayF3NonPos = Annotated[NDArrayF, NDim3 & _is_non_positive]
NDArrayF3NonPosNan = Annotated[NDArrayF, NDim3 & _is_non_positive_or_nan]
NDArrayF3NonZr = Annotated[NDArrayF, NDim3 & _is_non_zero]
NDArrayF3NonZrNan = Annotated[NDArrayF, NDim3 & _is_non_zero_or_nan]
NDArrayF3Pos = Annotated[NDArrayF, NDim3 & _is_positive]
NDArrayF3PosNan = Annotated[NDArrayF, NDim3 & _is_positive_or_nan]
NDArrayF3Zr = Annotated[NDArrayF, NDim3 & _is_zero]
NDArrayF3ZrFinNonMic = Annotated[NDArrayF, NDim3 & _is_zero_or_finite_and_non_micro]
NDArrayF3ZrFinNonMicNan = Annotated[
    NDArrayF, NDim3 & _is_zero_or_finite_and_non_micro_or_nan
]
NDArrayF3ZrNan = Annotated[NDArrayF, NDim3 & _is_zero_or_nan]
NDArrayF3ZrNonMic = Annotated[NDArrayF, NDim3 & _is_zero_or_non_micro]
NDArrayF3ZrNonMicNan = Annotated[NDArrayF, NDim3 & _is_zero_or_non_micro_or_nan]
